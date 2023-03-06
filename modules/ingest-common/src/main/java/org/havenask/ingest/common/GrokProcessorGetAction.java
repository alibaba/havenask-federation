/*
*Copyright (c) 2021, Alibaba Group;
*Licensed under the Apache License, Version 2.0 (the "License");
*you may not use this file except in compliance with the License.
*You may obtain a copy of the License at

*   http://www.apache.org/licenses/LICENSE-2.0

*Unless required by applicable law or agreed to in writing, software
*distributed under the License is distributed on an "AS IS" BASIS,
*WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*See the License for the specific language governing permissions and
*limitations under the License.
*/

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Modifications Copyright Havenask Contributors. See
 * GitHub history for details.
 */

package org.havenask.ingest.common;

import org.havenask.LegacyESVersion;
import org.havenask.action.ActionListener;
import org.havenask.action.ActionRequest;
import org.havenask.action.ActionRequestValidationException;
import org.havenask.action.ActionResponse;
import org.havenask.action.ActionType;
import org.havenask.action.support.ActionFilters;
import org.havenask.action.support.HandledTransportAction;
import org.havenask.client.node.NodeClient;
import org.havenask.common.inject.Inject;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.io.stream.StreamOutput;
import org.havenask.common.xcontent.ToXContentObject;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.grok.Grok;
import org.havenask.rest.BaseRestHandler;
import org.havenask.rest.RestRequest;
import org.havenask.rest.action.RestToXContentListener;
import org.havenask.tasks.Task;
import org.havenask.transport.TransportService;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static java.util.Collections.singletonList;
import static org.havenask.rest.RestRequest.Method.GET;

public class GrokProcessorGetAction extends ActionType<GrokProcessorGetAction.Response> {

    static final GrokProcessorGetAction INSTANCE = new GrokProcessorGetAction();
    static final String NAME = "cluster:admin/ingest/processor/grok/get";

    private GrokProcessorGetAction() {
        super(NAME, Response::new);
    }

    public static class Request extends ActionRequest {

        private final boolean sorted;

        public Request(boolean sorted) {
            this.sorted = sorted;
        }

        Request(StreamInput in) throws IOException {
            super(in);
            this.sorted = in.getVersion().onOrAfter(LegacyESVersion.V_7_10_0) ? in.readBoolean() : false;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            if (out.getVersion().onOrAfter(LegacyESVersion.V_7_10_0)) {
                out.writeBoolean(sorted);
            }
        }

        public boolean sorted() {
            return sorted;
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {
        private final Map<String, String> grokPatterns;

        Response(Map<String, String> grokPatterns) {
            this.grokPatterns = grokPatterns;
        }

        Response(StreamInput in) throws IOException {
            super(in);
            grokPatterns = in.readMap(StreamInput::readString, StreamInput::readString);
        }

        public Map<String, String> getGrokPatterns() {
            return grokPatterns;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("patterns");
            builder.map(grokPatterns);
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeMap(grokPatterns, StreamOutput::writeString, StreamOutput::writeString);
        }
    }

    public static class TransportAction extends HandledTransportAction<Request, Response> {

        private final Map<String, String> grokPatterns;
        private final Map<String, String> sortedGrokPatterns;

        @Inject
        public TransportAction(TransportService transportService, ActionFilters actionFilters) {
            this(transportService, actionFilters, Grok.BUILTIN_PATTERNS);
        }

        // visible for testing
        TransportAction(TransportService transportService, ActionFilters actionFilters, Map<String, String> grokPatterns) {
            super(NAME, transportService, actionFilters, Request::new);
            this.grokPatterns = grokPatterns;
            this.sortedGrokPatterns = new TreeMap<>(this.grokPatterns);
        }

        @Override
        protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
            try {
                listener.onResponse(new Response(request.sorted() ? sortedGrokPatterns : grokPatterns));
            } catch (Exception e) {
                listener.onFailure(e);
            }
        }
    }

    public static class RestAction extends BaseRestHandler {

        @Override
        public List<Route> routes() {
            return singletonList(new Route(GET, "/_ingest/processor/grok"));
        }

        @Override
        public String getName() {
            return "ingest_processor_grok_get";
        }

        @Override
        protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
            boolean sorted = request.paramAsBoolean("s", false);
            Request grokPatternsRequest = new Request(sorted);
            return channel -> client.executeLocally(INSTANCE, grokPatternsRequest, new RestToXContentListener<>(channel));
        }
    }
}

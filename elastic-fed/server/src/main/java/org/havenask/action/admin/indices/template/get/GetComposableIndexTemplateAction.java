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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.havenask.action.admin.indices.template.get;

import org.havenask.action.ActionRequestValidationException;
import org.havenask.action.ActionResponse;
import org.havenask.action.ActionType;
import org.havenask.action.support.master.MasterNodeReadRequest;
import org.havenask.cluster.metadata.ComposableIndexTemplate;
import org.havenask.common.Nullable;
import org.havenask.common.ParseField;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.io.stream.StreamOutput;
import org.havenask.common.xcontent.ToXContentObject;
import org.havenask.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class GetComposableIndexTemplateAction extends ActionType<GetComposableIndexTemplateAction.Response> {

    public static final GetComposableIndexTemplateAction INSTANCE = new GetComposableIndexTemplateAction();
    public static final String NAME = "indices:admin/index_template/get";

    private GetComposableIndexTemplateAction() {
        super(NAME, GetComposableIndexTemplateAction.Response::new);
    }

    /**
     * Request that to retrieve one or more index templates
     */
    public static class Request extends MasterNodeReadRequest<Request> {

        @Nullable
        private String name;

        public Request() { }

        public Request(@Nullable String name) {
            this.name = name;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            name = in.readOptionalString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeOptionalString(name);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        /**
         * Sets the name of the index template.
         */
        public Request name(String name) {
            this.name = name;
            return this;
        }

        /**
         * The name of the index templates.
         */
        public String name() {
            return this.name;
        }

        @Override
        public int hashCode() {
            return Objects.hash(name);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Request other = (Request) obj;
            return Objects.equals(name, other.name);
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {
        public static final ParseField NAME = new ParseField("name");
        public static final ParseField INDEX_TEMPLATES = new ParseField("index_templates");
        public static final ParseField INDEX_TEMPLATE = new ParseField("index_template");

        private final Map<String, ComposableIndexTemplate> indexTemplates;

        public Response(StreamInput in) throws IOException {
            super(in);
            indexTemplates = in.readMap(StreamInput::readString, ComposableIndexTemplate::new);
        }

        public Response(Map<String, ComposableIndexTemplate> indexTemplates) {
            this.indexTemplates = indexTemplates;
        }

        public Map<String, ComposableIndexTemplate> indexTemplates() {
            return indexTemplates;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeMap(indexTemplates, StreamOutput::writeString, (o, v) -> v.writeTo(o));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            GetComposableIndexTemplateAction.Response that = (GetComposableIndexTemplateAction.Response) o;
            return Objects.equals(indexTemplates, that.indexTemplates);
        }

        @Override
        public int hashCode() {
            return Objects.hash(indexTemplates);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.startArray(INDEX_TEMPLATES.getPreferredName());
            for (Map.Entry<String, ComposableIndexTemplate> indexTemplate : this.indexTemplates.entrySet()) {
                builder.startObject();
                builder.field(NAME.getPreferredName(), indexTemplate.getKey());
                builder.field(INDEX_TEMPLATE.getPreferredName(), indexTemplate.getValue());
                builder.endObject();
            }
            builder.endArray();
            builder.endObject();
            return builder;
        }

    }

}

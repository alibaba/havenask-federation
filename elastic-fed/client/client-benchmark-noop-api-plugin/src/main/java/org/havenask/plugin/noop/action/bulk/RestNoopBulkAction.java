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

package org.havenask.plugin.noop.action.bulk;

import org.havenask.action.DocWriteRequest;
import org.havenask.action.DocWriteResponse;
import org.havenask.action.bulk.BulkItemResponse;
import org.havenask.action.bulk.BulkRequest;
import org.havenask.action.bulk.BulkShardRequest;
import org.havenask.action.support.ActiveShardCount;
import org.havenask.action.update.UpdateResponse;
import org.havenask.client.Requests;
import org.havenask.client.node.NodeClient;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.index.shard.ShardId;
import org.havenask.rest.BaseRestHandler;
import org.havenask.rest.BytesRestResponse;
import org.havenask.rest.RestChannel;
import org.havenask.rest.RestRequest;
import org.havenask.rest.RestResponse;
import org.havenask.rest.action.RestBuilderListener;

import java.io.IOException;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.havenask.rest.RestRequest.Method.POST;
import static org.havenask.rest.RestRequest.Method.PUT;
import static org.havenask.rest.RestStatus.OK;

public class RestNoopBulkAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return unmodifiableList(asList(
            new Route(POST, "/_noop_bulk"),
            new Route(PUT, "/_noop_bulk"),
            new Route(POST, "/{index}/_noop_bulk"),
            new Route(PUT, "/{index}/_noop_bulk"),
            new Route(POST, "/{index}/{type}/_noop_bulk"),
            new Route(PUT, "/{index}/{type}/_noop_bulk")));
    }

    @Override
    public String getName() {
        return "noop_bulk_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        BulkRequest bulkRequest = Requests.bulkRequest();
        String defaultIndex = request.param("index");
        String defaultType = request.param("type");
        String defaultRouting = request.param("routing");
        String defaultPipeline = request.param("pipeline");
        Boolean defaultRequireAlias = request.paramAsBoolean("require_alias", null);

        String waitForActiveShards = request.param("wait_for_active_shards");
        if (waitForActiveShards != null) {
            bulkRequest.waitForActiveShards(ActiveShardCount.parseString(waitForActiveShards));
        }
        bulkRequest.timeout(request.paramAsTime("timeout", BulkShardRequest.DEFAULT_TIMEOUT));
        bulkRequest.setRefreshPolicy(request.param("refresh"));
        bulkRequest.add(request.requiredContent(), defaultIndex, defaultType, defaultRouting,
            null, defaultPipeline, defaultRequireAlias, true, request.getXContentType());

        // short circuit the call to the transport layer
        return channel -> {
            BulkRestBuilderListener listener = new BulkRestBuilderListener(channel, request);
            listener.onResponse(bulkRequest);
        };
    }

    private static class BulkRestBuilderListener extends RestBuilderListener<BulkRequest> {
        private final BulkItemResponse ITEM_RESPONSE = new BulkItemResponse(1, DocWriteRequest.OpType.UPDATE,
            new UpdateResponse(new ShardId("mock", "", 1), "mock_type", "1", 0L, 1L, 1L, DocWriteResponse.Result.CREATED));

        private final RestRequest request;


        BulkRestBuilderListener(RestChannel channel, RestRequest request) {
            super(channel);
            this.request = request;
        }

        @Override
        public RestResponse buildResponse(BulkRequest bulkRequest, XContentBuilder builder) throws Exception {
            builder.startObject();
            builder.field(Fields.TOOK, 0);
            builder.field(Fields.ERRORS, false);
            builder.startArray(Fields.ITEMS);
            for (int idx = 0; idx < bulkRequest.numberOfActions(); idx++) {
                ITEM_RESPONSE.toXContent(builder, request);
            }
            builder.endArray();
            builder.endObject();
            return new BytesRestResponse(OK, builder);
        }
    }

    static final class Fields {
        static final String ITEMS = "items";
        static final String ERRORS = "errors";
        static final String TOOK = "took";
    }
}

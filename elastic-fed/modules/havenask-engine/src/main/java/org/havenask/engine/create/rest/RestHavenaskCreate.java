/*
 * Copyright (c) 2021, Alibaba Group;
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.havenask.engine.create.rest;

import org.havenask.action.admin.indices.create.CreateIndexRequest;
import org.havenask.client.node.NodeClient;
import org.havenask.common.xcontent.LoggingDeprecationHandler;
import org.havenask.common.xcontent.XContentParser;
import org.havenask.engine.util.JsonPrettyFormatter;
import org.havenask.rest.BaseRestHandler;
import org.havenask.rest.RestRequest;
import org.havenask.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class RestHavenaskCreate extends BaseRestHandler {
    @Override
    public String getName() {
        return "havenask_create_action";
    }

    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.PUT, "/_havenask/create/{index}"));
    }

    @Override
    @SuppressWarnings("unchecked")
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String index = request.param("index");
        XContentParser parser = request.contentParser();
        Map<String, Object> source = parser.map();

        Map<String, Object> clusters = (Map<String, Object>) source.remove("cluster");
        Map<String, Object> dataTables = (Map<String, Object>) source.remove("data_table");
        Map<String, Object> schemas = (Map<String, Object>) source.remove("schema");

        Map<String, Object> settings = (Map<String, Object>) source.remove("settings");
        if (clusters != null) {
            settings.put("index.havenask.cluster_json", JsonPrettyFormatter.toJsonString(clusters));
        }
        if (dataTables != null) {
            settings.put("index.havenask.data_table_json", JsonPrettyFormatter.toJsonString(dataTables));
        }
        if (schemas != null) {
            settings.put("index.havenask.schema_json", JsonPrettyFormatter.toJsonString(schemas));
        }

        CreateIndexRequest createIndexRequest = new CreateIndexRequest(index);
        createIndexRequest.settings(settings);
        Map<String, Object> mappings = (Map<String, Object>) source.remove("mappings");
        createIndexRequest.mapping("_doc", mappings);
        createIndexRequest.source(source, LoggingDeprecationHandler.INSTANCE);

        return channel -> client.admin().indices().create(createIndexRequest, new RestToXContentListener<>(channel));
    }
}

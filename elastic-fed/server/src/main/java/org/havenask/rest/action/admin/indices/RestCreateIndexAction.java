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

package org.havenask.rest.action.admin.indices;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.havenask.action.admin.indices.create.CreateIndexRequest;
import org.havenask.action.support.ActiveShardCount;
import org.havenask.client.node.NodeClient;
import org.havenask.common.logging.DeprecationLogger;
import org.havenask.common.xcontent.LoggingDeprecationHandler;
import org.havenask.common.xcontent.XContentHelper;
import org.havenask.index.mapper.MapperService;
import org.havenask.rest.BaseRestHandler;
import org.havenask.rest.RestRequest;
import org.havenask.rest.action.RestToXContentListener;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.havenask.rest.RestRequest.Method.PUT;

public class RestCreateIndexAction extends BaseRestHandler {
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RestCreateIndexAction.class);
    public static final String TYPES_DEPRECATION_MESSAGE = "[types removal] Using include_type_name in create " +
        "index requests is deprecated. The parameter will be removed in the next major version.";

    @Override
    public List<Route> routes() {
        return singletonList(new Route(PUT, "/{index}"));
    }

    @Override
    public String getName() {
        return "create_index_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final boolean includeTypeName = request.paramAsBoolean(INCLUDE_TYPE_NAME_PARAMETER,
            DEFAULT_INCLUDE_TYPE_NAME_POLICY);

        if (request.hasParam(INCLUDE_TYPE_NAME_PARAMETER)) {
            deprecationLogger.deprecate("create_index_with_types", TYPES_DEPRECATION_MESSAGE);
        }

        CreateIndexRequest createIndexRequest = new CreateIndexRequest(request.param("index"));

        if (request.hasContent()) {
            Map<String, Object> sourceAsMap = XContentHelper.convertToMap(request.requiredContent(), false,
                request.getXContentType()).v2();
            sourceAsMap = prepareMappings(sourceAsMap, includeTypeName);
            createIndexRequest.source(sourceAsMap, LoggingDeprecationHandler.INSTANCE);
        }

        createIndexRequest.timeout(request.paramAsTime("timeout", createIndexRequest.timeout()));
        createIndexRequest.masterNodeTimeout(request.paramAsTime("master_timeout", createIndexRequest.masterNodeTimeout()));
        createIndexRequest.waitForActiveShards(ActiveShardCount.parseString(request.param("wait_for_active_shards")));
        return channel -> client.admin().indices().create(createIndexRequest, new RestToXContentListener<>(channel));
    }


    static Map<String, Object> prepareMappings(Map<String, Object> source, boolean includeTypeName) {
        if (includeTypeName
            || source.containsKey("mappings") == false
            || (source.get("mappings") instanceof Map) == false) {
            return source;
        }

        Map<String, Object> newSource = new HashMap<>(source);

        @SuppressWarnings("unchecked")
        Map<String, Object> mappings = (Map<String, Object>) source.get("mappings");
        if (MapperService.isMappingSourceTyped(MapperService.SINGLE_MAPPING_NAME, mappings)) {
            throw new IllegalArgumentException("The mapping definition cannot be nested under a type " +
                "[" + MapperService.SINGLE_MAPPING_NAME + "] unless include_type_name is set to true.");
        }

        newSource.put("mappings", singletonMap(MapperService.SINGLE_MAPPING_NAME, mappings));
        return newSource;
    }
}

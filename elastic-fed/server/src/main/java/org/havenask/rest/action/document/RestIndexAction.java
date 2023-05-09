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

package org.havenask.rest.action.document;

import org.havenask.LegacyESVersion;
import org.havenask.action.DocWriteRequest;
import org.havenask.action.index.IndexRequest;
import org.havenask.action.support.ActiveShardCount;
import org.havenask.client.node.NodeClient;
import org.havenask.cluster.node.DiscoveryNodes;
import org.havenask.common.logging.DeprecationLogger;
import org.havenask.index.VersionType;
import org.havenask.index.mapper.MapperService;
import org.havenask.rest.BaseRestHandler;
import org.havenask.rest.RestRequest;
import org.havenask.rest.action.RestActions;
import org.havenask.rest.action.RestStatusToXContentListener;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.function.Supplier;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.havenask.rest.RestRequest.Method.POST;
import static org.havenask.rest.RestRequest.Method.PUT;

public class RestIndexAction extends BaseRestHandler {
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RestDeleteAction.class);
    public static final String TYPES_DEPRECATION_MESSAGE = "[types removal] Specifying types in document " +
        "index requests is deprecated, use the typeless endpoints instead (/{index}/_doc/{id}, /{index}/_doc, " +
        "or /{index}/_create/{id}).";

    @Override
    public List<Route> routes() {
        return unmodifiableList(asList(
            new Route(POST, "/{index}/_doc/{id}"),
            new Route(PUT, "/{index}/_doc/{id}"),
            new Route(POST, "/{index}/{type}/{id}"),
            new Route(PUT, "/{index}/{type}/{id}")));
    }

    @Override
    public String getName() {
        return "document_index_action";
    }

    public static final class CreateHandler extends RestIndexAction {

        @Override
        public String getName() {
            return "document_create_action";
        }

        @Override
        public List<Route> routes() {
            return unmodifiableList(asList(
                new Route(POST, "/{index}/_create/{id}"),
                new Route(PUT, "/{index}/_create/{id}"),
                new Route(POST, "/{index}/{type}/{id}/_create"),
                new Route(PUT, "/{index}/{type}/{id}/_create")));
        }

        @Override
        public RestChannelConsumer prepareRequest(RestRequest request, final NodeClient client) throws IOException {
            validateOpType(request.params().get("op_type"));
            request.params().put("op_type", "create");
            return super.prepareRequest(request, client);
        }

        void validateOpType(String opType) {
            if (null != opType && false == "create".equals(opType.toLowerCase(Locale.ROOT))) {
                throw new IllegalArgumentException("opType must be 'create', found: [" + opType + "]");
            }
        }
    }

    public static final class AutoIdHandler extends RestIndexAction {

        private final Supplier<DiscoveryNodes> nodesInCluster;

        public AutoIdHandler(Supplier<DiscoveryNodes> nodesInCluster) {
            this.nodesInCluster = nodesInCluster;
        }

        @Override
        public String getName() {
            return "document_create_action_auto_id";
        }

        @Override
        public List<Route> routes() {
            return unmodifiableList(asList(
                new Route(POST, "/{index}/_doc"),
                new Route(POST, "/{index}/{type}")));
        }

        @Override
        public RestChannelConsumer prepareRequest(RestRequest request, final NodeClient client) throws IOException {
            assert request.params().get("id") == null : "non-null id: " + request.params().get("id");
            if (request.params().get("op_type") == null && nodesInCluster.get().getMinNodeVersion().onOrAfter(LegacyESVersion.V_7_5_0)) {
                // default to op_type create
                request.params().put("op_type", "create");
            }
            return super.prepareRequest(request, client);
        }
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        IndexRequest indexRequest;
        final String type = request.param("type");
        if (type != null && type.equals(MapperService.SINGLE_MAPPING_NAME) == false) {
            deprecationLogger.deprecate("index_with_types", TYPES_DEPRECATION_MESSAGE);
            indexRequest = new IndexRequest(request.param("index"), type, request.param("id"));
        } else {
            indexRequest = new IndexRequest(request.param("index"));
            indexRequest.id(request.param("id"));
        }
        indexRequest.routing(request.param("routing"));
        indexRequest.setPipeline(request.param("pipeline"));
        indexRequest.source(request.requiredContent(), request.getXContentType());
        indexRequest.timeout(request.paramAsTime("timeout", IndexRequest.DEFAULT_TIMEOUT));
        indexRequest.setRefreshPolicy(request.param("refresh"));
        indexRequest.version(RestActions.parseVersion(request));
        indexRequest.versionType(VersionType.fromString(request.param("version_type"), indexRequest.versionType()));
        indexRequest.setIfSeqNo(request.paramAsLong("if_seq_no", indexRequest.ifSeqNo()));
        indexRequest.setIfPrimaryTerm(request.paramAsLong("if_primary_term", indexRequest.ifPrimaryTerm()));
        indexRequest.setRequireAlias(request.paramAsBoolean(DocWriteRequest.REQUIRE_ALIAS, indexRequest.isRequireAlias()));
        String sOpType = request.param("op_type");
        String waitForActiveShards = request.param("wait_for_active_shards");
        if (waitForActiveShards != null) {
            indexRequest.waitForActiveShards(ActiveShardCount.parseString(waitForActiveShards));
        }
        if (sOpType != null) {
            indexRequest.opType(sOpType);
        }

        return channel ->
                client.index(indexRequest, new RestStatusToXContentListener<>(channel, r -> r.getLocation(indexRequest.routing())));
    }

}

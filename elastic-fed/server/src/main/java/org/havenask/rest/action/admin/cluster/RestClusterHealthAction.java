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

package org.havenask.rest.action.admin.cluster;

import org.havenask.action.admin.cluster.health.ClusterHealthRequest;
import org.havenask.action.support.ActiveShardCount;
import org.havenask.action.support.IndicesOptions;
import org.havenask.client.node.NodeClient;
import org.havenask.cluster.health.ClusterHealthStatus;
import org.havenask.common.Priority;
import org.havenask.common.Strings;
import org.havenask.rest.BaseRestHandler;
import org.havenask.rest.RestRequest;
import org.havenask.rest.action.RestStatusToXContentListener;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.havenask.client.Requests.clusterHealthRequest;
import static org.havenask.rest.RestRequest.Method.GET;

public class RestClusterHealthAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return unmodifiableList(asList(new Route(GET, "/_cluster/health"),
            new Route(GET, "/_cluster/health/{index}")));
    }

    @Override
    public String getName() {
        return "cluster_health_action";
    }

    @Override
    public boolean allowSystemIndexAccessByDefault() {
        return true;
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final ClusterHealthRequest clusterHealthRequest = fromRequest(request);
        return channel -> client.admin().cluster().health(clusterHealthRequest, new RestStatusToXContentListener<>(channel));
    }

    public static ClusterHealthRequest fromRequest(final RestRequest request) {
        final ClusterHealthRequest clusterHealthRequest = clusterHealthRequest(Strings.splitStringByCommaToArray(request.param("index")));
        clusterHealthRequest.indicesOptions(IndicesOptions.fromRequest(request, clusterHealthRequest.indicesOptions()));
        clusterHealthRequest.local(request.paramAsBoolean("local", clusterHealthRequest.local()));
        clusterHealthRequest.masterNodeTimeout(request.paramAsTime("master_timeout", clusterHealthRequest.masterNodeTimeout()));
        clusterHealthRequest.timeout(request.paramAsTime("timeout", clusterHealthRequest.timeout()));
        String waitForStatus = request.param("wait_for_status");
        if (waitForStatus != null) {
            clusterHealthRequest.waitForStatus(ClusterHealthStatus.valueOf(waitForStatus.toUpperCase(Locale.ROOT)));
        }
        clusterHealthRequest.waitForNoRelocatingShards(
            request.paramAsBoolean("wait_for_no_relocating_shards", clusterHealthRequest.waitForNoRelocatingShards()));
        clusterHealthRequest.waitForNoInitializingShards(
            request.paramAsBoolean("wait_for_no_initializing_shards", clusterHealthRequest.waitForNoInitializingShards()));
        if (request.hasParam("wait_for_relocating_shards")) {
            // wait_for_relocating_shards has been removed in favor of wait_for_no_relocating_shards
            throw new IllegalArgumentException("wait_for_relocating_shards has been removed, " +
                "use wait_for_no_relocating_shards [true/false] instead");
        }
        String waitForActiveShards = request.param("wait_for_active_shards");
        if (waitForActiveShards != null) {
            clusterHealthRequest.waitForActiveShards(ActiveShardCount.parseString(waitForActiveShards));
        }
        clusterHealthRequest.waitForNodes(request.param("wait_for_nodes", clusterHealthRequest.waitForNodes()));
        if (request.param("wait_for_events") != null) {
            clusterHealthRequest.waitForEvents(Priority.valueOf(request.param("wait_for_events").toUpperCase(Locale.ROOT)));
        }
        return clusterHealthRequest;
    }

    private static final Set<String> RESPONSE_PARAMS = Collections.singleton("level");

    @Override
    protected Set<String> responseParams() {
        return RESPONSE_PARAMS;
    }

    @Override
    public boolean canTripCircuitBreaker() {
        return false;
    }

}

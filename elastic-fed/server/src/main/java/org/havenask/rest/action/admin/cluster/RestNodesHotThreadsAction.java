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

import org.havenask.action.admin.cluster.node.hotthreads.NodeHotThreads;
import org.havenask.action.admin.cluster.node.hotthreads.NodesHotThreadsRequest;
import org.havenask.action.admin.cluster.node.hotthreads.NodesHotThreadsResponse;
import org.havenask.client.node.NodeClient;
import org.havenask.common.Strings;
import org.havenask.common.unit.TimeValue;
import org.havenask.rest.BaseRestHandler;
import org.havenask.rest.BytesRestResponse;
import org.havenask.rest.RestRequest;
import org.havenask.rest.RestResponse;
import org.havenask.rest.RestStatus;
import org.havenask.rest.action.RestResponseListener;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.havenask.rest.RestRequest.Method.GET;

public class RestNodesHotThreadsAction extends BaseRestHandler {

    private static final String formatDeprecatedMessageWithoutNodeID = "[%s] is a deprecated endpoint. " +
        "Please use [/_nodes/hot_threads] instead.";
    private static final String formatDeprecatedMessageWithNodeID = "[%s] is a deprecated endpoint. " +
        "Please use [/_nodes/{nodeId}/hot_threads] instead.";
    private static final String DEPRECATED_MESSAGE_CLUSTER_NODES_HOT_THREADS = String.format(Locale.ROOT,
        formatDeprecatedMessageWithoutNodeID,
        "/_cluster/nodes/hot_threads"
    );
    private static final String DEPRECATED_MESSAGE_CLUSTER_NODES_NODEID_HOT_THREADS = String.format(Locale.ROOT,
        formatDeprecatedMessageWithNodeID,
        "/_cluster/nodes/{nodeId}/hot_threads"
    );
    private static final String DEPRECATED_MESSAGE_CLUSTER_NODES_HOTTHREADS = String.format(Locale.ROOT,
        formatDeprecatedMessageWithoutNodeID,
        "/_cluster/nodes/hotthreads"
    );
    private static final String DEPRECATED_MESSAGE_CLUSTER_NODES_NODEID_HOTTHREADS = String.format(Locale.ROOT,
        formatDeprecatedMessageWithNodeID,
        "/_cluster/nodes/{nodeId}/hotthreads"
    );
    private static final String DEPRECATED_MESSAGE_NODES_HOTTHREADS = String.format(Locale.ROOT,
        formatDeprecatedMessageWithoutNodeID,
        "/_nodes/hotthreads"
    );
    private static final String DEPRECATED_MESSAGE_NODES_NODEID_HOTTHREADS = String.format(Locale.ROOT,
        formatDeprecatedMessageWithNodeID,
        "/_nodes/{nodeId}/hotthreads"
    );

    @Override
    public List<DeprecatedRoute> deprecatedRoutes() {
        return unmodifiableList(asList(
            new DeprecatedRoute(GET, "/_cluster/nodes/hot_threads",
                    DEPRECATED_MESSAGE_CLUSTER_NODES_HOT_THREADS),
            new DeprecatedRoute(GET, "/_cluster/nodes/{nodeId}/hot_threads",
                    DEPRECATED_MESSAGE_CLUSTER_NODES_NODEID_HOT_THREADS),
            new DeprecatedRoute(GET, "/_cluster/nodes/hotthreads",
                    DEPRECATED_MESSAGE_CLUSTER_NODES_HOTTHREADS),
            new DeprecatedRoute(GET, "/_cluster/nodes/{nodeId}/hotthreads",
                    DEPRECATED_MESSAGE_CLUSTER_NODES_NODEID_HOTTHREADS),
            new DeprecatedRoute(GET, "/_nodes/hotthreads",
                    DEPRECATED_MESSAGE_NODES_HOTTHREADS),
            new DeprecatedRoute(GET, "/_nodes/{nodeId}/hotthreads",
                    DEPRECATED_MESSAGE_NODES_NODEID_HOTTHREADS)
        ));
    }

    @Override
    public List<Route> routes() {
        return unmodifiableList(asList(
            new Route(GET, "/_nodes/hot_threads"),
            new Route(GET, "/_nodes/{nodeId}/hot_threads")
        ));
    }

    @Override
    public String getName() {
        return "nodes_hot_threads_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        String[] nodesIds = Strings.splitStringByCommaToArray(request.param("nodeId"));
        NodesHotThreadsRequest nodesHotThreadsRequest = new NodesHotThreadsRequest(nodesIds);
        nodesHotThreadsRequest.threads(request.paramAsInt("threads", nodesHotThreadsRequest.threads()));
        nodesHotThreadsRequest.ignoreIdleThreads(request.paramAsBoolean("ignore_idle_threads", nodesHotThreadsRequest.ignoreIdleThreads()));
        nodesHotThreadsRequest.type(request.param("type", nodesHotThreadsRequest.type()));
        nodesHotThreadsRequest.interval(TimeValue.parseTimeValue(request.param("interval"), nodesHotThreadsRequest.interval(), "interval"));
        nodesHotThreadsRequest.snapshots(request.paramAsInt("snapshots", nodesHotThreadsRequest.snapshots()));
        nodesHotThreadsRequest.timeout(request.param("timeout"));
        return channel -> client.admin().cluster().nodesHotThreads(
                nodesHotThreadsRequest,
                new RestResponseListener<NodesHotThreadsResponse>(channel) {
                    @Override
                    public RestResponse buildResponse(NodesHotThreadsResponse response) throws Exception {
                        StringBuilder sb = new StringBuilder();
                        for (NodeHotThreads node : response.getNodes()) {
                            sb.append("::: ").append(node.getNode().toString()).append("\n");
                            Strings.spaceify(3, node.getHotThreads(), sb);
                            sb.append('\n');
                        }
                        return new BytesRestResponse(RestStatus.OK, sb.toString());
                    }
                });
    }

    @Override
    public boolean canTripCircuitBreaker() {
        return false;
    }
}

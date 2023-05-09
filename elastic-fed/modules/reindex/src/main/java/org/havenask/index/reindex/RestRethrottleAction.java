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

package org.havenask.index.reindex;

import org.havenask.client.node.NodeClient;
import org.havenask.cluster.node.DiscoveryNodes;
import org.havenask.rest.BaseRestHandler;
import org.havenask.rest.RestRequest;
import org.havenask.tasks.TaskId;

import java.util.List;
import java.util.function.Supplier;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.havenask.rest.RestRequest.Method.POST;
import static org.havenask.rest.action.admin.cluster.RestListTasksAction.listTasksResponseListener;

public class RestRethrottleAction extends BaseRestHandler {
    private final Supplier<DiscoveryNodes> nodesInCluster;

    public RestRethrottleAction(Supplier<DiscoveryNodes> nodesInCluster) {
        this.nodesInCluster = nodesInCluster;
    }

    @Override
    public List<Route> routes() {
        return unmodifiableList(asList(
            new Route(POST, "/_update_by_query/{taskId}/_rethrottle"),
            new Route(POST, "/_delete_by_query/{taskId}/_rethrottle"),
            new Route(POST, "/_reindex/{taskId}/_rethrottle")));
    }

    @Override
    public String getName() {
        return "rethrottle_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) {
        RethrottleRequest internalRequest = new RethrottleRequest();
        internalRequest.setTaskId(new TaskId(request.param("taskId")));
        Float requestsPerSecond = AbstractBaseReindexRestHandler.parseRequestsPerSecond(request);
        if (requestsPerSecond == null) {
            throw new IllegalArgumentException("requests_per_second is a required parameter");
        }
        internalRequest.setRequestsPerSecond(requestsPerSecond);
        final String groupBy = request.param("group_by", "nodes");
        return channel ->
            client.execute(RethrottleAction.INSTANCE, internalRequest, listTasksResponseListener(nodesInCluster, groupBy, channel));
    }
}

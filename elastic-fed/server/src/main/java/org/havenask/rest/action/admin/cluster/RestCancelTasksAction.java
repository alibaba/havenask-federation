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

import org.havenask.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.havenask.client.node.NodeClient;
import org.havenask.cluster.node.DiscoveryNodes;
import org.havenask.common.Strings;
import org.havenask.rest.BaseRestHandler;
import org.havenask.rest.RestRequest;
import org.havenask.tasks.TaskId;

import java.io.IOException;
import java.util.List;
import java.util.function.Supplier;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.havenask.rest.RestRequest.Method.POST;
import static org.havenask.rest.action.admin.cluster.RestListTasksAction.listTasksResponseListener;


public class RestCancelTasksAction extends BaseRestHandler {
    private final Supplier<DiscoveryNodes> nodesInCluster;

    public RestCancelTasksAction(Supplier<DiscoveryNodes> nodesInCluster) {
        this.nodesInCluster = nodesInCluster;
    }

    @Override
    public String getName() {
        return "cancel_tasks_action";
    }

    @Override
    public List<Route> routes() {
        return unmodifiableList(asList(new Route(POST, "/_tasks/_cancel"),
            new Route(POST, "/_tasks/{task_id}/_cancel")));
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final String[] nodesIds = Strings.splitStringByCommaToArray(request.param("nodes"));
        final TaskId taskId = new TaskId(request.param("task_id"));
        final String[] actions = Strings.splitStringByCommaToArray(request.param("actions"));
        final TaskId parentTaskId = new TaskId(request.param("parent_task_id"));
        final String groupBy = request.param("group_by", "nodes");

        CancelTasksRequest cancelTasksRequest = new CancelTasksRequest();
        cancelTasksRequest.setTaskId(taskId);
        cancelTasksRequest.setNodes(nodesIds);
        cancelTasksRequest.setActions(actions);
        cancelTasksRequest.setParentTaskId(parentTaskId);
        cancelTasksRequest.setWaitForCompletion(request.paramAsBoolean("wait_for_completion", cancelTasksRequest.waitForCompletion()));
        return channel ->
            client.admin().cluster().cancelTasks(cancelTasksRequest, listTasksResponseListener(nodesInCluster, groupBy, channel));
    }

    @Override
    public boolean canTripCircuitBreaker() {
        return false;
    }

}

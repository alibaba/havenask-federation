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

import org.havenask.action.ActionListener;
import org.havenask.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.havenask.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.havenask.client.node.NodeClient;
import org.havenask.cluster.node.DiscoveryNodes;
import org.havenask.common.Strings;
import org.havenask.common.unit.TimeValue;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.rest.BaseRestHandler;
import org.havenask.rest.BytesRestResponse;
import org.havenask.rest.RestChannel;
import org.havenask.rest.RestRequest;
import org.havenask.rest.RestResponse;
import org.havenask.rest.RestStatus;
import org.havenask.rest.action.RestBuilderListener;
import org.havenask.rest.action.RestToXContentListener;
import org.havenask.tasks.TaskId;

import java.io.IOException;
import java.util.List;
import java.util.function.Supplier;

import static java.util.Collections.singletonList;
import static org.havenask.rest.RestRequest.Method.GET;


public class RestListTasksAction extends BaseRestHandler {

    private final Supplier<DiscoveryNodes> nodesInCluster;

    public RestListTasksAction(Supplier<DiscoveryNodes> nodesInCluster) {
        this.nodesInCluster = nodesInCluster;
    }

    @Override
    public List<Route> routes() {
        return singletonList(new Route(GET, "/_tasks"));
    }

    @Override
    public String getName() {
        return "list_tasks_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final ListTasksRequest listTasksRequest = generateListTasksRequest(request);
        final String groupBy = request.param("group_by", "nodes");
        return channel -> client.admin().cluster().listTasks(listTasksRequest,
                listTasksResponseListener(nodesInCluster, groupBy, channel));
    }

    public static ListTasksRequest generateListTasksRequest(RestRequest request) {
        boolean detailed = request.paramAsBoolean("detailed", false);
        String[] nodes = Strings.splitStringByCommaToArray(request.param("nodes"));
        String[] actions = Strings.splitStringByCommaToArray(request.param("actions"));
        TaskId parentTaskId = new TaskId(request.param("parent_task_id"));
        boolean waitForCompletion = request.paramAsBoolean("wait_for_completion", false);
        TimeValue timeout = request.paramAsTime("timeout", null);

        ListTasksRequest listTasksRequest = new ListTasksRequest();
        listTasksRequest.setNodes(nodes);
        listTasksRequest.setDetailed(detailed);
        listTasksRequest.setActions(actions);
        listTasksRequest.setParentTaskId(parentTaskId);
        listTasksRequest.setWaitForCompletion(waitForCompletion);
        listTasksRequest.setTimeout(timeout);
        return listTasksRequest;
    }

    /**
     * Standard listener for extensions of {@link ListTasksResponse} that supports {@code group_by=nodes}.
     */
    public static <T extends ListTasksResponse> ActionListener<T> listTasksResponseListener(
                Supplier<DiscoveryNodes> nodesInCluster,
                String groupBy,
                final RestChannel channel) {
        if ("nodes".equals(groupBy)) {
            return new RestBuilderListener<T>(channel) {
                @Override
                public RestResponse buildResponse(T response, XContentBuilder builder) throws Exception {
                    builder.startObject();
                    response.toXContentGroupedByNode(builder, channel.request(), nodesInCluster.get());
                    builder.endObject();
                    return new BytesRestResponse(RestStatus.OK, builder);
                }
            };
        } else if ("parents".equals(groupBy)) {
            return new RestBuilderListener<T>(channel) {
                @Override
                public RestResponse buildResponse(T response, XContentBuilder builder) throws Exception {
                    builder.startObject();
                    response.toXContentGroupedByParents(builder, channel.request());
                    builder.endObject();
                    return new BytesRestResponse(RestStatus.OK, builder);
                }
            };
        } else if ("none".equals(groupBy)) {
            return new RestToXContentListener<>(channel);
        } else {
            throw new IllegalArgumentException("[group_by] must be one of [nodes], [parents] or [none] but was [" + groupBy + "]");
        }
    }

    @Override
    public boolean canTripCircuitBreaker() {
        return false;
    }
}

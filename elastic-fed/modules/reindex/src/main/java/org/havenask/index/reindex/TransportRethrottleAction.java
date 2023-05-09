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

import org.apache.logging.log4j.Logger;
import org.havenask.action.ActionListener;
import org.havenask.action.FailedNodeException;
import org.havenask.action.TaskOperationFailure;
import org.havenask.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.havenask.action.support.ActionFilters;
import org.havenask.action.support.tasks.TransportTasksAction;
import org.havenask.client.Client;
import org.havenask.cluster.service.ClusterService;
import org.havenask.common.inject.Inject;
import org.havenask.tasks.TaskId;
import org.havenask.tasks.TaskInfo;
import org.havenask.threadpool.ThreadPool;
import org.havenask.transport.TransportService;

import java.util.List;

public class TransportRethrottleAction extends TransportTasksAction<BulkByScrollTask, RethrottleRequest, ListTasksResponse, TaskInfo> {
    private final Client client;

    @Inject
    public TransportRethrottleAction(ClusterService clusterService, TransportService transportService,
                                     ActionFilters actionFilters, Client client) {
        super(RethrottleAction.NAME, clusterService, transportService, actionFilters,
            RethrottleRequest::new, ListTasksResponse::new, TaskInfo::new, ThreadPool.Names.MANAGEMENT);
        this.client = client;
    }

    @Override
    protected void taskOperation(RethrottleRequest request, BulkByScrollTask task, ActionListener<TaskInfo> listener) {
        rethrottle(logger, clusterService.localNode().getId(), client, task, request.getRequestsPerSecond(), listener);
    }

    static void rethrottle(Logger logger, String localNodeId, Client client, BulkByScrollTask task, float newRequestsPerSecond,
            ActionListener<TaskInfo> listener) {

        if (task.isWorker()) {
            rethrottleChildTask(logger, localNodeId, task, newRequestsPerSecond, listener);
            return;
        }

        if (task.isLeader()) {
            rethrottleParentTask(logger, localNodeId, client, task, newRequestsPerSecond, listener);
            return;
        }

        throw new IllegalArgumentException("task [" + task.getId() + "] has not yet been initialized to the point where it knows how to " +
            "rethrottle itself");
    }

    private static void rethrottleParentTask(Logger logger, String localNodeId, Client client, BulkByScrollTask task,
                                             float newRequestsPerSecond, ActionListener<TaskInfo> listener) {
        final LeaderBulkByScrollTaskState leaderState = task.getLeaderState();
        final int runningSubtasks = leaderState.runningSliceSubTasks();

        if (runningSubtasks > 0) {
            RethrottleRequest subRequest = new RethrottleRequest();
            subRequest.setRequestsPerSecond(newRequestsPerSecond / runningSubtasks);
            subRequest.setParentTaskId(new TaskId(localNodeId, task.getId()));
            logger.debug("rethrottling children of task [{}] to [{}] requests per second", task.getId(),
                subRequest.getRequestsPerSecond());
            client.execute(RethrottleAction.INSTANCE, subRequest, ActionListener.wrap(
                r -> {
                    r.rethrowFailures("Rethrottle");
                    listener.onResponse(task.taskInfoGivenSubtaskInfo(localNodeId, r.getTasks()));
                },
                listener::onFailure));
        } else {
            logger.debug("children of task [{}] are already finished, nothing to rethrottle", task.getId());
            listener.onResponse(task.taskInfo(localNodeId, true));
        }
    }

    private static void rethrottleChildTask(Logger logger, String localNodeId, BulkByScrollTask task, float newRequestsPerSecond,
                                            ActionListener<TaskInfo> listener) {
        logger.debug("rethrottling local task [{}] to [{}] requests per second", task.getId(), newRequestsPerSecond);
        task.getWorkerState().rethrottle(newRequestsPerSecond);
        listener.onResponse(task.taskInfo(localNodeId, true));
    }

    @Override
    protected ListTasksResponse newResponse(RethrottleRequest request, List<TaskInfo> tasks,
            List<TaskOperationFailure> taskOperationFailures, List<FailedNodeException> failedNodeExceptions) {
        return new ListTasksResponse(tasks, taskOperationFailures, failedNodeExceptions);
    }

}

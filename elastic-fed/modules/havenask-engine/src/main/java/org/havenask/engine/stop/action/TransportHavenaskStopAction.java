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

package org.havenask.engine.stop.action;

import org.havenask.action.FailedNodeException;
import org.havenask.action.support.ActionFilters;
import org.havenask.action.support.nodes.TransportNodesAction;
import org.havenask.cluster.service.ClusterService;
import org.havenask.common.inject.Inject;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.unit.TimeValue;
import org.havenask.engine.NativeProcessControlService;
import org.havenask.rest.RestStatus;
import org.havenask.threadpool.ThreadPool;
import org.havenask.transport.TransportService;

import java.io.IOException;
import java.util.List;

public class TransportHavenaskStopAction extends TransportNodesAction<
    HavenaskStopRequest,
    HavenaskStopResponse,
    HavenaskStopNodeRequest,
    HavenaskStopNodeResponse> {

    private static final String stopSearcherCommand = "ps -ef | grep searcher | grep -v grep | awk '{print $2}' | xargs kill -9";
    private static final String stopQrsCommand = "ps -ef | grep qrs | grep -v grep | awk '{print $2}' | xargs kill -9";
    private final TimeValue commandTimeout;

    @Inject
    public TransportHavenaskStopAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters
    ) {
        super(
            HavenaskStopAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            HavenaskStopRequest::new,
            HavenaskStopNodeRequest::new,
            ThreadPool.Names.MANAGEMENT,
            ThreadPool.Names.MANAGEMENT,
            HavenaskStopNodeResponse.class
        );
        commandTimeout = TimeValue.timeValueSeconds(10);
    }

    @Override
    protected HavenaskStopResponse newResponse(
        HavenaskStopRequest request,
        List<HavenaskStopNodeResponse> responses,
        List<FailedNodeException> failures
    ) {
        return new HavenaskStopResponse(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected HavenaskStopNodeRequest newNodeRequest(HavenaskStopRequest request) {
        return new HavenaskStopNodeRequest(request);
    }

    @Override
    protected HavenaskStopNodeResponse newNodeResponse(StreamInput in) throws IOException {
        return new HavenaskStopNodeResponse(in);
    }

    @Override
    protected HavenaskStopNodeResponse nodeOperation(HavenaskStopNodeRequest request) {
        String role = request.getRole();
        StringBuilder result = new StringBuilder("target stop role: " + role + "; ");
        // use shell to kill the process
        try {
            if (role.equals("searcher") || role.equals("all")) {
                boolean success = NativeProcessControlService.runCommand(stopSearcherCommand, commandTimeout);
                if (success) result.append("run stopping searcher command success; ");
                else result.append("stop searcher failed; ");
            }
            if (role.equals("qrs") || role.equals("all")) {
                boolean success = NativeProcessControlService.runCommand(stopQrsCommand, commandTimeout);
                if (success) result.append("run stopping qrs command success; ");
                else result.append("stop qrs failed; ");
            }
            return new HavenaskStopNodeResponse(clusterService.localNode(), result.toString(), RestStatus.OK.getStatus());
        } catch (Exception e) {
            return new HavenaskStopNodeResponse(
                clusterService.localNode(),
                "exception occur :" + e,
                RestStatus.EXPECTATION_FAILED.getStatus()
            );
        }
    }
}

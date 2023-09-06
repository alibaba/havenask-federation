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

import org.havenask.action.ActionListener;
import org.havenask.action.support.ActionFilters;
import org.havenask.action.support.HandledTransportAction;
import org.havenask.common.inject.Inject;
import org.havenask.common.unit.TimeValue;
import org.havenask.engine.NativeProcessControlService;
import org.havenask.rest.RestStatus;
import org.havenask.tasks.Task;
import org.havenask.threadpool.ThreadPool;
import org.havenask.transport.TransportService;

// todo: stop接口需要继承TransportNodesAction，HandledTransportAction只适合单机版使用
public class TransportHavenaskStopAction extends HandledTransportAction<HavenaskStopRequest, HavenaskStopResponse> {

    private static final String stopSearcherCommand = "ps -ef | grep searcher | grep -v grep | awk '{print $2}' | xargs kill -9";
    private static final String stopQrsCommand = "ps -ef | grep qrs | grep -v grep | awk '{print $2}' | xargs kill -9";
    private final TimeValue commandTimeout;

    @Inject
    public TransportHavenaskStopAction(TransportService transportService, ActionFilters actionFilters) {
        super(HavenaskStopAction.NAME, transportService, actionFilters, HavenaskStopRequest::new, ThreadPool.Names.SEARCH);
        commandTimeout = TimeValue.timeValueSeconds(10);
    }

    @Override
    protected void doExecute(Task task, HavenaskStopRequest request, ActionListener<HavenaskStopResponse> listener) {
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
            listener.onResponse(new HavenaskStopResponse(result.toString(), RestStatus.OK.getStatus()));
        } catch (Exception e) {
            listener.onResponse(new HavenaskStopResponse("exception occur :" + e, RestStatus.EXPECTATION_FAILED.getStatus()));
        }

    }
}

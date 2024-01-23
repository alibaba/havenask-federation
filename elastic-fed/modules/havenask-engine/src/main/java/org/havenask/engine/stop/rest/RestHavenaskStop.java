/*
 * Copyright (c) 2021, Alibaba Group;
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.havenask.engine.stop.rest;

import org.havenask.client.node.NodeClient;
import org.havenask.engine.stop.action.HavenaskStopAction;
import org.havenask.engine.stop.action.HavenaskStopRequest;

import org.havenask.rest.BaseRestHandler;
import org.havenask.rest.RestRequest;
import org.havenask.rest.RestRequest.Method;

import java.util.List;

public class RestHavenaskStop extends BaseRestHandler {

    @Override
    public String getName() {
        return "havenask_stop_searcher_action";
    }

    public List<Route> routes() {
        return List.of(new Route(Method.POST, "/_havenask/stop"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        // role can be "searcher", "qrs", "all"
        String role = request.param("role");
        String[] nodeIds = request.paramAsStringArray("node", null);
        HavenaskStopRequest havenaskStopRequest = new HavenaskStopRequest(role, nodeIds);
        return channel -> client.admin().cluster().execute(HavenaskStopAction.INSTANCE, havenaskStopRequest);
    }

}

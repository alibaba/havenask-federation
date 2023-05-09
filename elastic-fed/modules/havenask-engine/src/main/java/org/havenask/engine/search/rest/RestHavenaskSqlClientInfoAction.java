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

package org.havenask.engine.search.rest;

import java.util.List;

import org.havenask.client.node.NodeClient;
import org.havenask.engine.search.action.HavenaskSqlClientInfoAction;
import org.havenask.rest.BaseRestHandler;
import org.havenask.rest.RestRequest;
import org.havenask.rest.RestRequest.Method;
import org.havenask.rest.action.RestToXContentListener;

public class RestHavenaskSqlClientInfoAction extends BaseRestHandler {
    @Override
    public String getName() {
        return "havenask_sql_info_action";
    }

    public List<Route> routes() {
        return List.of(new Route(Method.GET, "/_havenask/sql_info"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        HavenaskSqlClientInfoAction.Request havenaskSqlRequest = new HavenaskSqlClientInfoAction.Request();
        return channel -> client.execute(HavenaskSqlClientInfoAction.INSTANCE, havenaskSqlRequest, new RestToXContentListener<>(channel));
    }
}

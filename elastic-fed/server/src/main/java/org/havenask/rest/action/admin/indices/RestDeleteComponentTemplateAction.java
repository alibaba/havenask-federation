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

package org.havenask.rest.action.admin.indices;

import org.havenask.action.admin.indices.template.delete.DeleteComponentTemplateAction;
import org.havenask.client.node.NodeClient;
import org.havenask.rest.BaseRestHandler;
import org.havenask.rest.RestRequest;
import org.havenask.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.havenask.rest.RestRequest.Method.DELETE;

public class RestDeleteComponentTemplateAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return Collections.singletonList(new Route(DELETE, "/_component_template/{name}"));
    }

    @Override
    public String getName() {
        return "delete_component_template_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {

        DeleteComponentTemplateAction.Request deleteReq = new DeleteComponentTemplateAction.Request(request.param("name"));
        deleteReq.masterNodeTimeout(request.paramAsTime("master_timeout", deleteReq.masterNodeTimeout()));

        return channel -> client.execute(DeleteComponentTemplateAction.INSTANCE, deleteReq, new RestToXContentListener<>(channel));
    }
}

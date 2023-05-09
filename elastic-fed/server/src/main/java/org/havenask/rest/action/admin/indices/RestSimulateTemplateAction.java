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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import org.havenask.action.admin.indices.template.post.SimulateTemplateAction;
import org.havenask.action.admin.indices.template.put.PutComposableIndexTemplateAction;
import org.havenask.client.node.NodeClient;
import org.havenask.cluster.metadata.ComposableIndexTemplate;
import org.havenask.rest.BaseRestHandler;
import org.havenask.rest.RestRequest;
import org.havenask.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.havenask.rest.RestRequest.Method.POST;

public class RestSimulateTemplateAction extends BaseRestHandler {
    @Override
    public List<Route> routes() {
        return Arrays.asList(
            new Route(POST, "/_index_template/_simulate"),
            new Route(POST, "/_index_template/_simulate/{name}"));
    }

    @Override
    public String getName() {
        return "simulate_template_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        SimulateTemplateAction.Request simulateRequest = new SimulateTemplateAction.Request();
        simulateRequest.templateName(request.param("name"));
        if (request.hasContent()) {
            PutComposableIndexTemplateAction.Request indexTemplateRequest =
                new PutComposableIndexTemplateAction.Request("simulating_template");
            indexTemplateRequest.indexTemplate(ComposableIndexTemplate.parse(request.contentParser()));
            indexTemplateRequest.create(request.paramAsBoolean("create", false));
            indexTemplateRequest.cause(request.param("cause", "api"));

            simulateRequest.indexTemplateRequest(indexTemplateRequest);
        }
        simulateRequest.masterNodeTimeout(request.paramAsTime("master_timeout", simulateRequest.masterNodeTimeout()));

        return channel -> client.execute(SimulateTemplateAction.INSTANCE, simulateRequest, new RestToXContentListener<>(channel));
    }
}

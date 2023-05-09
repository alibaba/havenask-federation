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

import org.havenask.action.admin.indices.template.get.GetComposableIndexTemplateAction;
import org.havenask.client.node.NodeClient;
import org.havenask.common.settings.Settings;
import org.havenask.rest.BaseRestHandler;
import org.havenask.rest.RestRequest;
import org.havenask.rest.RestStatus;
import org.havenask.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static org.havenask.rest.RestRequest.Method.GET;
import static org.havenask.rest.RestRequest.Method.HEAD;
import static org.havenask.rest.RestStatus.NOT_FOUND;
import static org.havenask.rest.RestStatus.OK;

public class RestGetComposableIndexTemplateAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return Arrays.asList(
            new Route(GET, "/_index_template"),
            new Route(GET, "/_index_template/{name}"),
            new Route(HEAD, "/_index_template/{name}"));
    }

    @Override
    public String getName() {
        return "get_composable_index_template_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final GetComposableIndexTemplateAction.Request getRequest = new GetComposableIndexTemplateAction.Request(request.param("name"));

        getRequest.local(request.paramAsBoolean("local", getRequest.local()));
        getRequest.masterNodeTimeout(request.paramAsTime("master_timeout", getRequest.masterNodeTimeout()));

        final boolean implicitAll = getRequest.name() == null;

        return channel ->
            client.execute(GetComposableIndexTemplateAction.INSTANCE, getRequest,
                new RestToXContentListener<GetComposableIndexTemplateAction.Response>(channel) {
                    @Override
                    protected RestStatus getStatus(final GetComposableIndexTemplateAction.Response response) {
                        final boolean templateExists = response.indexTemplates().isEmpty() == false;
                        return (templateExists || implicitAll) ? OK : NOT_FOUND;
                    }
                });
    }

    @Override
    protected Set<String> responseParams() {
        return Settings.FORMAT_PARAMS;
    }

}

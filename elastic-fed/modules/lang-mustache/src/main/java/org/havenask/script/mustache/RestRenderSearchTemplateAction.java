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

package org.havenask.script.mustache;

import org.havenask.client.node.NodeClient;
import org.havenask.common.xcontent.XContentParser;
import org.havenask.rest.BaseRestHandler;
import org.havenask.rest.RestRequest;
import org.havenask.rest.action.RestToXContentListener;
import org.havenask.script.ScriptType;

import java.io.IOException;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.havenask.rest.RestRequest.Method.GET;
import static org.havenask.rest.RestRequest.Method.POST;

public class RestRenderSearchTemplateAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return unmodifiableList(asList(
            new Route(GET, "/_render/template"),
            new Route(POST, "/_render/template"),
            new Route(GET, "/_render/template/{id}"),
            new Route(POST, "/_render/template/{id}")));
    }

    @Override
    public String getName() {
        return "render_search_template_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        // Creates the render template request
        SearchTemplateRequest renderRequest;
        try (XContentParser parser = request.contentOrSourceParamParser()) {
            renderRequest = SearchTemplateRequest.fromXContent(parser);
        }
        renderRequest.setSimulate(true);

        String id = request.param("id");
        if (id != null) {
            renderRequest.setScriptType(ScriptType.STORED);
            renderRequest.setScript(id);
        }

        return channel -> client.execute(SearchTemplateAction.INSTANCE, renderRequest, new RestToXContentListener<>(channel));
    }
}

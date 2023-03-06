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

import org.havenask.action.admin.cluster.storedscripts.PutStoredScriptRequest;
import org.havenask.client.node.NodeClient;
import org.havenask.common.bytes.BytesReference;
import org.havenask.common.xcontent.XContentType;
import org.havenask.rest.BaseRestHandler;
import org.havenask.rest.RestRequest;
import org.havenask.rest.action.RestToXContentListener;
import org.havenask.script.StoredScriptSource;

import java.io.IOException;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.havenask.rest.RestRequest.Method.POST;
import static org.havenask.rest.RestRequest.Method.PUT;

public class RestPutStoredScriptAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return unmodifiableList(asList(
            new Route(POST, "/_scripts/{id}"),
            new Route(PUT, "/_scripts/{id}"),
            new Route(POST, "/_scripts/{id}/{context}"),
            new Route(PUT, "/_scripts/{id}/{context}")));
    }

    @Override
    public String getName() {
        return "put_stored_script_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String id = request.param("id");
        String context = request.param("context");
        BytesReference content = request.requiredContent();
        XContentType xContentType = request.getXContentType();
        StoredScriptSource source = StoredScriptSource.parse(content, xContentType);

        PutStoredScriptRequest putRequest = new PutStoredScriptRequest(id, context, content, request.getXContentType(), source);
        putRequest.masterNodeTimeout(request.paramAsTime("master_timeout", putRequest.masterNodeTimeout()));
        putRequest.timeout(request.paramAsTime("timeout", putRequest.timeout()));
        return channel -> client.admin().cluster().putStoredScript(putRequest, new RestToXContentListener<>(channel));
    }
}

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

import org.havenask.action.admin.cluster.repositories.cleanup.CleanupRepositoryRequest;
import org.havenask.client.node.NodeClient;
import org.havenask.rest.BaseRestHandler;
import org.havenask.rest.RestRequest;
import org.havenask.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.havenask.client.Requests.cleanupRepositoryRequest;
import static org.havenask.rest.RestRequest.Method.POST;

/**
 * Cleans up a repository
 */
public class RestCleanupRepositoryAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return singletonList(new Route(POST, "/_snapshot/{repository}/_cleanup"));
    }

    @Override
    public String getName() {
        return "cleanup_repository_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        CleanupRepositoryRequest cleanupRepositoryRequest = cleanupRepositoryRequest(request.param("repository"));
        cleanupRepositoryRequest.timeout(request.paramAsTime("timeout", cleanupRepositoryRequest.timeout()));
        cleanupRepositoryRequest.masterNodeTimeout(request.paramAsTime("master_timeout", cleanupRepositoryRequest.masterNodeTimeout()));
        return channel -> client.admin().cluster().cleanupRepository(cleanupRepositoryRequest, new RestToXContentListener<>(channel));
    }
}

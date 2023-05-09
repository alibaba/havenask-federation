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

package org.havenask.rest.action.cat;

import org.havenask.action.admin.cluster.repositories.get.GetRepositoriesRequest;
import org.havenask.action.admin.cluster.repositories.get.GetRepositoriesResponse;
import org.havenask.client.node.NodeClient;
import org.havenask.cluster.metadata.RepositoryMetadata;
import org.havenask.common.Table;
import org.havenask.rest.RestRequest;
import org.havenask.rest.RestResponse;
import org.havenask.rest.action.RestResponseListener;

import java.util.List;

import static java.util.Collections.singletonList;
import static org.havenask.rest.RestRequest.Method.GET;

/**
 * Cat API class to display information about snapshot repositories
 */
public class RestRepositoriesAction extends AbstractCatAction {

    @Override
    public List<Route> routes() {
        return singletonList(new Route(GET, "/_cat/repositories"));
    }

    @Override
    protected RestChannelConsumer doCatRequest(RestRequest request, NodeClient client) {
        GetRepositoriesRequest getRepositoriesRequest = new GetRepositoriesRequest();
        getRepositoriesRequest.local(request.paramAsBoolean("local", getRepositoriesRequest.local()));
        getRepositoriesRequest.masterNodeTimeout(request.paramAsTime("master_timeout", getRepositoriesRequest.masterNodeTimeout()));

        return channel ->
                client.admin()
                        .cluster()
                        .getRepositories(getRepositoriesRequest, new RestResponseListener<GetRepositoriesResponse>(channel) {
                            @Override
                            public RestResponse buildResponse(GetRepositoriesResponse getRepositoriesResponse) throws Exception {
                                return RestTable.buildResponse(buildTable(request, getRepositoriesResponse), channel);
                            }
                        });
    }

    @Override
    public String getName() {
        return "cat_repositories_action";
    }

    @Override
    protected void documentation(StringBuilder sb) {
        sb.append("/_cat/repositories\n");
    }

    @Override
    protected Table getTableWithHeader(RestRequest request) {
        return new Table()
                .startHeaders()
                .addCell("id", "alias:id,repoId;desc:unique repository id")
                .addCell("type", "alias:t,type;text-align:right;desc:repository type")
                .endHeaders();
    }

    private Table buildTable(RestRequest req, GetRepositoriesResponse getRepositoriesResponse) {
        Table table = getTableWithHeader(req);
        for (RepositoryMetadata repositoryMetadata : getRepositoriesResponse.repositories()) {
            table.startRow();

            table.addCell(repositoryMetadata.name());
            table.addCell(repositoryMetadata.type());

            table.endRow();
        }

        return table;
    }
}

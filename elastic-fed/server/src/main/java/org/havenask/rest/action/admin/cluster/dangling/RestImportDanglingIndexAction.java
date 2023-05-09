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

package org.havenask.rest.action.admin.cluster.dangling;

import static java.util.Collections.singletonList;
import static org.havenask.rest.RestRequest.Method.POST;
import static org.havenask.rest.RestStatus.ACCEPTED;

import java.io.IOException;
import java.util.List;

import org.havenask.action.admin.indices.dangling.import_index.ImportDanglingIndexRequest;
import org.havenask.action.support.master.AcknowledgedResponse;
import org.havenask.client.node.NodeClient;
import org.havenask.rest.BaseRestHandler;
import org.havenask.rest.RestRequest;
import org.havenask.rest.RestStatus;
import org.havenask.rest.action.RestToXContentListener;

public class RestImportDanglingIndexAction extends BaseRestHandler {
    @Override
    public List<Route> routes() {
        return singletonList(new Route(POST, "/_dangling/{index_uuid}"));
    }

    @Override
    public String getName() {
        return "import_dangling_index";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, NodeClient client) throws IOException {
        final ImportDanglingIndexRequest importRequest = new ImportDanglingIndexRequest(
            request.param("index_uuid"),
            request.paramAsBoolean("accept_data_loss", false)
        );

        importRequest.timeout(request.paramAsTime("timeout", importRequest.timeout()));
        importRequest.masterNodeTimeout(request.paramAsTime("master_timeout", importRequest.masterNodeTimeout()));

        return channel -> client.admin()
            .cluster()
            .importDanglingIndex(importRequest, new RestToXContentListener<AcknowledgedResponse>(channel) {
                @Override
                protected RestStatus getStatus(AcknowledgedResponse acknowledgedResponse) {
                    return ACCEPTED;
                }
            });
    }
}

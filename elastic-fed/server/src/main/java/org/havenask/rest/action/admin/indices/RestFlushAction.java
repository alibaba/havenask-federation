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

import org.havenask.action.admin.indices.flush.FlushRequest;
import org.havenask.action.support.IndicesOptions;
import org.havenask.client.node.NodeClient;
import org.havenask.common.Strings;
import org.havenask.rest.BaseRestHandler;
import org.havenask.rest.RestRequest;
import org.havenask.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.havenask.rest.RestRequest.Method.GET;
import static org.havenask.rest.RestRequest.Method.POST;

public class RestFlushAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return unmodifiableList(asList(
            new Route(GET, "/_flush"),
            new Route(POST, "/_flush"),
            new Route(GET, "/{index}/_flush"),
            new Route(POST, "/{index}/_flush")));
    }

    @Override
    public String getName() {
        return "flush_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        FlushRequest flushRequest = new FlushRequest(Strings.splitStringByCommaToArray(request.param("index")));
        flushRequest.indicesOptions(IndicesOptions.fromRequest(request, flushRequest.indicesOptions()));
        flushRequest.force(request.paramAsBoolean("force", flushRequest.force()));
        flushRequest.waitIfOngoing(request.paramAsBoolean("wait_if_ongoing", flushRequest.waitIfOngoing()));
        return channel -> client.admin().indices().flush(flushRequest, new RestToXContentListener<>(channel));
    }
}

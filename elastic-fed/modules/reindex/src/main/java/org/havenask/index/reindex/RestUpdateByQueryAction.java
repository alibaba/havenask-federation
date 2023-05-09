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

package org.havenask.index.reindex;

import org.havenask.client.node.NodeClient;
import org.havenask.common.io.stream.NamedWriteableRegistry;
import org.havenask.rest.RestRequest;
import org.havenask.script.Script;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.havenask.rest.RestRequest.Method.POST;

public class RestUpdateByQueryAction extends AbstractBulkByQueryRestHandler<UpdateByQueryRequest, UpdateByQueryAction> {

    public RestUpdateByQueryAction() {
        super(UpdateByQueryAction.INSTANCE);
    }

    @Override
    public List<Route> routes() {
        return unmodifiableList(asList(
            new Route(POST, "/{index}/_update_by_query"),
            new Route(POST, "/{index}/{type}/_update_by_query")));
    }

    @Override
    public String getName() {
        return "update_by_query_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        return doPrepareRequest(request, client, false, true);
    }

    @Override
    protected UpdateByQueryRequest buildRequest(RestRequest request, NamedWriteableRegistry namedWriteableRegistry) throws IOException {
        /*
         * Passing the search request through UpdateByQueryRequest first allows
         * it to set its own defaults which differ from SearchRequest's
         * defaults. Then the parse can override them.
         */
        UpdateByQueryRequest internal = new UpdateByQueryRequest();

        Map<String, Consumer<Object>> consumers = new HashMap<>();
        consumers.put("conflicts", o -> internal.setConflicts((String) o));
        consumers.put("script", o -> internal.setScript(Script.parse(o)));
        consumers.put("max_docs", s -> setMaxDocsValidateIdentical(internal, ((Number) s).intValue()));

        parseInternalRequest(internal, request, namedWriteableRegistry, consumers);

        internal.setPipeline(request.param("pipeline"));
        return internal;
    }
}

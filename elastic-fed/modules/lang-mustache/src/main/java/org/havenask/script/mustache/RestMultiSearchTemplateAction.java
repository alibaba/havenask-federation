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
import org.havenask.common.logging.DeprecationLogger;
import org.havenask.common.settings.Settings;
import org.havenask.rest.BaseRestHandler;
import org.havenask.rest.RestRequest;
import org.havenask.rest.action.RestToXContentListener;
import org.havenask.rest.action.search.RestMultiSearchAction;
import org.havenask.rest.action.search.RestSearchAction;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.havenask.rest.RestRequest.Method.GET;
import static org.havenask.rest.RestRequest.Method.POST;

public class RestMultiSearchTemplateAction extends BaseRestHandler {
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RestMultiSearchTemplateAction.class);
    static final String TYPES_DEPRECATION_MESSAGE = "[types removal]" +
        " Specifying types in multi search template requests is deprecated.";

    private static final Set<String> RESPONSE_PARAMS;

    static {
        final Set<String> responseParams = new HashSet<>(
            asList(RestSearchAction.TYPED_KEYS_PARAM, RestSearchAction.TOTAL_HITS_AS_INT_PARAM)
        );
        RESPONSE_PARAMS = Collections.unmodifiableSet(responseParams);
    }


    private final boolean allowExplicitIndex;

    public RestMultiSearchTemplateAction(Settings settings) {
        this.allowExplicitIndex = MULTI_ALLOW_EXPLICIT_INDEX.get(settings);
    }

    @Override
    public List<Route> routes() {
        return unmodifiableList(asList(
            new Route(GET, "/_msearch/template"),
            new Route(POST, "/_msearch/template"),
            new Route(GET, "/{index}/_msearch/template"),
            new Route(POST, "/{index}/_msearch/template"),
            // Deprecated typed endpoints.
            new Route(GET, "/{index}/{type}/_msearch/template"),
            new Route(POST, "/{index}/{type}/_msearch/template")));
    }

    @Override
    public String getName() {
        return "multi_search_template_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        MultiSearchTemplateRequest multiRequest = parseRequest(request, allowExplicitIndex);

        // Emit a single deprecation message if any search template contains types.
        for (SearchTemplateRequest searchTemplateRequest : multiRequest.requests()) {
            if (searchTemplateRequest.getRequest().types().length > 0) {
                deprecationLogger.deprecate("msearch_with_types", TYPES_DEPRECATION_MESSAGE);
                break;
            }
        }
        return channel -> client.execute(MultiSearchTemplateAction.INSTANCE, multiRequest, new RestToXContentListener<>(channel));
    }

    /**
     * Parses a {@link RestRequest} body and returns a {@link MultiSearchTemplateRequest}
     */
    public static MultiSearchTemplateRequest parseRequest(RestRequest restRequest, boolean allowExplicitIndex) throws IOException {
        MultiSearchTemplateRequest multiRequest = new MultiSearchTemplateRequest();
        if (restRequest.hasParam("max_concurrent_searches")) {
            multiRequest.maxConcurrentSearchRequests(restRequest.paramAsInt("max_concurrent_searches", 0));
        }

        RestMultiSearchAction.parseMultiLineRequest(restRequest, multiRequest.indicesOptions(), allowExplicitIndex,
                (searchRequest, bytes) -> {
                    SearchTemplateRequest searchTemplateRequest = SearchTemplateRequest.fromXContent(bytes);
                    if (searchTemplateRequest.getScript() != null) {
                        searchTemplateRequest.setRequest(searchRequest);
                        multiRequest.add(searchTemplateRequest);
                    } else {
                        throw new IllegalArgumentException("Malformed search template");
                    }
                    RestSearchAction.checkRestTotalHits(restRequest, searchRequest);
                });
        return multiRequest;
    }

    @Override
    public boolean supportsContentStream() {
        return true;
    }

    @Override
    protected Set<String> responseParams() {
        return RESPONSE_PARAMS;
    }
}

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

package org.havenask.rest.action.search;

import org.havenask.action.search.SearchScrollRequest;
import org.havenask.client.node.NodeClient;
import org.havenask.rest.BaseRestHandler;
import org.havenask.rest.RestRequest;
import org.havenask.rest.action.RestStatusToXContentListener;
import org.havenask.search.Scroll;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.havenask.common.unit.TimeValue.parseTimeValue;
import static org.havenask.rest.RestRequest.Method.GET;
import static org.havenask.rest.RestRequest.Method.POST;

public class RestSearchScrollAction extends BaseRestHandler {
    private static final Set<String> RESPONSE_PARAMS = Collections.singleton(RestSearchAction.TOTAL_HITS_AS_INT_PARAM);

    @Override
    public String getName() {
        return "search_scroll_action";
    }

    @Override
    public List<Route> routes() {
        return unmodifiableList(asList(
            new Route(GET, "/_search/scroll"),
            new Route(POST, "/_search/scroll"),
            new Route(GET, "/_search/scroll/{scroll_id}"),
            new Route(POST, "/_search/scroll/{scroll_id}")));
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        String scrollId = request.param("scroll_id");
        SearchScrollRequest searchScrollRequest = new SearchScrollRequest();
        searchScrollRequest.scrollId(scrollId);
        String scroll = request.param("scroll");
        if (scroll != null) {
            searchScrollRequest.scroll(new Scroll(parseTimeValue(scroll, null, "scroll")));
        }

        request.withContentOrSourceParamParserOrNull(xContentParser -> {
            if (xContentParser != null) {
                // NOTE: if rest request with xcontent body has request parameters, values parsed from request body have the precedence
                try {
                    searchScrollRequest.fromXContent(xContentParser);
                } catch (IOException e) {
                    throw new IllegalArgumentException("Failed to parse request body", e);
                }
            }});
        return channel -> client.searchScroll(searchScrollRequest, new RestStatusToXContentListener<>(channel));
    }

    @Override
    protected Set<String> responseParams() {
        return RESPONSE_PARAMS;
    }
}

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

package org.havenask.rest.action.document;

import org.havenask.action.get.MultiGetRequest;
import org.havenask.client.node.NodeClient;
import org.havenask.common.Strings;
import org.havenask.common.logging.DeprecationLogger;
import org.havenask.common.settings.Settings;
import org.havenask.common.xcontent.XContentParser;
import org.havenask.rest.BaseRestHandler;
import org.havenask.rest.RestRequest;
import org.havenask.rest.action.RestToXContentListener;
import org.havenask.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.havenask.rest.RestRequest.Method.GET;
import static org.havenask.rest.RestRequest.Method.POST;

public class RestMultiGetAction extends BaseRestHandler {
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RestMultiGetAction.class);
    public static final String TYPES_DEPRECATION_MESSAGE = "[types removal]" +
        " Specifying types in multi get requests is deprecated.";

    private final boolean allowExplicitIndex;

    public RestMultiGetAction(Settings settings) {
        this.allowExplicitIndex = MULTI_ALLOW_EXPLICIT_INDEX.get(settings);
    }

    @Override
    public List<Route> routes() {
        return unmodifiableList(asList(
            new Route(GET, "/_mget"),
            new Route(POST, "/_mget"),
            new Route(GET, "/{index}/_mget"),
            new Route(POST, "/{index}/_mget"),
            // Deprecated typed endpoints.
            new Route(GET, "/{index}/{type}/_mget"),
            new Route(POST, "/{index}/{type}/_mget")));
    }

    @Override
    public String getName() {
        return "document_mget_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        if (request.param("type") != null) {
            deprecationLogger.deprecate("mget_with_types", TYPES_DEPRECATION_MESSAGE);
        }

        MultiGetRequest multiGetRequest = new MultiGetRequest();
        multiGetRequest.refresh(request.paramAsBoolean("refresh", multiGetRequest.refresh()));
        multiGetRequest.preference(request.param("preference"));
        multiGetRequest.realtime(request.paramAsBoolean("realtime", multiGetRequest.realtime()));
        if (request.param("fields") != null) {
            throw new IllegalArgumentException("The parameter [fields] is no longer supported, " +
                "please use [stored_fields] to retrieve stored fields or _source filtering if the field is not stored");
        }
        String[] sFields = null;
        String sField = request.param("stored_fields");
        if (sField != null) {
            sFields = Strings.splitStringByCommaToArray(sField);
        }

        FetchSourceContext defaultFetchSource = FetchSourceContext.parseFromRestRequest(request);
        try (XContentParser parser = request.contentOrSourceParamParser()) {
            multiGetRequest.add(request.param("index"), request.param("type"), sFields, defaultFetchSource,
                request.param("routing"), parser, allowExplicitIndex);
        }

        for (MultiGetRequest.Item item : multiGetRequest.getItems()) {
            if (item.type() != null) {
                deprecationLogger.deprecate("multi_get_types_removal", TYPES_DEPRECATION_MESSAGE);
                break;
            }
        }

        return channel -> client.multiGet(multiGetRequest, new RestToXContentListener<>(channel));
    }
}

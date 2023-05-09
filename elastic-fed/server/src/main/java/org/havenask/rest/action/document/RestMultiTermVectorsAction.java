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

import org.havenask.action.termvectors.MultiTermVectorsRequest;
import org.havenask.action.termvectors.TermVectorsRequest;
import org.havenask.client.node.NodeClient;
import org.havenask.common.Strings;
import org.havenask.common.logging.DeprecationLogger;
import org.havenask.index.mapper.MapperService;
import org.havenask.rest.BaseRestHandler;
import org.havenask.rest.RestRequest;
import org.havenask.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.havenask.rest.RestRequest.Method.GET;
import static org.havenask.rest.RestRequest.Method.POST;

public class RestMultiTermVectorsAction extends BaseRestHandler {
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RestTermVectorsAction.class);
    static final String TYPES_DEPRECATION_MESSAGE = "[types removal] " +
        "Specifying types in multi term vector requests is deprecated.";

    @Override
    public List<Route> routes() {
        return unmodifiableList(asList(
            new Route(GET, "/_mtermvectors"),
            new Route(POST, "/_mtermvectors"),
            new Route(GET, "/{index}/_mtermvectors"),
            new Route(POST, "/{index}/_mtermvectors"),
            // Deprecated typed endpoints.
            new Route(GET, "/{index}/{type}/_mtermvectors"),
            new Route(POST, "/{index}/{type}/_mtermvectors")));
    }

    @Override
    public String getName() {
        return "document_multi_term_vectors_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        MultiTermVectorsRequest multiTermVectorsRequest = new MultiTermVectorsRequest();
        TermVectorsRequest template = new TermVectorsRequest()
            .index(request.param("index"));

        if (request.hasParam("type")) {
            deprecationLogger.deprecate("mtermvectors_with_types", TYPES_DEPRECATION_MESSAGE);
            template.type(request.param("type"));
        } else {
            template.type(MapperService.SINGLE_MAPPING_NAME);
        }

        RestTermVectorsAction.readURIParameters(template, request);
        multiTermVectorsRequest.ids(Strings.commaDelimitedListToStringArray(request.param("ids")));
        request.withContentOrSourceParamParserOrNull(p -> multiTermVectorsRequest.add(template, p));

        return channel -> client.multiTermVectors(multiTermVectorsRequest, new RestToXContentListener<>(channel));
    }

}

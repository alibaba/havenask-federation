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

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.havenask.HavenaskTimeoutException;
import org.havenask.action.ActionRunnable;
import org.havenask.action.admin.indices.mapping.get.GetMappingsRequest;
import org.havenask.action.admin.indices.mapping.get.GetMappingsResponse;
import org.havenask.action.support.IndicesOptions;
import org.havenask.client.node.NodeClient;
import org.havenask.cluster.metadata.MappingMetadata;
import org.havenask.common.Strings;
import org.havenask.common.collect.ImmutableOpenMap;
import org.havenask.common.logging.DeprecationLogger;
import org.havenask.common.regex.Regex;
import org.havenask.common.unit.TimeValue;
import org.havenask.common.util.set.Sets;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.indices.TypeMissingException;
import org.havenask.rest.BaseRestHandler;
import org.havenask.rest.BytesRestResponse;
import org.havenask.rest.RestRequest;
import org.havenask.rest.RestResponse;
import org.havenask.rest.RestStatus;
import org.havenask.rest.action.RestActionListener;
import org.havenask.rest.action.RestBuilderListener;
import org.havenask.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.SortedSet;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.havenask.rest.RestRequest.Method.GET;
import static org.havenask.rest.RestRequest.Method.HEAD;

public class RestGetMappingAction extends BaseRestHandler {
    private static final Logger logger = LogManager.getLogger(RestGetMappingAction.class);
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(logger.getName());
    public static final String TYPES_DEPRECATION_MESSAGE = "[types removal] Using include_type_name in get" +
        " mapping requests is deprecated. The parameter will be removed in the next major version.";

    private final ThreadPool threadPool;

    public RestGetMappingAction(ThreadPool threadPool) {
        this.threadPool = threadPool;
    }

    @Override
    public List<Route> routes() {
        return unmodifiableList(asList(
            new Route(GET, "/_mapping"),
            new Route(GET, "/_mappings"),
            new Route(GET, "/{index}/{type}/_mapping"),
            new Route(GET, "/{index}/_mapping"),
            new Route(GET, "/{index}/_mappings"),
            new Route(GET, "/{index}/_mappings/{type}"),
            new Route(GET, "/{index}/_mapping/{type}"),
            new Route(HEAD, "/{index}/_mapping/{type}"),
            new Route(GET, "/_mapping/{type}")));
    }

    @Override
    public String getName() {
        return "get_mapping_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        final String[] types = request.paramAsStringArrayOrEmptyIfAll("type");
        boolean includeTypeName = request.paramAsBoolean(INCLUDE_TYPE_NAME_PARAMETER, DEFAULT_INCLUDE_TYPE_NAME_POLICY);

        if (request.method().equals(HEAD)) {
            deprecationLogger.deprecate("get_mapping_types_removal",
                    "Type exists requests are deprecated, as types have been deprecated.");
        } else if (includeTypeName == false && types.length > 0) {
            throw new IllegalArgumentException("Types cannot be provided in get mapping requests, unless" +
                    " include_type_name is set to true.");
        }
        if (request.hasParam(INCLUDE_TYPE_NAME_PARAMETER)) {
            deprecationLogger.deprecate("get_mapping_with_types", TYPES_DEPRECATION_MESSAGE);
        }

        final GetMappingsRequest getMappingsRequest = new GetMappingsRequest();
        getMappingsRequest.indices(indices).types(types);
        getMappingsRequest.indicesOptions(IndicesOptions.fromRequest(request, getMappingsRequest.indicesOptions()));
        final TimeValue timeout = request.paramAsTime("master_timeout", getMappingsRequest.masterNodeTimeout());
        getMappingsRequest.masterNodeTimeout(timeout);
        getMappingsRequest.local(request.paramAsBoolean("local", getMappingsRequest.local()));
        return channel -> client.admin().indices().getMappings(getMappingsRequest, new RestActionListener<GetMappingsResponse>(channel) {

            @Override
            protected void processResponse(GetMappingsResponse getMappingsResponse) {
                final long startTimeMs = threadPool.relativeTimeInMillis();
                // Process serialization on GENERIC pool since the serialization of the raw mappings to XContent can be too slow to execute
                // on an IO thread
                threadPool.executor(ThreadPool.Names.MANAGEMENT).execute(
                        ActionRunnable.wrap(this, l -> new RestBuilderListener<GetMappingsResponse>(channel) {
                            @Override
                            public RestResponse buildResponse(final GetMappingsResponse response,
                                                              final XContentBuilder builder) throws Exception {
                                if (threadPool.relativeTimeInMillis() - startTimeMs > timeout.millis()) {
                                    throw new HavenaskTimeoutException("Timed out getting mappings");
                                }
                                final ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetadata>> mappingsByIndex =
                                        response.getMappings();
                                if (mappingsByIndex.isEmpty() && types.length != 0) {
                                    builder.close();
                                    return new BytesRestResponse(channel, new TypeMissingException("_all", String.join(",", types)));
                                }

                                final Set<String> typeNames = new HashSet<>();
                                for (final ObjectCursor<ImmutableOpenMap<String, MappingMetadata>> cursor : mappingsByIndex.values()) {
                                    for (final ObjectCursor<String> inner : cursor.value.keys()) {
                                        typeNames.add(inner.value);
                                    }
                                }

                                final SortedSet<String> difference =
                                        Sets.sortedDifference(Arrays.stream(types).collect(Collectors.toSet()), typeNames);

                                // now remove requested aliases that contain wildcards that are simple matches
                                final List<String> matches = new ArrayList<>();
                                outer:
                                for (final String pattern : difference) {
                                    if (pattern.contains("*")) {
                                        for (final String typeName : typeNames) {
                                            if (Regex.simpleMatch(pattern, typeName)) {
                                                matches.add(pattern);
                                                continue outer;
                                            }
                                        }
                                    }
                                }
                                difference.removeAll(matches);

                                final RestStatus status;
                                builder.startObject();
                                {
                                    if (difference.isEmpty()) {
                                        status = RestStatus.OK;
                                    } else {
                                        status = RestStatus.NOT_FOUND;
                                        final String message = String.format(Locale.ROOT, "type" + (difference.size() == 1 ? "" : "s") +
                                                " [%s] missing", Strings.collectionToCommaDelimitedString(difference));
                                        builder.field("error", message);
                                        builder.field("status", status.getStatus());
                                    }
                                    response.toXContent(builder, request);
                                }
                                builder.endObject();

                                return new BytesRestResponse(status, builder);
                            }
                        }.onResponse(getMappingsResponse)));
            }
        });
    }
}

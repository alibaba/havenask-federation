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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.havenask.http;

import org.havenask.action.index.IndexRequest;
import org.havenask.action.support.WriteRequest;
import org.havenask.client.Request;
import org.havenask.client.RequestOptions;
import org.havenask.client.Response;
import org.havenask.client.node.NodeClient;
import org.havenask.cluster.metadata.IndexNameExpressionResolver;
import org.havenask.cluster.node.DiscoveryNodes;
import org.havenask.common.settings.ClusterSettings;
import org.havenask.common.settings.IndexScopedSettings;
import org.havenask.common.settings.Settings;
import org.havenask.common.settings.SettingsFilter;
import org.havenask.indices.SystemIndexDescriptor;
import org.havenask.plugins.Plugin;
import org.havenask.plugins.SystemIndexPlugin;
import org.havenask.rest.BaseRestHandler;
import org.havenask.rest.RestController;
import org.havenask.rest.RestHandler;
import org.havenask.rest.RestRequest;
import org.havenask.rest.action.RestStatusToXContentListener;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.havenask.test.rest.HavenaskRestTestCase.entityAsMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;

public class SystemIndexRestIT extends HttpSmokeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(SystemIndexTestPlugin.class);
        return plugins;
    }

    public void testSystemIndexAccessBlockedByDefault() throws Exception {
        // create index
        {
            Request putDocRequest = new Request("POST", "/_sys_index_test/add_doc/42");
            Response resp = getRestClient().performRequest(putDocRequest);
            assertThat(resp.getStatusLine().getStatusCode(), equalTo(201));
        }


        // make sure the system index now exists
        assertBusy(() -> {
            Request searchRequest = new Request("GET", "/" + SystemIndexTestPlugin.SYSTEM_INDEX_NAME + "/_count");
            searchRequest.setOptions(expectWarnings("this request accesses system indices: [" + SystemIndexTestPlugin.SYSTEM_INDEX_NAME +
                "], but in a future major version, direct access to system indices will be prevented by default"));

            // Disallow no indices to cause an exception if the flag above doesn't work
            searchRequest.addParameter("allow_no_indices", "false");
            searchRequest.setJsonEntity("{\"query\": {\"match\":  {\"some_field\":  \"some_value\"}}}");

            final Response searchResponse = getRestClient().performRequest(searchRequest);
            assertThat(searchResponse.getStatusLine().getStatusCode(), is(200));
            Map<String, Object> responseMap = entityAsMap(searchResponse);
            assertThat(responseMap, hasKey("count"));
            assertThat(responseMap.get("count"), equalTo(1));
        });

        // And with a partial wildcard
        assertDeprecationWarningOnAccess(".test-*", SystemIndexTestPlugin.SYSTEM_INDEX_NAME);

        // And with a total wildcard
        assertDeprecationWarningOnAccess(randomFrom("*", "_all"), SystemIndexTestPlugin.SYSTEM_INDEX_NAME);

        // Try to index a doc directly
        {
            String expectedWarning = "this request accesses system indices: [" + SystemIndexTestPlugin.SYSTEM_INDEX_NAME + "], but in a " +
                "future major version, direct access to system indices will be prevented by default";
            Request putDocDirectlyRequest = new Request("PUT", "/" + SystemIndexTestPlugin.SYSTEM_INDEX_NAME + "/_doc/43");
            putDocDirectlyRequest.setJsonEntity("{\"some_field\":  \"some_other_value\"}");
            putDocDirectlyRequest.setOptions(expectWarnings(expectedWarning));
            Response response = getRestClient().performRequest(putDocDirectlyRequest);
            assertThat(response.getStatusLine().getStatusCode(), equalTo(201));
        }
    }

    private void assertDeprecationWarningOnAccess(String queryPattern, String warningIndexName) throws IOException {
        String expectedWarning = "this request accesses system indices: [" + warningIndexName + "], but in a " +
            "future major version, direct access to system indices will be prevented by default";
        Request searchRequest = new Request("GET", "/" + queryPattern + randomFrom("/_count", "/_search"));
        searchRequest.setJsonEntity("{\"query\": {\"match\":  {\"some_field\":  \"some_value\"}}}");
        // Disallow no indices to cause an exception if this resolves to zero indices, so that we're sure it resolved the index
        searchRequest.addParameter("allow_no_indices", "false");
        searchRequest.setOptions(expectWarnings(expectedWarning));

        Response response = getRestClient().performRequest(searchRequest);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
    }

    private RequestOptions expectWarnings(String expectedWarning) {
        final RequestOptions.Builder builder = RequestOptions.DEFAULT.toBuilder();
        builder.setWarningsHandler(w -> w.contains(expectedWarning) == false || w.size() != 1);
        return builder.build();
    }


    public static class SystemIndexTestPlugin extends Plugin implements SystemIndexPlugin {

        public static final String SYSTEM_INDEX_NAME = ".test-system-idx";

        @Override
        public List<RestHandler> getRestHandlers(Settings settings, RestController restController, ClusterSettings clusterSettings,
                                                 IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter,
                                                 IndexNameExpressionResolver indexNameExpressionResolver,
                                                 Supplier<DiscoveryNodes> nodesInCluster) {
            return org.havenask.common.collect.List.of(new AddDocRestHandler());
        }

        @Override
        public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
            return Collections.singletonList(new SystemIndexDescriptor(SYSTEM_INDEX_NAME, "System indices for tests"));
        }

        public static class AddDocRestHandler extends BaseRestHandler {
            @Override
            public boolean allowSystemIndexAccessByDefault() {
                return true;
            }

            @Override
            public String getName() {
                return "system_index_test_doc_adder";
            }

            @Override
            public List<Route> routes() {
                return org.havenask.common.collect.List.of(new Route(RestRequest.Method.POST, "/_sys_index_test/add_doc/{id}"));
            }

            @Override
            protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
                IndexRequest indexRequest = new IndexRequest(SYSTEM_INDEX_NAME);
                indexRequest.id(request.param("id"));
                indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
                indexRequest.source(org.havenask.common.collect.Map.of("some_field", "some_value"));
                return channel -> client.index(indexRequest,
                    new RestStatusToXContentListener<>(channel, r -> r.getLocation(indexRequest.routing())));
            }
        }
    }
}

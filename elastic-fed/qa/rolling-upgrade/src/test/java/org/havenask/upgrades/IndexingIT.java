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

package org.havenask.upgrades;

import org.apache.http.util.EntityUtils;
import org.havenask.LegacyESVersion;
import org.havenask.Version;
import org.havenask.client.Request;
import org.havenask.client.Response;
import org.havenask.client.ResponseException;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.common.Booleans;
import org.havenask.common.Strings;
import org.havenask.common.settings.Settings;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.rest.action.document.RestBulkAction;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.havenask.common.xcontent.XContentFactory.jsonBuilder;
import static org.havenask.rest.action.search.RestSearchAction.TOTAL_HITS_AS_INT_PARAM;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;

/**
 * Basic test that indexed documents survive the rolling restart. See
 * {@link RecoveryIT} for much more in depth testing of the mechanism
 * by which they survive.
 * <p>
 * This test is an almost exact copy of <code>IndexingIT</code> in the
 * xpack rolling restart tests. We should work on a way to remove this
 * duplication but for now we have no real way to share code.
 */
public class IndexingIT extends AbstractRollingTestCase {

    public void testIndexing() throws IOException {
        switch (CLUSTER_TYPE) {
        case OLD:
            break;
        case MIXED:
            Request waitForYellow = new Request("GET", "/_cluster/health");
            waitForYellow.addParameter("wait_for_nodes", "3");
            waitForYellow.addParameter("wait_for_status", "yellow");
            client().performRequest(waitForYellow);
            break;
        case UPGRADED:
            Request waitForGreen = new Request("GET", "/_cluster/health/test_index,index_with_replicas,empty_index");
            waitForGreen.addParameter("wait_for_nodes", "3");
            waitForGreen.addParameter("wait_for_status", "green");
            // wait for long enough that we give delayed unassigned shards to stop being delayed
            waitForGreen.addParameter("timeout", "70s");
            waitForGreen.addParameter("level", "shards");
            client().performRequest(waitForGreen);
            break;
        default:
            throw new UnsupportedOperationException("Unknown cluster type [" + CLUSTER_TYPE + "]");
        }

        if (CLUSTER_TYPE == ClusterType.OLD) {
            {
                Version minimumIndexCompatibilityVersion = Version.CURRENT.minimumIndexCompatibilityVersion();
                assertThat("this branch is not needed if we aren't compatible with 6.0",
                    minimumIndexCompatibilityVersion.onOrBefore(LegacyESVersion.V_6_0_0), equalTo(true));
                if (minimumIndexCompatibilityVersion.before(LegacyESVersion.V_7_0_0)) {
                    XContentBuilder template = jsonBuilder();
                    template.startObject();
                    {
                        template.array("index_patterns", "test_index", "index_with_replicas", "empty_index");
                        template.startObject("settings");
                        template.field("number_of_shards", 5);
                        template.endObject();
                    }
                    template.endObject();
                    Request createTemplate = new Request("PUT", "/_template/prevent-bwc-deprecation-template");
                    createTemplate.setJsonEntity(Strings.toString(template));
                    client().performRequest(createTemplate);
                }
            }
            Request createTestIndex = new Request("PUT", "/test_index");
            createTestIndex.setJsonEntity("{\"settings\": {\"index.number_of_replicas\": 0}}");
            useIgnoreMultipleMatchingTemplatesWarningsHandler(createTestIndex);
            client().performRequest(createTestIndex);
            allowedWarnings("index [test_index] matches multiple legacy templates [global, prevent-bwc-deprecation-template], " +
                "composable templates will only match a single template");

            String recoverQuickly = "{\"settings\": {\"index.unassigned.node_left.delayed_timeout\": \"100ms\"}}";
            Request createIndexWithReplicas = new Request("PUT", "/index_with_replicas");
            createIndexWithReplicas.setJsonEntity(recoverQuickly);
            useIgnoreMultipleMatchingTemplatesWarningsHandler(createIndexWithReplicas);
            client().performRequest(createIndexWithReplicas);

            Request createEmptyIndex = new Request("PUT", "/empty_index");
            // Ask for recovery to be quick
            createEmptyIndex.setJsonEntity(recoverQuickly);
            useIgnoreMultipleMatchingTemplatesWarningsHandler(createEmptyIndex);
            client().performRequest(createEmptyIndex);

            bulk("test_index", "_OLD", 5);
            bulk("index_with_replicas", "_OLD", 5);
        }

        int expectedCount;
        switch (CLUSTER_TYPE) {
        case OLD:
            expectedCount = 5;
            break;
        case MIXED:
            if (Booleans.parseBoolean(System.getProperty("tests.first_round"))) {
                expectedCount = 5;
            } else {
                expectedCount = 10;
            }
            break;
        case UPGRADED:
            expectedCount = 15;
            break;
        default:
            throw new UnsupportedOperationException("Unknown cluster type [" + CLUSTER_TYPE + "]");
        }

        assertCount("test_index", expectedCount);
        assertCount("index_with_replicas", 5);
        assertCount("empty_index", 0);

        if (CLUSTER_TYPE != ClusterType.OLD) {
            bulk("test_index", "_" + CLUSTER_TYPE, 5);
            Request toBeDeleted = new Request("PUT", "/test_index/_doc/to_be_deleted");
            toBeDeleted.addParameter("refresh", "true");
            toBeDeleted.setJsonEntity("{\"f1\": \"delete-me\"}");
            client().performRequest(toBeDeleted);
            assertCount("test_index", expectedCount + 6);

            Request delete = new Request("DELETE", "/test_index/_doc/to_be_deleted");
            delete.addParameter("refresh", "true");
            client().performRequest(delete);

            assertCount("test_index", expectedCount + 5);
        }
    }

    public void testAutoIdWithOpTypeCreate() throws IOException {
        final String indexName = "auto_id_and_op_type_create_index";
        StringBuilder b = new StringBuilder();
        b.append("{\"create\": {\"_index\": \"").append(indexName).append("\"}}\n");
        b.append("{\"f1\": \"v\"}\n");
        Request bulk = new Request("POST", "/_bulk");
        bulk.addParameter("refresh", "true");
        bulk.setJsonEntity(b.toString());

        switch (CLUSTER_TYPE) {
            case OLD:
                Settings.Builder settings = Settings.builder()
                    .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                    .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0);
                createIndex(indexName, settings.build());
                break;
            case MIXED:
                Request waitForGreen = new Request("GET", "/_cluster/health");
                waitForGreen.addParameter("wait_for_nodes", "3");
                client().performRequest(waitForGreen);

                Version minNodeVersion = null;
                Map<?, ?> response = entityAsMap(client().performRequest(new Request("GET", "_nodes")));
                Map<?, ?> nodes = (Map<?, ?>) response.get("nodes");
                for (Map.Entry<?, ?> node : nodes.entrySet()) {
                    Map<?, ?> nodeInfo = (Map<?, ?>) node.getValue();
                    Version nodeVersion = Version.fromString(nodeInfo.get("version").toString());
                    if (minNodeVersion == null) {
                        minNodeVersion = nodeVersion;
                    } else if (nodeVersion.before(minNodeVersion)) {
                        minNodeVersion = nodeVersion;
                    }
                }

                if (minNodeVersion.before(LegacyESVersion.V_7_5_0)) {
                    ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(bulk));
                    assertEquals(400, e.getResponse().getStatusLine().getStatusCode());
                    assertThat(e.getMessage(),
                        // if request goes to 7.5+ node
                        either(containsString("optype create not supported for indexing requests without explicit id until"))
                            // if request goes to < 7.5 node
                            .or(containsString("an id must be provided if version type or value are set")
                            ));
                } else {
                    client().performRequest(bulk);
                }
                break;
            case UPGRADED:
                client().performRequest(bulk);
                break;
            default:
                throw new UnsupportedOperationException("Unknown cluster type [" + CLUSTER_TYPE + "]");
        }
    }

    private void bulk(String index, String valueSuffix, int count) throws IOException {
        StringBuilder b = new StringBuilder();
        for (int i = 0; i < count; i++) {
            b.append("{\"index\": {\"_index\": \"").append(index).append("\", \"_type\": \"_doc\"}}\n");
            b.append("{\"f1\": \"v").append(i).append(valueSuffix).append("\", \"f2\": ").append(i).append("}\n");
        }
        Request bulk = new Request("POST", "/_bulk");
        bulk.addParameter("refresh", "true");
        bulk.setOptions(expectWarnings(RestBulkAction.TYPES_DEPRECATION_MESSAGE));
        bulk.setJsonEntity(b.toString());
        client().performRequest(bulk);
    }

    private void assertCount(String index, int count) throws IOException {
        Request searchTestIndexRequest = new Request("POST", "/" + index + "/_search");
        searchTestIndexRequest.addParameter(TOTAL_HITS_AS_INT_PARAM, "true");
        searchTestIndexRequest.addParameter("filter_path", "hits.total");
        Response searchTestIndexResponse = client().performRequest(searchTestIndexRequest);
        assertEquals("{\"hits\":{\"total\":" + count + "}}",
                EntityUtils.toString(searchTestIndexResponse.getEntity(), StandardCharsets.UTF_8));
    }
}

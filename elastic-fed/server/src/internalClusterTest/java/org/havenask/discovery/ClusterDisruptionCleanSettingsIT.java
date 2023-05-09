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

package org.havenask.discovery;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.havenask.action.index.IndexRequestBuilder;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.common.settings.Settings;
import org.havenask.common.xcontent.XContentType;
import org.havenask.indices.store.IndicesStoreIntegrationIT;
import org.havenask.plugins.Plugin;
import org.havenask.test.HavenaskIntegTestCase;
import org.havenask.test.transport.MockTransportService;

import static org.havenask.test.hamcrest.HavenaskAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

@HavenaskIntegTestCase.ClusterScope(scope = HavenaskIntegTestCase.Scope.TEST, numDataNodes = 0, transportClientRatio = 0)
public class ClusterDisruptionCleanSettingsIT extends HavenaskIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockTransportService.TestPlugin.class);
    }

    /**
     * This test creates a scenario where a primary shard (0 replicas) relocates and is in POST_RECOVERY on the target
     * node but already deleted on the source node. Search request should still work.
     */
    public void testSearchWithRelocationAndSlowClusterStateProcessing() throws Exception {
        // Don't use AbstractDisruptionTestCase.DEFAULT_SETTINGS as settings
        // (which can cause node disconnects on a slow CI machine)
        internalCluster().startMasterOnlyNode();
        final String node_1 = internalCluster().startDataOnlyNode();

        logger.info("--> creating index [test] with one shard and on replica");
        assertAcked(prepareCreate("test").setSettings(
            Settings.builder().put(indexSettings())
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0))
        );
        ensureGreen("test");

        final String node_2 = internalCluster().startDataOnlyNode();
        List<IndexRequestBuilder> indexRequestBuilderList = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            indexRequestBuilderList.add(client().prepareIndex().setIndex("test").setType("_doc")
                .setSource("{\"int_field\":1}", XContentType.JSON));
        }
        indexRandom(true, indexRequestBuilderList);

        IndicesStoreIntegrationIT.relocateAndBlockCompletion(logger, "test", 0, node_1, node_2);
        // now search for the documents and see if we get a reply
        assertThat(client().prepareSearch().setSize(0).get().getHits().getTotalHits().value, equalTo(100L));
    }
}

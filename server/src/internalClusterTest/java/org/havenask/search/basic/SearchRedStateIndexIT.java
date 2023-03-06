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

package org.havenask.search.basic;


import org.havenask.action.admin.cluster.settings.ClusterUpdateSettingsResponse;

import org.havenask.action.search.SearchPhaseExecutionException;
import org.havenask.action.search.SearchResponse;
import org.havenask.cluster.ClusterState;
import org.havenask.cluster.health.ClusterHealthStatus;
import org.havenask.cluster.routing.ShardRouting;
import org.havenask.cluster.routing.ShardRoutingState;
import org.havenask.common.settings.Settings;
import org.havenask.rest.RestStatus;
import org.havenask.search.SearchService;
import org.havenask.test.HavenaskIntegTestCase;

import org.junit.After;

import java.util.List;

import static org.havenask.test.hamcrest.HavenaskAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

@HavenaskIntegTestCase.ClusterScope(minNumDataNodes = 2)
public class SearchRedStateIndexIT extends HavenaskIntegTestCase {


    public void testAllowPartialsWithRedState() throws Exception {
        final int numShards = cluster().numDataNodes()+2;
        buildRedIndex(numShards);

        SearchResponse searchResponse = client().prepareSearch().setSize(0).setAllowPartialSearchResults(true)
                .get();
        assertThat(RestStatus.OK, equalTo(searchResponse.status()));
        assertThat("Expect no shards failed", searchResponse.getFailedShards(), equalTo(0));
        assertThat("Expect no shards skipped", searchResponse.getSkippedShards(), equalTo(0));
        assertThat("Expect subset of shards successful", searchResponse.getSuccessfulShards(), lessThan(numShards));
        assertThat("Expected total shards", searchResponse.getTotalShards(), equalTo(numShards));
    }

    public void testClusterAllowPartialsWithRedState() throws Exception {
        final int numShards = cluster().numDataNodes()+2;
        buildRedIndex(numShards);

        setClusterDefaultAllowPartialResults(true);

        SearchResponse searchResponse = client().prepareSearch().setSize(0).get();
        assertThat(RestStatus.OK, equalTo(searchResponse.status()));
        assertThat("Expect no shards failed", searchResponse.getFailedShards(), equalTo(0));
        assertThat("Expect no shards skipped", searchResponse.getSkippedShards(), equalTo(0));
        assertThat("Expect subset of shards successful", searchResponse.getSuccessfulShards(), lessThan(numShards));
        assertThat("Expected total shards", searchResponse.getTotalShards(), equalTo(numShards));
    }


    public void testDisallowPartialsWithRedState() throws Exception {
        buildRedIndex(cluster().numDataNodes()+2);

        SearchPhaseExecutionException ex = expectThrows(SearchPhaseExecutionException.class,
                () ->
            client().prepareSearch().setSize(0).setAllowPartialSearchResults(false).get()
        );
        assertThat(ex.getDetailedMessage(), containsString("Search rejected due to missing shard"));
    }


    public void testClusterDisallowPartialsWithRedState() throws Exception {
        buildRedIndex(cluster().numDataNodes()+2);

        setClusterDefaultAllowPartialResults(false);
        SearchPhaseExecutionException ex = expectThrows(SearchPhaseExecutionException.class,
                () ->
            client().prepareSearch().setSize(0).get()
        );
        assertThat(ex.getDetailedMessage(), containsString("Search rejected due to missing shard"));
    }

    private void setClusterDefaultAllowPartialResults(boolean allowPartialResults) {
        String key = SearchService.DEFAULT_ALLOW_PARTIAL_SEARCH_RESULTS.getKey();

        Settings transientSettings = Settings.builder().put(key, allowPartialResults).build();

        ClusterUpdateSettingsResponse response1 = client().admin().cluster()
                .prepareUpdateSettings()
                .setTransientSettings(transientSettings)
                .get();

        assertAcked(response1);
        assertEquals(response1.getTransientSettings().getAsBoolean(key, null), allowPartialResults);
    }

    private void buildRedIndex(int numShards) throws Exception {
        assertAcked(prepareCreate("test").setSettings(Settings.builder().put("index.number_of_shards",
                numShards).put("index.number_of_replicas", 0)));
        ensureGreen();
        for (int i = 0; i < 10; i++) {
            client().prepareIndex("test", "type1", ""+i).setSource("field1", "value1").get();
        }
        refresh();

        internalCluster().stopRandomDataNode();

        client().admin().cluster().prepareHealth().setWaitForStatus(ClusterHealthStatus.RED).get();

        assertBusy(() -> {
            ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
            List<ShardRouting> unassigneds = clusterState.getRoutingTable().shardsWithState(ShardRoutingState.UNASSIGNED);
            assertThat(unassigneds.size(), greaterThan(0));
        });

    }

    @After
    public void cleanup() throws Exception {
        assertAcked(client().admin().cluster().prepareUpdateSettings()
            .setTransientSettings(Settings.builder().putNull(SearchService.DEFAULT_ALLOW_PARTIAL_SEARCH_RESULTS.getKey())));
    }
}

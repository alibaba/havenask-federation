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

package org.havenask.rest.action.cat;

import org.havenask.Version;
import org.havenask.action.admin.indices.stats.CommonStats;
import org.havenask.action.admin.indices.stats.IndexStats;
import org.havenask.cluster.health.ClusterHealthStatus;
import org.havenask.cluster.health.ClusterIndexHealth;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.cluster.routing.IndexRoutingTable;
import org.havenask.cluster.routing.ShardRoutingState;
import org.havenask.cluster.routing.TestShardRouting;
import org.havenask.common.Table;
import org.havenask.common.UUIDs;
import org.havenask.common.settings.Settings;
import org.havenask.index.Index;
import org.havenask.index.IndexSettings;
import org.havenask.index.shard.ShardId;
import org.havenask.test.HavenaskTestCase;
import org.havenask.test.rest.FakeRestRequest;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RestIndicesActionTests extends HavenaskTestCase {

    public void testBuildTable() {
        final int numIndices = randomIntBetween(3, 20);
        final Map<String, Settings> indicesSettings = new LinkedHashMap<>();
        final Map<String, IndexMetadata> indicesMetadatas = new LinkedHashMap<>();
        final Map<String, ClusterIndexHealth> indicesHealths = new LinkedHashMap<>();
        final Map<String, IndexStats> indicesStats = new LinkedHashMap<>();

        for (int i = 0; i < numIndices; i++) {
            String indexName = "index-" + i;

            Settings indexSettings = Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
                .put(IndexSettings.INDEX_SEARCH_THROTTLED.getKey(), randomBoolean())
                .build();
            indicesSettings.put(indexName, indexSettings);

            IndexMetadata.State indexState = randomBoolean() ? IndexMetadata.State.OPEN : IndexMetadata.State.CLOSE;
            if (frequently()) {
                ClusterHealthStatus healthStatus = randomFrom(ClusterHealthStatus.values());
                int numberOfShards = randomIntBetween(1, 3);
                int numberOfReplicas = healthStatus == ClusterHealthStatus.YELLOW ? 1 : randomInt(1);
                IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
                    .settings(indexSettings)
                    .creationDate(System.currentTimeMillis())
                    .numberOfShards(numberOfShards)
                    .numberOfReplicas(numberOfReplicas)
                    .state(indexState)
                    .build();
                indicesMetadatas.put(indexName, indexMetadata);

                if (frequently()) {
                    Index index = indexMetadata.getIndex();
                    IndexRoutingTable.Builder indexRoutingTable = IndexRoutingTable.builder(index);
                    switch (randomFrom(ClusterHealthStatus.values())) {
                        case GREEN:
                            IntStream.range(0, numberOfShards)
                                .mapToObj(n -> new ShardId(index, n))
                                .map(shardId -> TestShardRouting.newShardRouting(shardId, "nodeA", true, ShardRoutingState.STARTED))
                                .forEach(indexRoutingTable::addShard);
                            if (numberOfReplicas > 0) {
                                IntStream.range(0, numberOfShards)
                                    .mapToObj(n -> new ShardId(index, n))
                                    .map(shardId -> TestShardRouting.newShardRouting(shardId, "nodeB", false, ShardRoutingState.STARTED))
                                    .forEach(indexRoutingTable::addShard);
                            }
                            break;
                        case YELLOW:
                            IntStream.range(0, numberOfShards)
                                .mapToObj(n -> new ShardId(index, n))
                                .map(shardId -> TestShardRouting.newShardRouting(shardId, "nodeA", true, ShardRoutingState.STARTED))
                                .forEach(indexRoutingTable::addShard);
                            if (numberOfReplicas > 0) {
                                IntStream.range(0, numberOfShards)
                                    .mapToObj(n -> new ShardId(index, n))
                                    .map(shardId -> TestShardRouting.newShardRouting(shardId, null, false, ShardRoutingState.UNASSIGNED))
                                    .forEach(indexRoutingTable::addShard);
                            }
                            break;
                        case RED:
                            break;
                    }
                    indicesHealths.put(indexName, new ClusterIndexHealth(indexMetadata, indexRoutingTable.build()));

                    if (frequently()) {
                        IndexStats indexStats = mock(IndexStats.class);
                        when(indexStats.getPrimaries()).thenReturn(new CommonStats());
                        when(indexStats.getTotal()).thenReturn(new CommonStats());
                        indicesStats.put(indexName, indexStats);
                    }
                }
            }
        }

        final RestIndicesAction action = new RestIndicesAction();
        final Table table = action.buildTable(new FakeRestRequest(), indicesSettings, indicesHealths, indicesStats, indicesMetadatas);

        // now, verify the table is correct
        List<Table.Cell> headers = table.getHeaders();
        assertThat(headers.get(0).value, equalTo("health"));
        assertThat(headers.get(1).value, equalTo("status"));
        assertThat(headers.get(2).value, equalTo("index"));
        assertThat(headers.get(3).value, equalTo("uuid"));
        assertThat(headers.get(4).value, equalTo("pri"));
        assertThat(headers.get(5).value, equalTo("rep"));

        final List<List<Table.Cell>> rows = table.getRows();
        assertThat(rows.size(), equalTo(indicesMetadatas.size()));

        for (final List<Table.Cell> row : rows) {
            final String indexName = (String) row.get(2).value;

            ClusterIndexHealth indexHealth = indicesHealths.get(indexName);
            IndexStats indexStats = indicesStats.get(indexName);
            IndexMetadata indexMetadata = indicesMetadatas.get(indexName);

            if (indexHealth != null) {
                assertThat(row.get(0).value, equalTo(indexHealth.getStatus().toString().toLowerCase(Locale.ROOT)));
            } else if (indexStats != null) {
                assertThat(row.get(0).value, equalTo("red*"));
            } else {
                assertThat(row.get(0).value, equalTo(""));
            }

            assertThat(row.get(1).value, equalTo(indexMetadata.getState().toString().toLowerCase(Locale.ROOT)));
            assertThat(row.get(2).value, equalTo(indexName));
            assertThat(row.get(3).value, equalTo(indexMetadata.getIndexUUID()));
            if (indexHealth != null) {
                assertThat(row.get(4).value, equalTo(indexMetadata.getNumberOfShards()));
                assertThat(row.get(5).value, equalTo(indexMetadata.getNumberOfReplicas()));
            } else {
                assertThat(row.get(4).value, nullValue());
                assertThat(row.get(5).value, nullValue());
            }
        }
    }
}

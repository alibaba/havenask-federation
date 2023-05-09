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

package org.havenask.indices.recovery;

import org.havenask.action.admin.indices.close.CloseIndexResponse;
import org.havenask.cluster.ClusterState;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.cluster.node.DiscoveryNode;
import org.havenask.cluster.routing.IndexShardRoutingTable;
import org.havenask.cluster.routing.ShardRouting;
import org.havenask.test.BackgroundIndexer;
import org.havenask.test.HavenaskIntegTestCase;
import org.havenask.test.InternalTestCluster;

import java.util.Locale;

import static org.havenask.test.hamcrest.HavenaskAssertions.assertAcked;
import static org.havenask.test.hamcrest.HavenaskAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

@HavenaskIntegTestCase.ClusterScope(numDataNodes = 2)
public class ReplicaToPrimaryPromotionIT extends HavenaskIntegTestCase {

    @Override
    protected int numberOfReplicas() {
        return 1;
    }

    public void testPromoteReplicaToPrimary() throws Exception {
        final String indexName = randomAlphaOfLength(5).toLowerCase(Locale.ROOT);
        createIndex(indexName);

        final int numOfDocs = scaledRandomIntBetween(0, 200);
        if (numOfDocs > 0) {
            try (BackgroundIndexer indexer = new BackgroundIndexer(indexName, "_doc", client(), numOfDocs)) {
                waitForDocs(numOfDocs, indexer);
            }
            refresh(indexName);
        }

        assertHitCount(client().prepareSearch(indexName).setSize(0).get(), numOfDocs);
        ensureGreen(indexName);

        // sometimes test with a closed index
        final IndexMetadata.State indexState = randomFrom(IndexMetadata.State.OPEN, IndexMetadata.State.CLOSE);
        if (indexState == IndexMetadata.State.CLOSE) {
            CloseIndexResponse closeIndexResponse = client().admin().indices().prepareClose(indexName).get();
            assertThat("close index not acked - " + closeIndexResponse, closeIndexResponse.isAcknowledged(), equalTo(true));
            ensureGreen(indexName);
        }

        // pick up a data node that contains a random primary shard
        ClusterState state = client(internalCluster().getMasterName()).admin().cluster().prepareState().get().getState();
        final int numShards = state.metadata().index(indexName).getNumberOfShards();
        final ShardRouting primaryShard = state.routingTable().index(indexName).shard(randomIntBetween(0, numShards - 1)).primaryShard();
        final DiscoveryNode randomNode = state.nodes().resolveNode(primaryShard.currentNodeId());

        // stop the random data node, all remaining shards are promoted to primaries
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(randomNode.getName()));
        ensureYellowAndNoInitializingShards(indexName);

        state = client(internalCluster().getMasterName()).admin().cluster().prepareState().get().getState();
        for (IndexShardRoutingTable shardRoutingTable : state.routingTable().index(indexName)) {
            for (ShardRouting shardRouting : shardRoutingTable.activeShards()) {
                assertThat(shardRouting + " should be promoted as a primary", shardRouting.primary(), is(true));
            }
        }

        if (indexState == IndexMetadata.State.CLOSE) {
            assertAcked(client().admin().indices().prepareOpen(indexName));
            ensureYellowAndNoInitializingShards(indexName);
        }
        assertHitCount(client().prepareSearch(indexName).setSize(0).get(), numOfDocs);
    }
}

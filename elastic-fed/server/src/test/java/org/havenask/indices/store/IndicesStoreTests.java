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

package org.havenask.indices.store;

import org.havenask.Version;
import org.havenask.cluster.node.DiscoveryNode;
import org.havenask.cluster.routing.IndexShardRoutingTable;
import org.havenask.cluster.routing.ShardRoutingState;
import org.havenask.cluster.routing.TestShardRouting;
import org.havenask.cluster.routing.UnassignedInfo;
import org.havenask.index.shard.ShardId;
import org.havenask.test.HavenaskTestCase;
import org.junit.Before;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;

public class IndicesStoreTests extends HavenaskTestCase {
    private static final ShardRoutingState[] NOT_STARTED_STATES;

    static {
        Set<ShardRoutingState> set = new HashSet<>();
        set.addAll(Arrays.asList(ShardRoutingState.values()));
        set.remove(ShardRoutingState.STARTED);
        NOT_STARTED_STATES = set.toArray(new ShardRoutingState[set.size()]);
    }

    private DiscoveryNode localNode;

    @Before
    public void createLocalNode() {
        localNode = new DiscoveryNode("abc", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
    }

    public void testShardCanBeDeletedNoShardRouting() throws Exception {
        IndexShardRoutingTable.Builder routingTable = new IndexShardRoutingTable.Builder(new ShardId("test", "_na_", 1));
        assertFalse(IndicesStore.shardCanBeDeleted(localNode.getId(), routingTable.build()));
    }

    public void testShardCanBeDeletedNoShardStarted() throws Exception {
        int numShards = randomIntBetween(1, 7);
        int numReplicas = randomInt(2);

        IndexShardRoutingTable.Builder routingTable = new IndexShardRoutingTable.Builder(new ShardId("test", "_na_", 1));

        for (int i = 0; i < numShards; i++) {
            int unStartedShard = randomInt(numReplicas);
            for (int j = 0; j <= numReplicas; j++) {
                ShardRoutingState state;
                if (j == unStartedShard) {
                    state = randomFrom(NOT_STARTED_STATES);
                } else {
                    state = randomFrom(ShardRoutingState.values());
                }
                UnassignedInfo unassignedInfo = null;
                if (state == ShardRoutingState.UNASSIGNED) {
                    unassignedInfo = new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null);
                }
                String currentNodeId = state == ShardRoutingState.UNASSIGNED ? null : randomAlphaOfLength(10);
                String relocatingNodeId = state == ShardRoutingState.RELOCATING ? randomAlphaOfLength(10) : null;
                routingTable.addShard(TestShardRouting.newShardRouting("test", i, currentNodeId, relocatingNodeId, j == 0, state,
                    unassignedInfo));
            }
        }

        assertFalse(IndicesStore.shardCanBeDeleted(localNode.getId(), routingTable.build()));
    }

    public void testShardCanBeDeletedShardExistsLocally() throws Exception {
        int numShards = randomIntBetween(1, 7);
        int numReplicas = randomInt(2);

        IndexShardRoutingTable.Builder routingTable = new IndexShardRoutingTable.Builder(new ShardId("test", "_na_", 1));
        int localShardId = randomInt(numShards - 1);
        for (int i = 0; i < numShards; i++) {
            int localNodeIndex = randomInt(numReplicas);
            boolean primaryOnLocalNode = i == localShardId && localNodeIndex == numReplicas;
            routingTable.addShard(TestShardRouting.newShardRouting("test", i, primaryOnLocalNode ? localNode.getId() :
                randomAlphaOfLength(10), true, ShardRoutingState.STARTED));
            for (int j = 0; j < numReplicas; j++) {
                boolean replicaOnLocalNode = i == localShardId && localNodeIndex == j;
                routingTable.addShard(TestShardRouting.newShardRouting("test", i, replicaOnLocalNode ? localNode.getId() :
                    randomAlphaOfLength(10), false, ShardRoutingState.STARTED));
            }
        }

        // Shard exists locally, can't delete shard
        assertFalse(IndicesStore.shardCanBeDeleted(localNode.getId(), routingTable.build()));
    }
}

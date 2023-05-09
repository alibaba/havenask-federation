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

package org.havenask.cluster.metadata;

import org.havenask.LegacyESVersion;
import org.havenask.Version;
import org.havenask.action.admin.cluster.reroute.ClusterRerouteRequest;
import org.havenask.action.admin.indices.create.CreateIndexRequest;
import org.havenask.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.havenask.action.support.ActiveShardCount;
import org.havenask.action.support.replication.ClusterStateCreationUtils;
import org.havenask.cluster.ClusterState;
import org.havenask.cluster.node.DiscoveryNode;
import org.havenask.cluster.node.DiscoveryNodeRole;
import org.havenask.cluster.node.DiscoveryNodes;
import org.havenask.cluster.routing.IndexShardRoutingTable;
import org.havenask.cluster.routing.ShardRoutingState;
import org.havenask.common.settings.Settings;
import org.havenask.indices.cluster.ClusterStateChanges;
import org.havenask.test.HavenaskTestCase;
import org.havenask.test.VersionUtils;
import org.havenask.threadpool.TestThreadPool;
import org.havenask.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.havenask.cluster.metadata.IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS;
import static org.havenask.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.is;

public class AutoExpandReplicasTests extends HavenaskTestCase {

    public void testParseSettings() {
        AutoExpandReplicas autoExpandReplicas = AutoExpandReplicas.SETTING
            .get(Settings.builder().put("index.auto_expand_replicas", "0-5").build());
        assertEquals(0, autoExpandReplicas.getMinReplicas());
        assertEquals(5, autoExpandReplicas.getMaxReplicas(8));
        assertEquals(2, autoExpandReplicas.getMaxReplicas(3));

        autoExpandReplicas = AutoExpandReplicas.SETTING.get(Settings.builder().put("index.auto_expand_replicas", "0-all").build());
        assertEquals(0, autoExpandReplicas.getMinReplicas());
        assertEquals(5, autoExpandReplicas.getMaxReplicas(6));
        assertEquals(2, autoExpandReplicas.getMaxReplicas(3));

        autoExpandReplicas = AutoExpandReplicas.SETTING.get(Settings.builder().put("index.auto_expand_replicas", "1-all").build());
        assertEquals(1, autoExpandReplicas.getMinReplicas());
        assertEquals(5, autoExpandReplicas.getMaxReplicas(6));
        assertEquals(2, autoExpandReplicas.getMaxReplicas(3));

    }

    public void testInvalidValues() {
        try {
            AutoExpandReplicas.SETTING.get(Settings.builder().put("index.auto_expand_replicas", "boom").build());
            fail();
        } catch (IllegalArgumentException ex) {
            assertEquals("failed to parse [index.auto_expand_replicas] from value: [boom] at index -1", ex.getMessage());
        }

        try {
            AutoExpandReplicas.SETTING.get(Settings.builder().put("index.auto_expand_replicas", "1-boom").build());
            fail();
        } catch (IllegalArgumentException ex) {
            assertEquals("failed to parse [index.auto_expand_replicas] from value: [1-boom] at index 1", ex.getMessage());
            assertEquals("For input string: \"boom\"", ex.getCause().getMessage());
        }

        try {
            AutoExpandReplicas.SETTING.get(Settings.builder().put("index.auto_expand_replicas", "boom-1").build());
            fail();
        } catch (IllegalArgumentException ex) {
            assertEquals("failed to parse [index.auto_expand_replicas] from value: [boom-1] at index 4", ex.getMessage());
            assertEquals("For input string: \"boom\"", ex.getCause().getMessage());
        }

        try {
            AutoExpandReplicas.SETTING.get(Settings.builder().put("index.auto_expand_replicas", "2-1").build());
        } catch (IllegalArgumentException ex) {
            assertEquals("[index.auto_expand_replicas] minReplicas must be =< maxReplicas but wasn't 2 > 1", ex.getMessage());
        }

    }

    private static final AtomicInteger nodeIdGenerator = new AtomicInteger();

    protected DiscoveryNode createNode(Version version, DiscoveryNodeRole... mustHaveRoles) {
        Set<DiscoveryNodeRole> roles = new HashSet<>(randomSubsetOf(DiscoveryNodeRole.BUILT_IN_ROLES));
        Collections.addAll(roles, mustHaveRoles);
        final String id = String.format(Locale.ROOT, "node_%03d", nodeIdGenerator.incrementAndGet());
        return new DiscoveryNode(id, id, buildNewFakeTransportAddress(), Collections.emptyMap(), roles, version);
    }

    protected DiscoveryNode createNode(DiscoveryNodeRole... mustHaveRoles) {
        return createNode(Version.CURRENT, mustHaveRoles);
    }

    /**
     * Checks that when nodes leave the cluster that the auto-expand-replica functionality only triggers after failing the shards on
     * the removed nodes. This ensures that active shards on other live nodes are not failed if the primary resided on a now dead node.
     * Instead, one of the replicas on the live nodes first gets promoted to primary, and the auto-expansion (removing replicas) only
     * triggers in a follow-up step.
     */
    public void testAutoExpandWhenNodeLeavesAndPossiblyRejoins() throws InterruptedException {
        final ThreadPool threadPool = new TestThreadPool(getClass().getName());
        final ClusterStateChanges cluster = new ClusterStateChanges(xContentRegistry(), threadPool);

        try {
            List<DiscoveryNode> allNodes = new ArrayList<>();
            DiscoveryNode localNode = createNode(DiscoveryNodeRole.MASTER_ROLE); // local node is the master
            allNodes.add(localNode);
            int numDataNodes = randomIntBetween(3, 5);
            List<DiscoveryNode> dataNodes = new ArrayList<>(numDataNodes);
            for (int i = 0; i < numDataNodes; i++) {
                dataNodes.add(createNode(DiscoveryNodeRole.DATA_ROLE));
            }
            allNodes.addAll(dataNodes);
            ClusterState state = ClusterStateCreationUtils.state(localNode, localNode, allNodes.toArray(new DiscoveryNode[0]));

            CreateIndexRequest request = new CreateIndexRequest("index",
                Settings.builder()
                    .put(SETTING_NUMBER_OF_SHARDS, 1)
                    .put(SETTING_AUTO_EXPAND_REPLICAS, "0-all").build())
                .waitForActiveShards(ActiveShardCount.NONE);
            state = cluster.createIndex(state, request);
            assertTrue(state.metadata().hasIndex("index"));
            while (state.routingTable().index("index").shard(0).allShardsStarted() == false) {
                logger.info(state);
                state = cluster.applyStartedShards(state,
                    state.routingTable().index("index").shard(0).shardsWithState(ShardRoutingState.INITIALIZING));
                state = cluster.reroute(state, new ClusterRerouteRequest());
            }

            IndexShardRoutingTable preTable = state.routingTable().index("index").shard(0);
            final Set<String> unchangedNodeIds;
            final IndexShardRoutingTable postTable;

            if (randomBoolean()) {
                // simulate node removal
                List<DiscoveryNode> nodesToRemove = randomSubsetOf(2, dataNodes);
                unchangedNodeIds = dataNodes.stream().filter(n -> nodesToRemove.contains(n) == false)
                    .map(DiscoveryNode::getId).collect(Collectors.toSet());

                state = cluster.removeNodes(state, nodesToRemove);
                postTable = state.routingTable().index("index").shard(0);

                assertTrue("not all shards started in " + state.toString(), postTable.allShardsStarted());
                assertThat(postTable.toString(), postTable.getAllAllocationIds(), everyItem(is(in(preTable.getAllAllocationIds()))));
            } else {
                // fake an election where conflicting nodes are removed and readded
                state = ClusterState.builder(state).nodes(DiscoveryNodes.builder(state.nodes()).masterNodeId(null).build()).build();

                List<DiscoveryNode> conflictingNodes = randomSubsetOf(2, dataNodes);
                unchangedNodeIds = dataNodes.stream().filter(n -> conflictingNodes.contains(n) == false)
                    .map(DiscoveryNode::getId).collect(Collectors.toSet());

                List<DiscoveryNode> nodesToAdd = conflictingNodes.stream()
                    .map(n -> new DiscoveryNode(n.getName(), n.getId(), buildNewFakeTransportAddress(),
                        n.getAttributes(), n.getRoles(), n.getVersion()))
                    .collect(Collectors.toList());

                if (randomBoolean()) {
                    nodesToAdd.add(createNode(DiscoveryNodeRole.DATA_ROLE));
                }

                state = cluster.joinNodesAndBecomeMaster(state, nodesToAdd);
                postTable = state.routingTable().index("index").shard(0);
            }

            Set<String> unchangedAllocationIds = preTable.getShards().stream().filter(shr -> unchangedNodeIds.contains(shr.currentNodeId()))
                .map(shr -> shr.allocationId().getId()).collect(Collectors.toSet());

            assertThat(postTable.toString(), unchangedAllocationIds, everyItem(is(in(postTable.getAllAllocationIds()))));

            postTable.getShards().forEach(
                shardRouting -> {
                    if (shardRouting.assignedToNode() && unchangedAllocationIds.contains(shardRouting.allocationId().getId())) {
                        assertTrue("Shard should be active: " + shardRouting, shardRouting.active());
                    }
                }
            );
        } finally {
            terminate(threadPool);
        }
    }

    public void testOnlyAutoExpandAllocationFilteringAfterAllNodesUpgraded() {
        final ThreadPool threadPool = new TestThreadPool(getClass().getName());
        final ClusterStateChanges cluster = new ClusterStateChanges(xContentRegistry(), threadPool);

        try {
            List<DiscoveryNode> allNodes = new ArrayList<>();
            DiscoveryNode oldNode =
                createNode(VersionUtils.randomVersionBetween(random(), LegacyESVersion.V_7_0_0, LegacyESVersion.V_7_5_1),
                DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.DATA_ROLE); // local node is the master
            allNodes.add(oldNode);
            ClusterState state = ClusterStateCreationUtils.state(oldNode, oldNode, allNodes.toArray(new DiscoveryNode[0]));

            CreateIndexRequest request = new CreateIndexRequest("index",
                Settings.builder()
                    .put(SETTING_NUMBER_OF_SHARDS, 1)
                    .put(SETTING_AUTO_EXPAND_REPLICAS, "0-all").build())
                .waitForActiveShards(ActiveShardCount.NONE);
            state = cluster.createIndex(state, request);
            assertTrue(state.metadata().hasIndex("index"));
            while (state.routingTable().index("index").shard(0).allShardsStarted() == false) {
                logger.info(state);
                state = cluster.applyStartedShards(state,
                    state.routingTable().index("index").shard(0).shardsWithState(ShardRoutingState.INITIALIZING));
                state = cluster.reroute(state, new ClusterRerouteRequest());
            }

            DiscoveryNode newNode = createNode(LegacyESVersion.V_7_6_0,
                DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.DATA_ROLE); // local node is the master

            state = cluster.addNodes(state, Collections.singletonList(newNode));

            // use allocation filtering
            state = cluster.updateSettings(state, new UpdateSettingsRequest("index").settings(Settings.builder()
                .put(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "._name", oldNode.getName()).build()));

            while (state.routingTable().index("index").shard(0).allShardsStarted() == false) {
                logger.info(state);
                state = cluster.applyStartedShards(state,
                    state.routingTable().index("index").shard(0).shardsWithState(ShardRoutingState.INITIALIZING));
                state = cluster.reroute(state, new ClusterRerouteRequest());
            }

            // check that presence of old node means that auto-expansion does not take allocation filtering into account
            assertThat(state.routingTable().index("index").shard(0).size(), equalTo(2));

            // remove old node and check that auto-expansion takes allocation filtering into account
            state = cluster.removeNodes(state, Collections.singletonList(oldNode));
            assertThat(state.routingTable().index("index").shard(0).size(), equalTo(1));
        } finally {
            terminate(threadPool);
        }
    }
}

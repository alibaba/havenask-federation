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

package org.havenask.snapshots;

import org.havenask.Version;
import org.havenask.cluster.ClusterState;
import org.havenask.cluster.SnapshotsInProgress;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.cluster.metadata.Metadata;
import org.havenask.cluster.node.DiscoveryNodes;
import org.havenask.cluster.routing.IndexRoutingTable;
import org.havenask.cluster.routing.IndexShardRoutingTable;
import org.havenask.cluster.routing.RoutingTable;
import org.havenask.cluster.routing.ShardRoutingState;
import org.havenask.cluster.routing.TestShardRouting;
import org.havenask.common.UUIDs;
import org.havenask.common.collect.ImmutableOpenMap;
import org.havenask.common.settings.Settings;
import org.havenask.index.Index;
import org.havenask.index.shard.ShardId;
import org.havenask.repositories.IndexId;
import org.havenask.repositories.RepositoryShardId;
import org.havenask.test.HavenaskTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.havenask.cluster.metadata.IndexMetadata.SETTING_VERSION_CREATED;
import static org.hamcrest.Matchers.is;

public class SnapshotsServiceTests extends HavenaskTestCase {

    public void testNoopShardStateUpdates() throws Exception {
        final String repoName = "test-repo";
        final Snapshot snapshot = snapshot(repoName, "snapshot-1");
        final SnapshotsInProgress.Entry snapshotNoShards = snapshotEntry(snapshot, Collections.emptyList(), ImmutableOpenMap.of());

        final String indexName1 = "index-1";
        final ShardId shardId1 = new ShardId(index(indexName1), 0);
        {
            final ClusterState state = stateWithSnapshots(snapshotNoShards);
            final SnapshotsService.ShardSnapshotUpdate shardCompletion =
                    new SnapshotsService.ShardSnapshotUpdate(snapshot, shardId1, successfulShardStatus(uuid()));
            assertIsNoop(state, shardCompletion);
        }
        {
            final ClusterState state = stateWithSnapshots(
                    snapshotEntry(
                            snapshot, Collections.singletonList(indexId(indexName1)), shardsMap(shardId1, initShardStatus(uuid()))));
            final SnapshotsService.ShardSnapshotUpdate shardCompletion = new SnapshotsService.ShardSnapshotUpdate(
                    snapshot("other-repo", snapshot.getSnapshotId().getName()), shardId1, successfulShardStatus(uuid()));
            assertIsNoop(state, shardCompletion);
        }
    }

    public void testUpdateSnapshotToSuccess() throws Exception {
        final String repoName = "test-repo";
        final Snapshot sn1 = snapshot(repoName, "snapshot-1");
        final String indexName1 = "index-1";
        final String dataNodeId = uuid();
        final IndexId indexId1 = indexId(indexName1);
        final ShardId shardId1 = new ShardId(index(indexName1), 0);
        final SnapshotsInProgress.Entry snapshotSingleShard =
                snapshotEntry(sn1, Collections.singletonList(indexId1), shardsMap(shardId1, initShardStatus(dataNodeId)));

        assertThat(snapshotSingleShard.state(), is(SnapshotsInProgress.State.STARTED));

        final SnapshotsService.ShardSnapshotUpdate completeShard = successUpdate(sn1, shardId1, dataNodeId);
        final ClusterState updatedClusterState = applyUpdates(stateWithSnapshots(snapshotSingleShard), completeShard);
        final SnapshotsInProgress snapshotsInProgress = updatedClusterState.custom(SnapshotsInProgress.TYPE);
        final SnapshotsInProgress.Entry updatedSnapshot1 = snapshotsInProgress.entries().get(0);
        assertThat(updatedSnapshot1.state(), is(SnapshotsInProgress.State.SUCCESS));
        assertIsNoop(updatedClusterState, completeShard);
    }

    public void testUpdateSnapshotMultipleShards() throws Exception {
        final String repoName = "test-repo";
        final Snapshot sn1 = snapshot(repoName, "snapshot-1");
        final String indexName1 = "index-1";
        final String dataNodeId = uuid();
        final IndexId indexId1 = indexId(indexName1);
        final Index routingIndex1 = index(indexName1);
        final ShardId shardId1 = new ShardId(routingIndex1, 0);
        final ShardId shardId2 = new ShardId(routingIndex1, 1);
        final SnapshotsInProgress.ShardSnapshotStatus shardInitStatus = initShardStatus(dataNodeId);
        final SnapshotsInProgress.Entry snapshotSingleShard = snapshotEntry(sn1, Collections.singletonList(indexId1),
                ImmutableOpenMap.builder(shardsMap(shardId1, shardInitStatus)).fPut(shardId2, shardInitStatus).build());

        assertThat(snapshotSingleShard.state(), is(SnapshotsInProgress.State.STARTED));

        final SnapshotsService.ShardSnapshotUpdate completeShard = successUpdate(sn1, shardId1, dataNodeId);
        final ClusterState updatedClusterState = applyUpdates(stateWithSnapshots(snapshotSingleShard), completeShard);
        final SnapshotsInProgress snapshotsInProgress = updatedClusterState.custom(SnapshotsInProgress.TYPE);
        final SnapshotsInProgress.Entry updatedSnapshot1 = snapshotsInProgress.entries().get(0);
        assertThat(updatedSnapshot1.state(), is(SnapshotsInProgress.State.STARTED));
        assertIsNoop(updatedClusterState, completeShard);
    }

    public void testUpdateCloneToSuccess() throws Exception {
        final String repoName = "test-repo";
        final Snapshot sourceSnapshot = snapshot(repoName, "source-snapshot");
        final Snapshot targetSnapshot = snapshot(repoName, "target-snapshot");
        final String indexName1 = "index-1";
        final String dataNodeId = uuid();
        final IndexId indexId1 = indexId(indexName1);
        final RepositoryShardId shardId1 = new RepositoryShardId(indexId1, 0);
        final SnapshotsInProgress.Entry cloneSingleShard =
                cloneEntry(targetSnapshot, sourceSnapshot.getSnapshotId(), clonesMap(shardId1, initShardStatus(dataNodeId)));

        assertThat(cloneSingleShard.state(), is(SnapshotsInProgress.State.STARTED));

        final SnapshotsService.ShardSnapshotUpdate completeShard = successUpdate(targetSnapshot, shardId1, dataNodeId);
        final ClusterState updatedClusterState = applyUpdates(stateWithSnapshots(cloneSingleShard), completeShard);
        final SnapshotsInProgress snapshotsInProgress = updatedClusterState.custom(SnapshotsInProgress.TYPE);
        final SnapshotsInProgress.Entry updatedSnapshot1 = snapshotsInProgress.entries().get(0);
        assertThat(updatedSnapshot1.state(), is(SnapshotsInProgress.State.SUCCESS));
        assertIsNoop(updatedClusterState, completeShard);
    }

    public void testUpdateCloneMultipleShards() throws Exception {
        final String repoName = "test-repo";
        final Snapshot sourceSnapshot = snapshot(repoName, "source-snapshot");
        final Snapshot targetSnapshot = snapshot(repoName, "target-snapshot");
        final String indexName1 = "index-1";
        final String dataNodeId = uuid();
        final IndexId indexId1 = indexId(indexName1);
        final RepositoryShardId shardId1 = new RepositoryShardId(indexId1, 0);
        final RepositoryShardId shardId2 = new RepositoryShardId(indexId1, 1);
        final SnapshotsInProgress.ShardSnapshotStatus shardInitStatus = initShardStatus(dataNodeId);
        final SnapshotsInProgress.Entry cloneMultipleShards = cloneEntry(targetSnapshot, sourceSnapshot.getSnapshotId(),
                ImmutableOpenMap.builder(clonesMap(shardId1, shardInitStatus)).fPut(shardId2, shardInitStatus).build());

        assertThat(cloneMultipleShards.state(), is(SnapshotsInProgress.State.STARTED));

        final SnapshotsService.ShardSnapshotUpdate completeShard = successUpdate(targetSnapshot, shardId1, dataNodeId);
        final ClusterState updatedClusterState = applyUpdates(stateWithSnapshots(cloneMultipleShards), completeShard);
        final SnapshotsInProgress snapshotsInProgress = updatedClusterState.custom(SnapshotsInProgress.TYPE);
        final SnapshotsInProgress.Entry updatedSnapshot1 = snapshotsInProgress.entries().get(0);
        assertThat(updatedSnapshot1.state(), is(SnapshotsInProgress.State.STARTED));
        assertIsNoop(updatedClusterState, completeShard);
    }

    public void testCompletedCloneStartsSnapshot() throws Exception {
        final String repoName = "test-repo";
        final Snapshot sourceSnapshot = snapshot(repoName, "source-snapshot");
        final Snapshot targetSnapshot = snapshot(repoName, "target-snapshot");
        final String indexName1 = "index-1";
        final String dataNodeId = uuid();
        final IndexId indexId1 = indexId(indexName1);
        final RepositoryShardId shardId1 = new RepositoryShardId(indexId1, 0);
        final SnapshotsInProgress.ShardSnapshotStatus shardInitStatus = initShardStatus(dataNodeId);
        final SnapshotsInProgress.Entry cloneSingleShard = cloneEntry(targetSnapshot, sourceSnapshot.getSnapshotId(),
                clonesMap(shardId1, shardInitStatus));

        final ClusterState stateWithIndex = stateWithUnassignedIndices(indexName1);
        final Snapshot plainSnapshot = snapshot(repoName, "test-snapshot");
        final ShardId routingShardId1 = new ShardId(stateWithIndex.metadata().index(indexName1).getIndex(), 0);
        final SnapshotsInProgress.Entry snapshotSingleShard = snapshotEntry(plainSnapshot, Collections.singletonList(indexId1),
                shardsMap(routingShardId1, SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED));

        assertThat(cloneSingleShard.state(), is(SnapshotsInProgress.State.STARTED));

        // 1. case: shard that just finished cloning is unassigned -> shard snapshot should go to MISSING state
        final ClusterState stateWithUnassignedRoutingShard = stateWithSnapshots(stateWithIndex, cloneSingleShard, snapshotSingleShard);
        final SnapshotsService.ShardSnapshotUpdate completeShardClone = successUpdate(targetSnapshot, shardId1, uuid());
        {
            final ClusterState updatedClusterState = applyUpdates(stateWithUnassignedRoutingShard, completeShardClone);
            final SnapshotsInProgress snapshotsInProgress = updatedClusterState.custom(SnapshotsInProgress.TYPE);
            final SnapshotsInProgress.Entry completedClone = snapshotsInProgress.entries().get(0);
            assertThat(completedClone.state(), is(SnapshotsInProgress.State.SUCCESS));
            final SnapshotsInProgress.Entry startedSnapshot = snapshotsInProgress.entries().get(1);
            assertThat(startedSnapshot.state(), is(SnapshotsInProgress.State.SUCCESS));
            assertThat(startedSnapshot.shards().get(routingShardId1).state(), is(SnapshotsInProgress.ShardState.MISSING));
            assertIsNoop(updatedClusterState, completeShardClone);
        }

        // 2. case: shard that just finished cloning is assigned correctly -> shard snapshot should go to INIT state
        final ClusterState stateWithAssignedRoutingShard =
                ClusterState.builder(stateWithUnassignedRoutingShard).routingTable(
                        RoutingTable.builder(stateWithUnassignedRoutingShard.routingTable()).add(
                                IndexRoutingTable.builder(routingShardId1.getIndex()).addIndexShard(
                                        new IndexShardRoutingTable.Builder(routingShardId1).addShard(
                                                TestShardRouting.newShardRouting(
                                                        routingShardId1, dataNodeId, true, ShardRoutingState.STARTED)
                                        ).build())).build()).build();
        {
            final ClusterState updatedClusterState = applyUpdates(stateWithAssignedRoutingShard, completeShardClone);
            final SnapshotsInProgress snapshotsInProgress = updatedClusterState.custom(SnapshotsInProgress.TYPE);
            final SnapshotsInProgress.Entry completedClone = snapshotsInProgress.entries().get(0);
            assertThat(completedClone.state(), is(SnapshotsInProgress.State.SUCCESS));
            final SnapshotsInProgress.Entry startedSnapshot = snapshotsInProgress.entries().get(1);
            assertThat(startedSnapshot.state(), is(SnapshotsInProgress.State.STARTED));
            final SnapshotsInProgress.ShardSnapshotStatus shardSnapshotStatus = startedSnapshot.shards().get(routingShardId1);
            assertThat(shardSnapshotStatus.state(), is(SnapshotsInProgress.ShardState.INIT));
            assertThat(shardSnapshotStatus.nodeId(), is(dataNodeId));
            assertIsNoop(updatedClusterState, completeShardClone);
        }

        // 3. case: shard that just finished cloning is currently initializing -> shard snapshot should go to WAITING state
        final ClusterState stateWithInitializingRoutingShard =
                ClusterState.builder(stateWithUnassignedRoutingShard).routingTable(
                        RoutingTable.builder(stateWithUnassignedRoutingShard.routingTable()).add(
                                IndexRoutingTable.builder(routingShardId1.getIndex()).addIndexShard(
                                        new IndexShardRoutingTable.Builder(routingShardId1).addShard(
                                                TestShardRouting.newShardRouting(
                                                        routingShardId1, dataNodeId, true, ShardRoutingState.INITIALIZING)
                                        ).build())).build()).build();
        {
            final ClusterState updatedClusterState = applyUpdates(stateWithInitializingRoutingShard, completeShardClone);
            final SnapshotsInProgress snapshotsInProgress = updatedClusterState.custom(SnapshotsInProgress.TYPE);
            final SnapshotsInProgress.Entry completedClone = snapshotsInProgress.entries().get(0);
            assertThat(completedClone.state(), is(SnapshotsInProgress.State.SUCCESS));
            final SnapshotsInProgress.Entry startedSnapshot = snapshotsInProgress.entries().get(1);
            assertThat(startedSnapshot.state(), is(SnapshotsInProgress.State.STARTED));
            assertThat(startedSnapshot.shards().get(routingShardId1).state(), is(SnapshotsInProgress.ShardState.WAITING));
            assertIsNoop(updatedClusterState, completeShardClone);
        }
    }

    public void testCompletedSnapshotStartsClone() throws Exception {
        final String repoName = "test-repo";
        final Snapshot sourceSnapshot = snapshot(repoName, "source-snapshot");
        final Snapshot targetSnapshot = snapshot(repoName, "target-snapshot");
        final String indexName = "index-1";
        final String dataNodeId = uuid();
        final IndexId indexId1 = indexId(indexName);
        final RepositoryShardId repositoryShardId = new RepositoryShardId(indexId1, 0);
        final SnapshotsInProgress.Entry cloneSingleShard = cloneEntry(targetSnapshot, sourceSnapshot.getSnapshotId(),
                clonesMap(repositoryShardId, SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED));

        final ClusterState stateWithIndex = stateWithUnassignedIndices(indexName);
        final Snapshot plainSnapshot = snapshot(repoName, "test-snapshot");
        final ShardId routingShardId = new ShardId(stateWithIndex.metadata().index(indexName).getIndex(), 0);
        final SnapshotsInProgress.Entry snapshotSingleShard = snapshotEntry(plainSnapshot, Collections.singletonList(indexId1),
                shardsMap(routingShardId, initShardStatus(dataNodeId)));

        assertThat(cloneSingleShard.state(), is(SnapshotsInProgress.State.STARTED));

        final SnapshotsService.ShardSnapshotUpdate completeShard = successUpdate(plainSnapshot, routingShardId, dataNodeId);

        final ClusterState updatedClusterState = applyUpdates(stateWithSnapshots(snapshotSingleShard, cloneSingleShard), completeShard);
        final SnapshotsInProgress snapshotsInProgress = updatedClusterState.custom(SnapshotsInProgress.TYPE);
        final SnapshotsInProgress.Entry completedClone = snapshotsInProgress.entries().get(0);
        assertThat(completedClone.state(), is(SnapshotsInProgress.State.SUCCESS));
        final SnapshotsInProgress.Entry startedSnapshot = snapshotsInProgress.entries().get(1);
        assertThat(startedSnapshot.state(), is(SnapshotsInProgress.State.STARTED));
        final SnapshotsInProgress.ShardSnapshotStatus shardCloneStatus = startedSnapshot.clones().get(repositoryShardId);
        assertThat(shardCloneStatus.state(), is(SnapshotsInProgress.ShardState.INIT));
        assertThat(shardCloneStatus.nodeId(), is(updatedClusterState.nodes().getLocalNodeId()));
        assertIsNoop(updatedClusterState, completeShard);
    }

    public void testCompletedSnapshotStartsNextSnapshot() throws Exception {
        final String repoName = "test-repo";
        final String indexName = "index-1";
        final String dataNodeId = uuid();
        final IndexId indexId1 = indexId(indexName);

        final ClusterState stateWithIndex = stateWithUnassignedIndices(indexName);
        final Snapshot plainSnapshot = snapshot(repoName, "test-snapshot-1");
        final ShardId routingShardId = new ShardId(stateWithIndex.metadata().index(indexName).getIndex(), 0);
        final SnapshotsInProgress.Entry snapshotSingleShard = snapshotEntry(plainSnapshot, Collections.singletonList(indexId1),
                shardsMap(routingShardId, initShardStatus(dataNodeId)));

        final Snapshot queuedSnapshot = snapshot(repoName, "test-snapshot-2");
        final SnapshotsInProgress.Entry queuedSnapshotSingleShard = snapshotEntry(queuedSnapshot, Collections.singletonList(indexId1),
                shardsMap(routingShardId, SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED));

        final SnapshotsService.ShardSnapshotUpdate completeShard = successUpdate(plainSnapshot, routingShardId, dataNodeId);

        final ClusterState updatedClusterState =
                applyUpdates(stateWithSnapshots(snapshotSingleShard, queuedSnapshotSingleShard), completeShard);
        final SnapshotsInProgress snapshotsInProgress = updatedClusterState.custom(SnapshotsInProgress.TYPE);
        final SnapshotsInProgress.Entry completedSnapshot = snapshotsInProgress.entries().get(0);
        assertThat(completedSnapshot.state(), is(SnapshotsInProgress.State.SUCCESS));
        final SnapshotsInProgress.Entry startedSnapshot = snapshotsInProgress.entries().get(1);
        assertThat(startedSnapshot.state(), is(SnapshotsInProgress.State.STARTED));
        final SnapshotsInProgress.ShardSnapshotStatus shardSnapshotStatus = startedSnapshot.shards().get(routingShardId);
        assertThat(shardSnapshotStatus.state(), is(SnapshotsInProgress.ShardState.INIT));
        assertThat(shardSnapshotStatus.nodeId(), is(dataNodeId));
        assertIsNoop(updatedClusterState, completeShard);
    }

    public void testCompletedCloneStartsNextClone() throws Exception {
        final String repoName = "test-repo";
        final Snapshot sourceSnapshot = snapshot(repoName, "source-snapshot");
        final Snapshot targetSnapshot = snapshot(repoName, "target-snapshot");
        final String indexName1 = "index-1";
        final IndexId indexId1 = indexId(indexName1);
        final RepositoryShardId shardId1 = new RepositoryShardId(indexId1, 0);
        final String masterNodeId = uuid();
        final SnapshotsInProgress.Entry cloneSingleShard = cloneEntry(targetSnapshot, sourceSnapshot.getSnapshotId(),
                clonesMap(shardId1, initShardStatus(masterNodeId)));

        final Snapshot queuedTargetSnapshot = snapshot(repoName, "test-snapshot");
        final SnapshotsInProgress.Entry queuedClone = cloneEntry(queuedTargetSnapshot, sourceSnapshot.getSnapshotId(),
                clonesMap(shardId1, SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED));

        assertThat(cloneSingleShard.state(), is(SnapshotsInProgress.State.STARTED));

        final ClusterState stateWithUnassignedRoutingShard = stateWithSnapshots(
                ClusterState.builder(ClusterState.EMPTY_STATE).nodes(discoveryNodes(masterNodeId)).build(), cloneSingleShard, queuedClone);
        final SnapshotsService.ShardSnapshotUpdate completeShardClone = successUpdate(targetSnapshot, shardId1, masterNodeId);

        final ClusterState updatedClusterState = applyUpdates(stateWithUnassignedRoutingShard, completeShardClone);
        final SnapshotsInProgress snapshotsInProgress = updatedClusterState.custom(SnapshotsInProgress.TYPE);
        final SnapshotsInProgress.Entry completedClone = snapshotsInProgress.entries().get(0);
        assertThat(completedClone.state(), is(SnapshotsInProgress.State.SUCCESS));
        final SnapshotsInProgress.Entry startedSnapshot = snapshotsInProgress.entries().get(1);
        assertThat(startedSnapshot.state(), is(SnapshotsInProgress.State.STARTED));
        assertThat(startedSnapshot.clones().get(shardId1).state(), is(SnapshotsInProgress.ShardState.INIT));
        assertIsNoop(updatedClusterState, completeShardClone);
    }

    private static DiscoveryNodes discoveryNodes(String localNodeId) {
        return DiscoveryNodes.builder().localNodeId(localNodeId).build();
    }

    private static ImmutableOpenMap<ShardId, SnapshotsInProgress.ShardSnapshotStatus> shardsMap(
            ShardId shardId, SnapshotsInProgress.ShardSnapshotStatus shardStatus) {
        return ImmutableOpenMap.<ShardId, SnapshotsInProgress.ShardSnapshotStatus>builder().fPut(shardId, shardStatus).build();
    }

    private static ImmutableOpenMap<RepositoryShardId, SnapshotsInProgress.ShardSnapshotStatus> clonesMap(
            RepositoryShardId shardId, SnapshotsInProgress.ShardSnapshotStatus shardStatus) {
        return ImmutableOpenMap.<RepositoryShardId, SnapshotsInProgress.ShardSnapshotStatus>builder().fPut(shardId, shardStatus).build();
    }

    private static SnapshotsService.ShardSnapshotUpdate successUpdate(Snapshot snapshot, ShardId shardId, String nodeId) {
        return new SnapshotsService.ShardSnapshotUpdate(snapshot, shardId, successfulShardStatus(nodeId));
    }

    private static SnapshotsService.ShardSnapshotUpdate successUpdate(Snapshot snapshot, RepositoryShardId shardId, String nodeId) {
        return new SnapshotsService.ShardSnapshotUpdate(snapshot, shardId, successfulShardStatus(nodeId));
    }

    private static ClusterState stateWithUnassignedIndices(String... indexNames) {
        final Metadata.Builder metaBuilder = Metadata.builder(Metadata.EMPTY_METADATA);
        for (String index : indexNames) {
            metaBuilder.put(IndexMetadata.builder(index)
                    .settings(Settings.builder().put(SETTING_VERSION_CREATED, Version.CURRENT.id))
                    .numberOfShards(1).numberOfReplicas(0)
                    .build(), false);
        }
        final RoutingTable.Builder routingTable = RoutingTable.builder();
        for (String index : indexNames) {
            final Index idx = metaBuilder.get(index).getIndex();
            routingTable.add(IndexRoutingTable.builder(idx).addIndexShard(
                    new IndexShardRoutingTable.Builder(new ShardId(idx, 0)).build()));
        }
        return ClusterState.builder(ClusterState.EMPTY_STATE).metadata(metaBuilder).routingTable(routingTable.build()).build();
    }

    private static ClusterState stateWithSnapshots(ClusterState state, SnapshotsInProgress.Entry... entries) {
        return ClusterState.builder(state).version(state.version() + 1L)
                .putCustom(SnapshotsInProgress.TYPE, SnapshotsInProgress.of(Arrays.asList(entries))).build();
    }

    private static ClusterState stateWithSnapshots(SnapshotsInProgress.Entry... entries) {
        return stateWithSnapshots(ClusterState.builder(ClusterState.EMPTY_STATE).nodes(discoveryNodes(uuid())).build(), entries);
    }

    private static void assertIsNoop(ClusterState state, SnapshotsService.ShardSnapshotUpdate shardCompletion) throws Exception {
        assertSame(applyUpdates(state, shardCompletion), state);
    }

    private static ClusterState applyUpdates(ClusterState state, SnapshotsService.ShardSnapshotUpdate... updates) throws Exception {
        return SnapshotsService.SHARD_STATE_EXECUTOR.execute(state, Arrays.asList(updates)).resultingState;
    }

    private static SnapshotsInProgress.Entry snapshotEntry(Snapshot snapshot, List<IndexId> indexIds,
                                                           ImmutableOpenMap<ShardId, SnapshotsInProgress.ShardSnapshotStatus> shards) {
        return SnapshotsInProgress.startedEntry(snapshot, randomBoolean(), randomBoolean(), indexIds, Collections.emptyList(),
                1L, randomNonNegativeLong(), shards, Collections.emptyMap(), Version.CURRENT);
    }

    private static SnapshotsInProgress.Entry cloneEntry(
            Snapshot snapshot, SnapshotId source, ImmutableOpenMap<RepositoryShardId, SnapshotsInProgress.ShardSnapshotStatus> clones) {
        final List<IndexId> indexIds = StreamSupport.stream(clones.keys().spliterator(), false)
                .map(k -> k.value.index()).distinct().collect(Collectors.toList());
        return SnapshotsInProgress.startClone(snapshot, source, indexIds, 1L, randomNonNegativeLong(), Version.CURRENT).withClones(clones);
    }

    private static SnapshotsInProgress.ShardSnapshotStatus initShardStatus(String nodeId) {
        return new SnapshotsInProgress.ShardSnapshotStatus(nodeId, uuid());
    }

    private static SnapshotsInProgress.ShardSnapshotStatus successfulShardStatus(String nodeId) {
        return new SnapshotsInProgress.ShardSnapshotStatus(nodeId, SnapshotsInProgress.ShardState.SUCCESS, uuid());
    }

    private static Snapshot snapshot(String repoName, String name) {
        return new Snapshot(repoName, new SnapshotId(name, uuid()));
    }

    private static Index index(String name) {
        return new Index(name, uuid());
    }

    private static IndexId indexId(String name) {
        return new IndexId(name, uuid());
    }

    private static String uuid() {
        return UUIDs.randomBase64UUID(random());
    }
}

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
import org.havenask.action.admin.indices.close.CloseIndexClusterStateUpdateRequest;
import org.havenask.action.admin.indices.close.CloseIndexResponse;
import org.havenask.action.admin.indices.close.CloseIndexResponse.IndexResult;
import org.havenask.action.admin.indices.datastream.DeleteDataStreamRequestTests;
import org.havenask.cluster.ClusterName;
import org.havenask.cluster.ClusterState;
import org.havenask.cluster.RestoreInProgress;
import org.havenask.cluster.SnapshotsInProgress;
import org.havenask.cluster.block.ClusterBlock;
import org.havenask.cluster.block.ClusterBlocks;
import org.havenask.cluster.node.DiscoveryNode;
import org.havenask.cluster.node.DiscoveryNodeRole;
import org.havenask.cluster.node.DiscoveryNodes;
import org.havenask.cluster.routing.IndexRoutingTable;
import org.havenask.cluster.routing.IndexShardRoutingTable;
import org.havenask.cluster.routing.RoutingTable;
import org.havenask.cluster.routing.ShardRouting;
import org.havenask.cluster.routing.ShardRoutingState;
import org.havenask.cluster.routing.UnassignedInfo;
import org.havenask.cluster.service.ClusterService;
import org.havenask.common.Nullable;
import org.havenask.common.Strings;
import org.havenask.common.collect.ImmutableOpenMap;
import org.havenask.common.collect.Tuple;
import org.havenask.common.settings.Settings;
import org.havenask.index.Index;
import org.havenask.index.IndexNotFoundException;
import org.havenask.index.shard.ShardId;
import org.havenask.repositories.IndexId;
import org.havenask.snapshots.Snapshot;
import org.havenask.snapshots.SnapshotId;
import org.havenask.snapshots.SnapshotInProgressException;
import org.havenask.snapshots.SnapshotInfoTests;
import org.havenask.test.HavenaskTestCase;
import org.havenask.test.VersionUtils;
import org.hamcrest.CoreMatchers;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonMap;
import static java.util.Collections.unmodifiableMap;
import static org.havenask.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.havenask.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.havenask.cluster.metadata.IndexMetadata.SETTING_VERSION_CREATED;
import static org.havenask.cluster.metadata.MetadataIndexStateService.INDEX_CLOSED_BLOCK;
import static org.havenask.cluster.metadata.MetadataIndexStateService.INDEX_CLOSED_BLOCK_ID;
import static org.havenask.cluster.routing.TestShardRouting.newShardRouting;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MetadataIndexStateServiceTests extends HavenaskTestCase {

    public void testCloseRoutingTable() {
        final Set<Index> nonBlockedIndices = new HashSet<>();
        final Map<Index, ClusterBlock> blockedIndices = new HashMap<>();
        final Map<Index, IndexResult> results = new HashMap<>();

        ClusterState state = ClusterState.builder(new ClusterName("testCloseRoutingTable")).build();
        for (int i = 0; i < randomIntBetween(1, 25); i++) {
            final String indexName = "index-" + i;

            if (randomBoolean()) {
                state = addOpenedIndex(indexName, randomIntBetween(1, 5), randomIntBetween(0, 5), state);
                nonBlockedIndices.add(state.metadata().index(indexName).getIndex());
            } else {
                final ClusterBlock closingBlock = MetadataIndexStateService.createIndexClosingBlock();
                state = addBlockedIndex(indexName, randomIntBetween(1, 5), randomIntBetween(0, 5), state, closingBlock);
                final Index index = state.metadata().index(indexName).getIndex();
                blockedIndices.put(index, closingBlock);
                if (randomBoolean()) {
                    results.put(index, new CloseIndexResponse.IndexResult(index));
                } else {
                    results.put(index, new CloseIndexResponse.IndexResult(index, new Exception("test")));
                }
            }
        }

        final ClusterState updatedState = MetadataIndexStateService.closeRoutingTable(state, blockedIndices, results).v1();
        assertThat(updatedState.metadata().indices().size(), equalTo(nonBlockedIndices.size() + blockedIndices.size()));

        for (Index nonBlockedIndex : nonBlockedIndices) {
            assertIsOpened(nonBlockedIndex.getName(), updatedState);
            assertThat(updatedState.blocks().hasIndexBlockWithId(nonBlockedIndex.getName(), INDEX_CLOSED_BLOCK_ID), is(false));
        }
        for (Index blockedIndex : blockedIndices.keySet()) {
            if (results.get(blockedIndex).hasFailures() == false) {
                assertIsClosed(blockedIndex.getName(), updatedState);
            } else {
                assertIsOpened(blockedIndex.getName(), updatedState);
                assertThat(updatedState.blocks().hasIndexBlockWithId(blockedIndex.getName(), INDEX_CLOSED_BLOCK_ID), is(true));
            }
        }
    }

    public void testCloseRoutingTableWithRestoredIndex() {
        ClusterState state = ClusterState.builder(new ClusterName("testCloseRoutingTableWithRestoredIndex")).build();

        String indexName = "restored-index";
        ClusterBlock block = MetadataIndexStateService.createIndexClosingBlock();
        state = addRestoredIndex(indexName, randomIntBetween(1, 5), randomIntBetween(0, 5), state);
        state = ClusterState.builder(state)
            .blocks(ClusterBlocks.builder().blocks(state.blocks()).addIndexBlock(indexName, block))
            .build();

        final Index index = state.metadata().index(indexName).getIndex();
        final ClusterState updatedState =
            MetadataIndexStateService.closeRoutingTable(state, singletonMap(index, block), singletonMap(index, new IndexResult(index)))
                .v1();
        assertIsOpened(index.getName(), updatedState);
        assertThat(updatedState.blocks().hasIndexBlockWithId(index.getName(), INDEX_CLOSED_BLOCK_ID), is(true));
    }

    public void testCloseRoutingTableWithSnapshottedIndex() {
        ClusterState state = ClusterState.builder(new ClusterName("testCloseRoutingTableWithSnapshottedIndex")).build();

        String indexName = "snapshotted-index";
        ClusterBlock block = MetadataIndexStateService.createIndexClosingBlock();
        state = addSnapshotIndex(indexName, randomIntBetween(1, 5), randomIntBetween(0, 5), state);
        state = ClusterState.builder(state)
            .blocks(ClusterBlocks.builder().blocks(state.blocks()).addIndexBlock(indexName, block))
            .build();

        final Index index = state.metadata().index(indexName).getIndex();
        final ClusterState updatedState =
            MetadataIndexStateService.closeRoutingTable(state, singletonMap(index, block), singletonMap(index, new IndexResult(index)))
                .v1();
        assertIsOpened(index.getName(), updatedState);
        assertThat(updatedState.blocks().hasIndexBlockWithId(index.getName(), INDEX_CLOSED_BLOCK_ID), is(true));
    }

    public void testCloseRoutingTableRemovesRoutingTable() {
        final Set<Index> nonBlockedIndices = new HashSet<>();
        final Map<Index, ClusterBlock> blockedIndices = new HashMap<>();
        final Map<Index, IndexResult> results = new HashMap<>();
        final ClusterBlock closingBlock = MetadataIndexStateService.createIndexClosingBlock();

        ClusterState state = ClusterState.builder(new ClusterName("testCloseRoutingTableRemovesRoutingTable")).build();
        for (int i = 0; i < randomIntBetween(1, 25); i++) {
            final String indexName = "index-" + i;

            if (randomBoolean()) {
                state = addOpenedIndex(indexName, randomIntBetween(1, 5), randomIntBetween(0, 5), state);
                nonBlockedIndices.add(state.metadata().index(indexName).getIndex());
            } else {
                state = addBlockedIndex(indexName, randomIntBetween(1, 5), randomIntBetween(0, 5), state, closingBlock);
                final Index index = state.metadata().index(indexName).getIndex();
                blockedIndices.put(index, closingBlock);
                if (randomBoolean()) {
                    results.put(index, new CloseIndexResponse.IndexResult(index));
                } else {
                    results.put(index, new CloseIndexResponse.IndexResult(index, new Exception("test")));
                }
            }
        }

        state = ClusterState.builder(state)
            .nodes(DiscoveryNodes.builder(state.nodes())
                .add(new DiscoveryNode("old_node", buildNewFakeTransportAddress(), emptyMap(),
                    new HashSet<>(DiscoveryNodeRole.BUILT_IN_ROLES), LegacyESVersion.V_7_0_0))
                .add(new DiscoveryNode("new_node", buildNewFakeTransportAddress(), emptyMap(),
                    new HashSet<>(DiscoveryNodeRole.BUILT_IN_ROLES), LegacyESVersion.V_7_2_0)))
            .build();

        state = MetadataIndexStateService.closeRoutingTable(state, blockedIndices, results).v1();
        assertThat(state.metadata().indices().size(), equalTo(nonBlockedIndices.size() + blockedIndices.size()));

        for (Index nonBlockedIndex : nonBlockedIndices) {
            assertIsOpened(nonBlockedIndex.getName(), state);
            assertThat(state.blocks().hasIndexBlockWithId(nonBlockedIndex.getName(), INDEX_CLOSED_BLOCK_ID), is(false));
        }
        for (Index blockedIndex : blockedIndices.keySet()) {
            if (results.get(blockedIndex).hasFailures() == false) {
                IndexMetadata indexMetadata = state.metadata().index(blockedIndex);
                assertThat(indexMetadata.getState(), is(IndexMetadata.State.CLOSE));
                Settings indexSettings = indexMetadata.getSettings();
                assertThat(indexSettings.hasValue(MetadataIndexStateService.VERIFIED_BEFORE_CLOSE_SETTING.getKey()), is(false));
                assertThat(state.blocks().hasIndexBlock(blockedIndex.getName(), MetadataIndexStateService.INDEX_CLOSED_BLOCK), is(true));
                assertThat("Index must have only 1 block with [id=" + MetadataIndexStateService.INDEX_CLOSED_BLOCK_ID + "]",
                    state.blocks().indices().getOrDefault(blockedIndex.getName(), emptySet()).stream()
                        .filter(clusterBlock -> clusterBlock.id() == MetadataIndexStateService.INDEX_CLOSED_BLOCK_ID).count(), equalTo(1L));
                assertThat("Index routing table should have been removed when closing the index on mixed cluster version",
                    state.routingTable().index(blockedIndex), nullValue());
            } else {
                assertIsOpened(blockedIndex.getName(), state);
                assertThat(state.blocks().hasIndexBlock(blockedIndex.getName(), closingBlock), is(true));
            }
        }
    }

    public void testAddIndexClosedBlocks() {
        final ClusterState initialState = ClusterState.builder(new ClusterName("testAddIndexClosedBlocks")).build();
        {
            final Map<Index, ClusterBlock> blockedIndices = new HashMap<>();
            Index[] indices = new Index[]{new Index("_name", "_uid")};
            expectThrows(IndexNotFoundException.class, () ->
                MetadataIndexStateService.addIndexClosedBlocks(indices, blockedIndices, initialState));
            assertTrue(blockedIndices.isEmpty());
        }
        {
            final Map<Index, ClusterBlock> blockedIndices = new HashMap<>();
            Index[] indices = Index.EMPTY_ARRAY;

            ClusterState updatedState = MetadataIndexStateService.addIndexClosedBlocks(indices, blockedIndices, initialState);
            assertSame(initialState, updatedState);
            assertTrue(blockedIndices.isEmpty());
        }
        {
            final Map<Index, ClusterBlock> blockedIndices = new HashMap<>();
            ClusterState state = addClosedIndex("closed", randomIntBetween(1, 3), randomIntBetween(0, 3), initialState);
            Index[] indices = new Index[]{state.metadata().index("closed").getIndex()};

            ClusterState updatedState = MetadataIndexStateService.addIndexClosedBlocks(indices, blockedIndices, state);
            assertSame(state, updatedState);
            assertTrue(blockedIndices.isEmpty());
        }
        {
            final Map<Index, ClusterBlock> blockedIndices = new HashMap<>();
            ClusterState state = addClosedIndex("closed", randomIntBetween(1, 3), randomIntBetween(0, 3), initialState);
            state = addOpenedIndex("opened", randomIntBetween(1, 3), randomIntBetween(0, 3), state);
            Index[] indices = new Index[]{state.metadata().index("opened").getIndex(), state.metadata().index("closed").getIndex()};

            ClusterState updatedState = MetadataIndexStateService.addIndexClosedBlocks(indices, blockedIndices, state);
            assertNotSame(state, updatedState);

            Index opened = updatedState.metadata().index("opened").getIndex();
            assertTrue(blockedIndices.containsKey(opened));
            assertHasBlock("opened", updatedState, blockedIndices.get(opened));

            Index closed = updatedState.metadata().index("closed").getIndex();
            assertFalse(blockedIndices.containsKey(closed));
        }
        {
            IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> {
                    ClusterState state = addRestoredIndex("restored", randomIntBetween(1, 3), randomIntBetween(0, 3), initialState);
                    if (randomBoolean()) {
                        state = addOpenedIndex("opened", randomIntBetween(1, 3), randomIntBetween(0, 3), state);
                    }
                    if (randomBoolean()) {
                        state = addOpenedIndex("closed", randomIntBetween(1, 3), randomIntBetween(0, 3), state);
                    }
                    Index[] indices = new Index[]{state.metadata().index("restored").getIndex()};
                    MetadataIndexStateService.addIndexClosedBlocks(indices, unmodifiableMap(emptyMap()), state);
                });
            assertThat(exception.getMessage(), containsString("Cannot close indices that are being restored: [[restored]]"));
        }
        {
            SnapshotInProgressException exception = expectThrows(SnapshotInProgressException.class, () -> {
                ClusterState state = addSnapshotIndex("snapshotted", randomIntBetween(1, 3), randomIntBetween(0, 3), initialState);
                if (randomBoolean()) {
                    state = addOpenedIndex("opened", randomIntBetween(1, 3), randomIntBetween(0, 3), state);
                }
                if (randomBoolean()) {
                    state = addOpenedIndex("closed", randomIntBetween(1, 3), randomIntBetween(0, 3), state);
                }
                Index[] indices = new Index[]{state.metadata().index("snapshotted").getIndex()};
                MetadataIndexStateService.addIndexClosedBlocks(indices, unmodifiableMap(emptyMap()), state);
            });
            assertThat(exception.getMessage(), containsString("Cannot close indices that are being snapshotted: [[snapshotted]]"));
        }
        {
            final Map<Index, ClusterBlock> blockedIndices = new HashMap<>();
            ClusterState state = addOpenedIndex("index-1", randomIntBetween(1, 3), randomIntBetween(0, 3), initialState);
            state = addOpenedIndex("index-2", randomIntBetween(1, 3), randomIntBetween(0, 3), state);
            state = addOpenedIndex("index-3", randomIntBetween(1, 3), randomIntBetween(0, 3), state);

            Index index1 = state.metadata().index("index-1").getIndex();
            Index index2 = state.metadata().index("index-2").getIndex();
            Index index3 = state.metadata().index("index-3").getIndex();
            Index[] indices = new Index[]{index1, index2, index3};

            ClusterState updatedState = MetadataIndexStateService.addIndexClosedBlocks(indices, blockedIndices, state);
            assertNotSame(state, updatedState);

            for (Index index : indices) {
                assertTrue(blockedIndices.containsKey(index));
                assertHasBlock(index.getName(), updatedState, blockedIndices.get(index));
            }
        }
    }

    public void testAddIndexClosedBlocksReusesBlocks() {
        ClusterState state = ClusterState.builder(new ClusterName("testAddIndexClosedBlocksReuseBlocks")).build();
        state = addOpenedIndex("test", randomIntBetween(1, 3), randomIntBetween(0, 3), state);

        Index test = state.metadata().index("test").getIndex();
        Index[] indices = new Index[]{test};

        final Map<Index, ClusterBlock> blockedIndices = new HashMap<>();
        state = MetadataIndexStateService.addIndexClosedBlocks(indices, blockedIndices, state);

        assertTrue(blockedIndices.containsKey(test));
        assertHasBlock(test.getName(), state, blockedIndices.get(test));

        final Map<Index, ClusterBlock> blockedIndices2 = new HashMap<>();
        state = MetadataIndexStateService.addIndexClosedBlocks(indices, blockedIndices2, state);

        assertTrue(blockedIndices2.containsKey(test));
        assertHasBlock(test.getName(), state, blockedIndices2.get(test));
        assertEquals(blockedIndices.get(test), blockedIndices2.get(test));
    }

    public void testIsIndexVerifiedBeforeClosed() {
        final ClusterState initialState = ClusterState.builder(new ClusterName("testIsIndexMetadataClosed")).build();
        {
            String indexName = "open";
            ClusterState state = addOpenedIndex(indexName, randomIntBetween(1, 3), randomIntBetween(0, 3), initialState);
            assertFalse(MetadataIndexStateService.isIndexVerifiedBeforeClosed(state.getMetadata().index(indexName)));
        }
        {
            String indexName = "closed";
            ClusterState state = addClosedIndex(indexName, randomIntBetween(1, 3), randomIntBetween(0, 3), initialState);
            assertTrue(MetadataIndexStateService.isIndexVerifiedBeforeClosed(state.getMetadata().index(indexName)));
        }
        {
            String indexName = "closed-no-setting";
            IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
                .state(IndexMetadata.State.CLOSE)
                .creationDate(randomNonNegativeLong())
                .settings(Settings.builder()
                    .put(SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(SETTING_NUMBER_OF_SHARDS, randomIntBetween(1, 3))
                    .put(SETTING_NUMBER_OF_REPLICAS, randomIntBetween(0, 3)))
                .build();
            assertFalse(MetadataIndexStateService.isIndexVerifiedBeforeClosed(indexMetadata));
        }
    }

    public void testCloseFailedIfBlockDisappeared() {
        ClusterState state = ClusterState.builder(new ClusterName("failedIfBlockDisappeared")).build();
        Map<Index, ClusterBlock> blockedIndices = new HashMap<>();
        int numIndices = between(1, 10);
        Set<Index> disappearedIndices = new HashSet<>();
        Map<Index, IndexResult> verifyResults = new HashMap<>();
        for (int i = 0; i < numIndices; i++) {
            String indexName = "test-" + i;
            state = addOpenedIndex(indexName, randomIntBetween(1, 3), randomIntBetween(0, 3), state);
            Index index = state.metadata().index(indexName).getIndex();
            state = MetadataIndexStateService.addIndexClosedBlocks(new Index[]{index}, blockedIndices, state);
            if (randomBoolean()) {
                state = ClusterState.builder(state)
                    .blocks(ClusterBlocks.builder().blocks(state.blocks()).removeIndexBlocks(indexName).build())
                    .build();
                disappearedIndices.add(index);
            }
            verifyResults.put(index, new IndexResult(index));
        }
        Collection<IndexResult> closingResults =
            MetadataIndexStateService.closeRoutingTable(state, blockedIndices, unmodifiableMap(verifyResults)).v2();
        assertThat(closingResults, hasSize(numIndices));
        Set<Index> failedIndices = closingResults.stream().filter(IndexResult::hasFailures)
            .map(IndexResult::getIndex).collect(Collectors.toSet());
        assertThat(failedIndices, equalTo(disappearedIndices));
    }

    public void testCloseCurrentWriteIndexForDataStream() {
        int numDataStreams = randomIntBetween(1, 3);
        List<Tuple<String, Integer>> dataStreamsToCreate = new ArrayList<>();
        List<String> writeIndices = new ArrayList<>();
        for (int k = 0; k < numDataStreams; k++) {
            String dataStreamName = randomAlphaOfLength(6).toLowerCase(Locale.ROOT);
            int numBackingIndices = randomIntBetween(1, 5);
            dataStreamsToCreate.add(new Tuple<>(dataStreamName, numBackingIndices));
            writeIndices.add(DataStream.getDefaultBackingIndexName(dataStreamName, numBackingIndices));
        }
        ClusterState cs = DeleteDataStreamRequestTests.getClusterStateWithDataStreams(dataStreamsToCreate,
            org.havenask.common.collect.List.of());

        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(cs);

        List<String> indicesToDelete = randomSubsetOf(randomIntBetween(1, numDataStreams), writeIndices);
        Index[] indicesToDeleteArray = new Index[indicesToDelete.size()];
        for (int k = 0; k < indicesToDelete.size(); k++) {
            Index indexToDelete = cs.metadata().index(indicesToDelete.get(k)).getIndex();
            indicesToDeleteArray[k] = indexToDelete;
        }
        MetadataIndexStateService service = new MetadataIndexStateService(clusterService, null, null, null, null, null, null, null);
        CloseIndexClusterStateUpdateRequest request = new CloseIndexClusterStateUpdateRequest(0L).indices(indicesToDeleteArray);
        Exception e = expectThrows(IllegalArgumentException.class, () -> service.closeIndices(request, null));
        assertThat(e.getMessage(), CoreMatchers.containsString("cannot close the following data stream write indices [" +
                Strings.collectionToCommaDelimitedString(indicesToDelete) + "]"));
    }

    public static ClusterState addOpenedIndex(final String index, final int numShards, final int numReplicas, final ClusterState state) {
        return addIndex(state, index, numShards, numReplicas, IndexMetadata.State.OPEN, null);
    }

    public static ClusterState addClosedIndex(final String index, final int numShards, final int numReplicas, final ClusterState state) {
        return addIndex(state, index, numShards, numReplicas, IndexMetadata.State.CLOSE, INDEX_CLOSED_BLOCK);
    }

    private static ClusterState addBlockedIndex(final String index, final int numShards, final int numReplicas, final ClusterState state,
                                                final ClusterBlock closingBlock) {
        return addIndex(state, index, numShards, numReplicas, IndexMetadata.State.OPEN, closingBlock);
    }

    private static ClusterState addRestoredIndex(final String index, final int numShards, final int numReplicas, final ClusterState state) {
        ClusterState newState = addOpenedIndex(index, numShards, numReplicas, state);

        final ImmutableOpenMap.Builder<ShardId, RestoreInProgress.ShardRestoreStatus> shardsBuilder = ImmutableOpenMap.builder();
        for (ShardRouting shardRouting : newState.routingTable().index(index).randomAllActiveShardsIt()) {
            shardsBuilder.put(shardRouting.shardId(), new RestoreInProgress.ShardRestoreStatus(shardRouting.currentNodeId()));
        }

        final Snapshot snapshot = new Snapshot(randomAlphaOfLength(10), new SnapshotId(randomAlphaOfLength(5), randomAlphaOfLength(5)));
        final RestoreInProgress.Entry entry =
            new RestoreInProgress.Entry("_uuid", snapshot, RestoreInProgress.State.INIT,
                Collections.singletonList(index), shardsBuilder.build());
        return ClusterState.builder(newState)
            .putCustom(RestoreInProgress.TYPE, new RestoreInProgress.Builder().add(entry).build())
            .build();
    }

    private static ClusterState addSnapshotIndex(final String index, final int numShards, final int numReplicas, final ClusterState state) {
        ClusterState newState = addOpenedIndex(index, numShards, numReplicas, state);

        final ImmutableOpenMap.Builder<ShardId, SnapshotsInProgress.ShardSnapshotStatus> shardsBuilder = ImmutableOpenMap.builder();
        for (ShardRouting shardRouting : newState.routingTable().index(index).randomAllActiveShardsIt()) {
            shardsBuilder.put(shardRouting.shardId(), new SnapshotsInProgress.ShardSnapshotStatus(shardRouting.currentNodeId(), "1"));
        }

        final Snapshot snapshot = new Snapshot(randomAlphaOfLength(10), new SnapshotId(randomAlphaOfLength(5), randomAlphaOfLength(5)));
        final SnapshotsInProgress.Entry entry =
            new SnapshotsInProgress.Entry(snapshot, randomBoolean(), false, SnapshotsInProgress.State.INIT,
                Collections.singletonList(new IndexId(index, index)), Collections.emptyList(), randomNonNegativeLong(), randomLong(),
                    shardsBuilder.build(), null, SnapshotInfoTests.randomUserMetadata(), VersionUtils.randomVersion(random()));
        return ClusterState.builder(newState).putCustom(SnapshotsInProgress.TYPE,
                SnapshotsInProgress.of(Collections.singletonList(entry))).build();
    }

    private static ClusterState addIndex(final ClusterState currentState,
                                         final String index,
                                         final int numShards,
                                         final int numReplicas,
                                         final IndexMetadata.State state,
                                         @Nullable final ClusterBlock block) {

        final Settings.Builder settings = Settings.builder()
            .put(SETTING_VERSION_CREATED, Version.CURRENT)
            .put(SETTING_NUMBER_OF_SHARDS, numShards)
            .put(SETTING_NUMBER_OF_REPLICAS, numReplicas);
        if (state == IndexMetadata.State.CLOSE) {
            settings.put(MetadataIndexStateService.VERIFIED_BEFORE_CLOSE_SETTING.getKey(), true);
        }
        final IndexMetadata indexMetadata = IndexMetadata.builder(index)
            .state(state)
            .creationDate(randomNonNegativeLong())
            .settings(settings)
            .build();

        final ClusterState.Builder clusterStateBuilder = ClusterState.builder(currentState);
        clusterStateBuilder.metadata(Metadata.builder(currentState.metadata()).put(indexMetadata, true));

        final IndexRoutingTable.Builder indexRoutingTable = IndexRoutingTable.builder(indexMetadata.getIndex());
        for (int j = 0; j < indexMetadata.getNumberOfShards(); j++) {
            ShardId shardId = new ShardId(indexMetadata.getIndex(), j);
            IndexShardRoutingTable.Builder indexShardRoutingBuilder = new IndexShardRoutingTable.Builder(shardId);
            indexShardRoutingBuilder.addShard(newShardRouting(shardId, randomAlphaOfLength(10), true, ShardRoutingState.STARTED));
            for (int k = 0; k < indexMetadata.getNumberOfReplicas(); k++) {
                indexShardRoutingBuilder.addShard(newShardRouting(shardId, randomAlphaOfLength(10), false, ShardRoutingState.STARTED));
            }
            indexRoutingTable.addIndexShard(indexShardRoutingBuilder.build());
        }
        clusterStateBuilder.routingTable(RoutingTable.builder(currentState.routingTable()).add(indexRoutingTable).build());

        if (block != null) {
            clusterStateBuilder.blocks(ClusterBlocks.builder().blocks(currentState.blocks()).addIndexBlock(index, block));
        }
        return clusterStateBuilder.build();
    }

    private static void assertIsOpened(final String indexName, final ClusterState clusterState) {
        final IndexMetadata indexMetadata = clusterState.metadata().indices().get(indexName);
        assertThat(indexMetadata.getState(), is(IndexMetadata.State.OPEN));
        assertThat(indexMetadata.getSettings().hasValue(MetadataIndexStateService.VERIFIED_BEFORE_CLOSE_SETTING.getKey()), is(false));
        assertThat(clusterState.routingTable().index(indexName), notNullValue());
        assertThat(clusterState.blocks().hasIndexBlock(indexName, MetadataIndexStateService.INDEX_CLOSED_BLOCK), is(false));
        assertThat(clusterState.routingTable().index(indexName), notNullValue());
    }

    private static void assertIsClosed(final String indexName, final ClusterState clusterState) {
        final IndexMetadata indexMetadata = clusterState.metadata().indices().get(indexName);
        assertThat(indexMetadata.getState(), is(IndexMetadata.State.CLOSE));
        final Settings indexSettings = indexMetadata.getSettings();
        assertThat(indexSettings.hasValue(MetadataIndexStateService.VERIFIED_BEFORE_CLOSE_SETTING.getKey()), is(true));
        assertThat(indexSettings.getAsBoolean(MetadataIndexStateService.VERIFIED_BEFORE_CLOSE_SETTING.getKey(), false), is(true));
        assertThat(clusterState.blocks().hasIndexBlock(indexName, MetadataIndexStateService.INDEX_CLOSED_BLOCK), is(true));
        assertThat("Index " + indexName + " must have only 1 block with [id=" + MetadataIndexStateService.INDEX_CLOSED_BLOCK_ID + "]",
            clusterState.blocks().indices().getOrDefault(indexName, emptySet()).stream()
                .filter(clusterBlock -> clusterBlock.id() == MetadataIndexStateService.INDEX_CLOSED_BLOCK_ID).count(), equalTo(1L));

        final IndexRoutingTable indexRoutingTable = clusterState.routingTable().index(indexName);
        assertThat(indexRoutingTable, notNullValue());

        for(IndexShardRoutingTable shardRoutingTable : indexRoutingTable) {
            assertThat(shardRoutingTable.shards().stream().allMatch(ShardRouting::unassigned), is(true));
            assertThat(shardRoutingTable.shards().stream().map(ShardRouting::unassignedInfo).map(UnassignedInfo::getReason)
                .allMatch(info -> info == UnassignedInfo.Reason.INDEX_CLOSED), is(true));
        }
    }

    private static void assertHasBlock(final String indexName, final ClusterState clusterState, final ClusterBlock closingBlock) {
        assertThat(clusterState.blocks().hasIndexBlock(indexName, closingBlock), is(true));
        assertThat("Index " + indexName + " must have only 1 block with [id=" + MetadataIndexStateService.INDEX_CLOSED_BLOCK_ID + "]",
            clusterState.blocks().indices().getOrDefault(indexName, emptySet()).stream()
                .filter(clusterBlock -> clusterBlock.id() == MetadataIndexStateService.INDEX_CLOSED_BLOCK_ID).count(), equalTo(1L));
    }
}

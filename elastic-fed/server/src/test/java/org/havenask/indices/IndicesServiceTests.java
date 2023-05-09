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

package org.havenask.indices;

import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.AlreadyClosedException;
import org.havenask.LegacyESVersion;
import org.havenask.Version;
import org.havenask.action.ActionListener;
import org.havenask.action.admin.indices.stats.CommonStatsFlags;
import org.havenask.action.admin.indices.stats.IndexShardStats;
import org.havenask.cluster.ClusterName;
import org.havenask.cluster.ClusterState;
import org.havenask.cluster.metadata.IndexGraveyard;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.cluster.metadata.Metadata;
import org.havenask.cluster.service.ClusterService;
import org.havenask.common.UUIDs;
import org.havenask.common.io.FileSystemUtils;
import org.havenask.common.settings.Setting;
import org.havenask.common.settings.Settings;
import org.havenask.common.unit.TimeValue;
import org.havenask.env.NodeEnvironment;
import org.havenask.env.ShardLockObtainFailedException;
import org.havenask.gateway.GatewayMetaState;
import org.havenask.gateway.LocalAllocateDangledIndices;
import org.havenask.gateway.MetaStateService;
import org.havenask.index.Index;
import org.havenask.index.IndexModule;
import org.havenask.index.IndexService;
import org.havenask.index.IndexSettings;
import org.havenask.index.engine.Engine;
import org.havenask.index.engine.EngineConfig;
import org.havenask.index.engine.EngineFactory;
import org.havenask.index.engine.InternalEngine;
import org.havenask.index.engine.InternalEngineFactory;
import org.havenask.index.mapper.KeywordFieldMapper;
import org.havenask.index.mapper.Mapper;
import org.havenask.index.mapper.MapperService;
import org.havenask.index.shard.IllegalIndexShardStateException;
import org.havenask.index.shard.IndexShard;
import org.havenask.index.shard.IndexShardState;
import org.havenask.index.shard.ShardId;
import org.havenask.index.shard.ShardPath;
import org.havenask.index.similarity.NonNegativeScoresSimilarity;
import org.havenask.indices.IndicesService.ShardDeletionCheckResult;
import org.havenask.plugins.EnginePlugin;
import org.havenask.plugins.MapperPlugin;
import org.havenask.plugins.Plugin;
import org.havenask.test.HavenaskSingleNodeTestCase;
import org.havenask.test.IndexSettingsModule;
import org.havenask.test.VersionUtils;
import org.havenask.test.hamcrest.RegexMatcher;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.havenask.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.havenask.test.hamcrest.HavenaskAssertions.assertAcked;
import static org.havenask.test.hamcrest.HavenaskAssertions.assertHitCount;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IndicesServiceTests extends HavenaskSingleNodeTestCase {

    public IndicesService getIndicesService() {
        return getInstanceFromNode(IndicesService.class);
    }

    public NodeEnvironment getNodeEnvironment() {
        return getInstanceFromNode(NodeEnvironment.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Stream.concat(
                super.getPlugins().stream(),
                Stream.of(TestPlugin.class, FooEnginePlugin.class, BarEnginePlugin.class))
                .collect(Collectors.toList());
    }

    public static class FooEnginePlugin extends Plugin implements EnginePlugin {

        static class FooEngineFactory implements EngineFactory {

            @Override
            public Engine newReadWriteEngine(final EngineConfig config) {
                return new InternalEngine(config);
            }

        }

        private static final Setting<Boolean> FOO_INDEX_SETTING =
                Setting.boolSetting("index.foo_index", false, Setting.Property.IndexScope);

        @Override
        public List<Setting<?>> getSettings() {
            return Collections.singletonList(FOO_INDEX_SETTING);
        }

        @Override
        public Optional<EngineFactory> getEngineFactory(final IndexSettings indexSettings) {
            if (FOO_INDEX_SETTING.get(indexSettings.getSettings())) {
                return Optional.of(new FooEngineFactory());
            } else {
                return Optional.empty();
            }
        }

    }

    public static class BarEnginePlugin extends Plugin implements EnginePlugin {

        static class BarEngineFactory implements EngineFactory {

            @Override
            public Engine newReadWriteEngine(final EngineConfig config) {
                return new InternalEngine(config);
            }

        }

        private static final Setting<Boolean> BAR_INDEX_SETTING =
                Setting.boolSetting("index.bar_index", false, Setting.Property.IndexScope);

        @Override
        public List<Setting<?>> getSettings() {
            return Collections.singletonList(BAR_INDEX_SETTING);
        }

        @Override
        public Optional<EngineFactory> getEngineFactory(final IndexSettings indexSettings) {
            if (BAR_INDEX_SETTING.get(indexSettings.getSettings())) {
                return Optional.of(new BarEngineFactory());
            } else {
                return Optional.empty();
            }
        }

    }

    public static class TestPlugin extends Plugin implements MapperPlugin {

        public TestPlugin() {}

        @Override
        public Map<String, Mapper.TypeParser> getMappers() {
            return Collections.singletonMap("fake-mapper", KeywordFieldMapper.PARSER);
        }

        @Override
        public void onIndexModule(IndexModule indexModule) {
            super.onIndexModule(indexModule);
            indexModule.addSimilarity("fake-similarity",
                    (settings, indexCreatedVersion, scriptService) -> new BM25Similarity());
        }
    }

    @Override
    protected boolean resetNodeAfterTest() {
        return true;
    }

    public void testCanDeleteShardContent() {
        IndicesService indicesService = getIndicesService();
        IndexMetadata meta = IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(
                1).build();
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test", meta.getSettings());
        ShardId shardId = new ShardId(meta.getIndex(), 0);
        assertEquals("no shard location", indicesService.canDeleteShardContent(shardId, indexSettings),
            ShardDeletionCheckResult.NO_FOLDER_FOUND);
        IndexService test = createIndex("test");
        shardId = new ShardId(test.index(), 0);
        assertTrue(test.hasShard(0));
        assertEquals("shard is allocated", indicesService.canDeleteShardContent(shardId, test.getIndexSettings()),
            ShardDeletionCheckResult.STILL_ALLOCATED);
        test.removeShard(0, "boom");
        assertEquals("shard is removed", indicesService.canDeleteShardContent(shardId, test.getIndexSettings()),
            ShardDeletionCheckResult.FOLDER_FOUND_CAN_DELETE);
        ShardId notAllocated = new ShardId(test.index(), 100);
        assertEquals("shard that was never on this node should NOT be deletable",
            indicesService.canDeleteShardContent(notAllocated, test.getIndexSettings()), ShardDeletionCheckResult.NO_FOLDER_FOUND);
    }

    public void testDeleteIndexStore() throws Exception {
        IndicesService indicesService = getIndicesService();
        IndexService test = createIndex("test");
        ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        IndexMetadata firstMetadata = clusterService.state().metadata().index("test");
        assertTrue(test.hasShard(0));
        ShardPath firstPath = ShardPath.loadShardPath(logger, getNodeEnvironment(), new ShardId(test.index(), 0),
            test.getIndexSettings().customDataPath());

        expectThrows(IllegalStateException.class, () -> indicesService.deleteIndexStore("boom", firstMetadata));
        assertTrue(firstPath.exists());

        GatewayMetaState gwMetaState = getInstanceFromNode(GatewayMetaState.class);
        Metadata meta = gwMetaState.getMetadata();
        assertNotNull(meta);
        assertNotNull(meta.index("test"));
        assertAcked(client().admin().indices().prepareDelete("test"));

        assertFalse(firstPath.exists());

        meta = gwMetaState.getMetadata();
        assertNotNull(meta);
        assertNull(meta.index("test"));

        test = createIndex("test");
        client().prepareIndex("test", "type", "1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();
        client().admin().indices().prepareFlush("test").get();
        assertHitCount(client().prepareSearch("test").get(), 1);
        IndexMetadata secondMetadata = clusterService.state().metadata().index("test");
        assertAcked(client().admin().indices().prepareClose("test").setWaitForActiveShards(1));
        ShardPath secondPath = ShardPath.loadShardPath(logger, getNodeEnvironment(), new ShardId(test.index(), 0),
            test.getIndexSettings().customDataPath());
        assertTrue(secondPath.exists());

        expectThrows(IllegalStateException.class, () -> indicesService.deleteIndexStore("boom", secondMetadata));
        assertTrue(secondPath.exists());

        assertAcked(client().admin().indices().prepareOpen("test"));
        ensureGreen("test");
    }

    public void testPendingTasks() throws Exception {
        final IndexService indexService = createIndex("test");
        final Index index = indexService.index();
        final IndexSettings indexSettings = indexService.getIndexSettings();

        final IndexShard indexShard = indexService.getShardOrNull(0);
        assertNotNull(indexShard);
        assertTrue(indexShard.routingEntry().started());

        final ShardPath shardPath = indexShard.shardPath();
        assertEquals(ShardPath.loadShardPath(logger, getNodeEnvironment(), indexShard.shardId(), indexSettings.customDataPath()),
            shardPath);

        final IndicesService indicesService = getIndicesService();
        expectThrows(ShardLockObtainFailedException.class, () ->
            indicesService.processPendingDeletes(index, indexSettings, TimeValue.timeValueMillis(0)));
        assertTrue(shardPath.exists());

        int numPending = 1;
        if (randomBoolean()) {
            indicesService.addPendingDelete(indexShard.shardId(), indexSettings);
        } else {
            if (randomBoolean()) {
                numPending++;
                indicesService.addPendingDelete(indexShard.shardId(), indexSettings);
            }
            indicesService.addPendingDelete(index, indexSettings);
        }

        assertAcked(client().admin().indices().prepareClose("test"));
        assertTrue(shardPath.exists());
        ensureGreen("test");

        assertEquals(indicesService.numPendingDeletes(index), numPending);
        assertTrue(indicesService.hasUncompletedPendingDeletes());

        expectThrows(ShardLockObtainFailedException.class, () ->
            indicesService.processPendingDeletes(index, indexSettings, TimeValue.timeValueMillis(0)));

        assertEquals(indicesService.numPendingDeletes(index), numPending);
        assertTrue(indicesService.hasUncompletedPendingDeletes());

        final boolean hasBogus = randomBoolean();
        if (hasBogus) {
            indicesService.addPendingDelete(new ShardId(index, 0), indexSettings);
            indicesService.addPendingDelete(new ShardId(index, 1), indexSettings);
            indicesService.addPendingDelete(new ShardId("bogus", "_na_", 1), indexSettings);
            assertEquals(indicesService.numPendingDeletes(index), numPending + 2);
            assertTrue(indicesService.hasUncompletedPendingDeletes());
        }

        assertAcked(client().admin().indices().prepareDelete("test"));
        assertBusy(() -> {
            try {
                indicesService.processPendingDeletes(index, indexSettings, TimeValue.timeValueMillis(0));
                assertEquals(indicesService.numPendingDeletes(index), 0);
            } catch (final Exception e) {
                fail(e.getMessage());
            }
        });
        assertBusy(() -> {
            assertThat(indicesService.hasUncompletedPendingDeletes(), equalTo(hasBogus)); // "bogus" index has not been removed
            assertFalse(shardPath.exists());
        });
    }

    public void testVerifyIfIndexContentDeleted() throws Exception {
        final Index index = new Index("test", UUIDs.randomBase64UUID());
        final IndicesService indicesService = getIndicesService();
        final NodeEnvironment nodeEnv = getNodeEnvironment();
        final MetaStateService metaStateService = getInstanceFromNode(MetaStateService.class);

        final ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        final Settings idxSettings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                                                        .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID())
                                                        .build();
        final IndexMetadata indexMetadata = new IndexMetadata.Builder(index.getName())
                                                             .settings(idxSettings)
                                                             .numberOfShards(1)
                                                             .numberOfReplicas(0)
                                                             .build();
        metaStateService.writeIndex("test index being created", indexMetadata);
        final Metadata metadata = Metadata.builder(clusterService.state().metadata()).put(indexMetadata, true).build();
        final ClusterState csWithIndex = new ClusterState.Builder(clusterService.state()).metadata(metadata).build();
        try {
            indicesService.verifyIndexIsDeleted(index, csWithIndex);
            fail("Should not be able to delete index contents when the index is part of the cluster state.");
        } catch (IllegalStateException e) {
            assertThat(e.getMessage(), containsString("Cannot delete index"));
        }

        final ClusterState withoutIndex = new ClusterState.Builder(csWithIndex)
                                                          .metadata(Metadata.builder(csWithIndex.metadata()).remove(index.getName()))
                                                          .build();
        indicesService.verifyIndexIsDeleted(index, withoutIndex);
        assertFalse("index files should be deleted", FileSystemUtils.exists(nodeEnv.indexPaths(index)));
    }

    public void testDanglingIndicesWithAliasConflict() throws Exception {
        final String indexName = "test-idx1";
        final String alias = "test-alias";
        final ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        createIndex(indexName);

        // create the alias for the index
        client().admin().indices().prepareAliases().addAlias(indexName, alias).get();
        final ClusterState originalState = clusterService.state();

        // try to import a dangling index with the same name as the alias, it should fail
        final LocalAllocateDangledIndices dangling = getInstanceFromNode(LocalAllocateDangledIndices.class);
        final Settings idxSettings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                                                       .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
                                                       .build();
        final IndexMetadata indexMetadata = new IndexMetadata.Builder(alias)
                                                             .settings(idxSettings)
                                                             .numberOfShards(1)
                                                             .numberOfReplicas(0)
                                                             .build();
        CountDownLatch latch = new CountDownLatch(1);
        dangling.allocateDangled(Arrays.asList(indexMetadata), ActionListener.wrap(latch::countDown));
        latch.await();
        assertThat(clusterService.state(), equalTo(originalState));

        // remove the alias
        client().admin().indices().prepareAliases().removeAlias(indexName, alias).get();

        // now try importing a dangling index with the same name as the alias, it should succeed.
        latch = new CountDownLatch(1);
        dangling.allocateDangled(Arrays.asList(indexMetadata), ActionListener.wrap(latch::countDown));
        latch.await();
        assertThat(clusterService.state(), not(originalState));
        assertNotNull(clusterService.state().getMetadata().index(alias));
    }

    public void testDanglingIndicesWithLaterVersion() throws Exception {
        final String indexNameLater = "test-idxnewer";
        final ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        final ClusterState originalState = clusterService.state();

        //import an index with minor version incremented by one over cluster master version, it should be ignored
        final LocalAllocateDangledIndices dangling = getInstanceFromNode(LocalAllocateDangledIndices.class);
        final Settings idxSettingsLater = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED,
                                                                Version.fromId(Version.CURRENT.id + 10000))
                                                            .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
                                                            .build();
        final IndexMetadata indexMetadataLater = new IndexMetadata.Builder(indexNameLater)
                                                             .settings(idxSettingsLater)
                                                             .numberOfShards(1)
                                                             .numberOfReplicas(0)
                                                             .build();
        CountDownLatch latch = new CountDownLatch(1);
        dangling.allocateDangled(Arrays.asList(indexMetadataLater), ActionListener.wrap(latch::countDown));
        latch.await();
        assertThat(clusterService.state(), equalTo(originalState));
    }

    /**
     * This test checks an edge case where, if a node had an index (lets call it A with UUID 1), then
     * deleted it (so a tombstone entry for A will exist in the cluster state), then created
     * a new index A with UUID 2, then shutdown, when the node comes back online, it will look at the
     * tombstones for deletions, and it should proceed with trying to delete A with UUID 1 and not
     * throw any errors that the index still exists in the cluster state.  This is a case of ensuring
     * that tombstones that have the same name as current valid indices don't cause confusion by
     * trying to delete an index that exists.
     * See https://github.com/elastic/elasticsearch/issues/18054
     */
    public void testIndexAndTombstoneWithSameNameOnStartup() throws Exception {
        final String indexName = "test";
        final Index index = new Index(indexName, UUIDs.randomBase64UUID());
        final IndicesService indicesService = getIndicesService();
        final Settings idxSettings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                                         .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID())
                                         .build();
        final IndexMetadata indexMetadata = new IndexMetadata.Builder(index.getName())
                                                .settings(idxSettings)
                                                .numberOfShards(1)
                                                .numberOfReplicas(0)
                                                .build();
        final Index tombstonedIndex = new Index(indexName, UUIDs.randomBase64UUID());
        final IndexGraveyard graveyard = IndexGraveyard.builder().addTombstone(tombstonedIndex).build();
        final Metadata metadata = Metadata.builder().put(indexMetadata, true).indexGraveyard(graveyard).build();
        final ClusterState clusterState = new ClusterState.Builder(new ClusterName("testCluster")).metadata(metadata).build();
        // if all goes well, this won't throw an exception, otherwise, it will throw an IllegalStateException
        indicesService.verifyIndexIsDeleted(tombstonedIndex, clusterState);
    }

    /**
     * Tests that teh {@link MapperService} created by {@link IndicesService#createIndexMapperService(IndexMetadata)} contains
     * custom types and similarities registered by plugins
     */
    public void testStandAloneMapperServiceWithPlugins() throws IOException {
        final String indexName = "test";
        final Index index = new Index(indexName, UUIDs.randomBase64UUID());
        final IndicesService indicesService = getIndicesService();
        final Settings idxSettings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID())
            .put(IndexModule.SIMILARITY_SETTINGS_PREFIX + ".test.type", "fake-similarity")
            .build();
        final IndexMetadata indexMetadata = new IndexMetadata.Builder(index.getName())
            .settings(idxSettings)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        MapperService mapperService = indicesService.createIndexMapperService(indexMetadata);
        assertNotNull(mapperService.documentMapperParser().parserContext().typeParser("fake-mapper"));
        Similarity sim = mapperService.documentMapperParser().parserContext().getSimilarity("test").get();
        assertThat(sim, instanceOf(NonNegativeScoresSimilarity.class));
        sim = ((NonNegativeScoresSimilarity) sim).getDelegate();
        assertThat(sim, instanceOf(BM25Similarity.class));
    }

    public void testStatsByShardDoesNotDieFromExpectedExceptions() {
        final int shardCount = randomIntBetween(2, 5);
        final int failedShardId = randomIntBetween(0, shardCount - 1);

        final Index index = new Index("test-index", "abc123");
        // the shard that is going to fail
        final ShardId shardId = new ShardId(index, failedShardId);

        final List<IndexShard> shards = new ArrayList<>(shardCount);
        final List<IndexShardStats> shardStats = new ArrayList<>(shardCount - 1);

        final IndexShardState state = randomFrom(IndexShardState.values());
        final String message = "TEST - expected";

        final RuntimeException expectedException =
                randomFrom(new IllegalIndexShardStateException(shardId, state, message), new AlreadyClosedException(message));

        // this allows us to control the indices that exist
        final IndicesService mockIndicesService = mock(IndicesService.class);
        final IndexService indexService = mock(IndexService.class);

        // generate fake shards and their responses
        for (int i = 0; i < shardCount; ++i) {
            final IndexShard shard = mock(IndexShard.class);

            shards.add(shard);

            if (failedShardId != i) {
                final IndexShardStats successfulShardStats = mock(IndexShardStats.class);

                shardStats.add(successfulShardStats);

                when(mockIndicesService.indexShardStats(mockIndicesService, shard, CommonStatsFlags.ALL)).thenReturn(successfulShardStats);
            } else {
                when(mockIndicesService.indexShardStats(mockIndicesService, shard, CommonStatsFlags.ALL)).thenThrow(expectedException);
            }
        }

        when(mockIndicesService.iterator()).thenReturn(Collections.singleton(indexService).iterator());
        when(indexService.iterator()).thenReturn(shards.iterator());
        when(indexService.index()).thenReturn(index);

        // real one, which has a logger defined
        final IndicesService indicesService = getIndicesService();

        final Map<Index, List<IndexShardStats>> indexStats = indicesService.statsByShard(mockIndicesService, CommonStatsFlags.ALL);

        assertThat(indexStats.isEmpty(), equalTo(false));
        assertThat("index not defined", indexStats.containsKey(index), equalTo(true));
        assertThat("unexpected shard stats", indexStats.get(index), equalTo(shardStats));
    }

    public void testIsMetadataField() {
        IndicesService indicesService = getIndicesService();
        final Version randVersion = VersionUtils.randomVersionBetween(random(), LegacyESVersion.V_6_0_0, Version.CURRENT);
        assertFalse(indicesService.isMetadataField(randVersion, randomAlphaOfLengthBetween(10, 15)));
        for (String builtIn : IndicesModule.getBuiltInMetadataFields()) {
            assertTrue(indicesService.isMetadataField(randVersion, builtIn));
        }
    }

    public void testGetEngineFactory() throws IOException {
        final IndicesService indicesService = getIndicesService();

        final Boolean[] values = new Boolean[] { true, false, null };
        for (final Boolean value : values) {
            final String indexName = "foo-" + value;
            final Index index = new Index(indexName, UUIDs.randomBase64UUID());
            final Settings.Builder builder = Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID());
            if (value != null) {
                builder.put(FooEnginePlugin.FOO_INDEX_SETTING.getKey(), value);
            }

            final IndexMetadata indexMetadata = new IndexMetadata.Builder(index.getName())
                    .settings(builder.build())
                    .numberOfShards(1)
                    .numberOfReplicas(0)
                    .build();
            final IndexService indexService = indicesService.createIndex(indexMetadata, Collections.emptyList(), false);
            if (value != null && value) {
                assertThat(indexService.getEngineFactory(), instanceOf(FooEnginePlugin.FooEngineFactory.class));
            } else {
                assertThat(indexService.getEngineFactory(), instanceOf(InternalEngineFactory.class));
            }
        }
    }

    public void testConflictingEngineFactories() {
        final String indexName = "foobar";
        final Index index = new Index(indexName, UUIDs.randomBase64UUID());
        final Settings settings = Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID())
                .put(FooEnginePlugin.FOO_INDEX_SETTING.getKey(), true)
                .put(BarEnginePlugin.BAR_INDEX_SETTING.getKey(), true)
                .build();
        final IndexMetadata indexMetadata = new IndexMetadata.Builder(index.getName())
                .settings(settings)
                .numberOfShards(1)
                .numberOfReplicas(0)
                .build();

        final IndicesService indicesService = getIndicesService();
        final IllegalStateException e =
                expectThrows(IllegalStateException.class, () -> indicesService.createIndex(indexMetadata, Collections.emptyList(), false));
        final String pattern =
                ".*multiple engine factories provided for \\[foobar/.*\\]: \\[.*FooEngineFactory\\],\\[.*BarEngineFactory\\].*";
        assertThat(e, hasToString(new RegexMatcher(pattern)));
    }
}

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

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.hamcrest.Matchers;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.common.xcontent.XContentHelper;
import org.havenask.common.xcontent.XContentType;
import org.havenask.index.IndexService;
import org.havenask.index.shard.IndexMappingProvider;
import org.havenask.test.HavenaskSingleNodeTestCase;
import org.junit.Before;
import org.havenask.ExceptionsHelper;
import org.havenask.LegacyESVersion;
import org.havenask.ResourceAlreadyExistsException;
import org.havenask.Version;
import org.havenask.action.admin.indices.alias.Alias;
import org.havenask.action.admin.indices.create.CreateIndexClusterStateUpdateRequest;
import org.havenask.action.admin.indices.shrink.ResizeType;
import org.havenask.cluster.ClusterName;
import org.havenask.cluster.ClusterState;
import org.havenask.cluster.EmptyClusterInfoService;
import org.havenask.cluster.HavenaskAllocationTestCase;
import org.havenask.cluster.block.ClusterBlocks;
import org.havenask.cluster.node.DiscoveryNode;
import org.havenask.cluster.node.DiscoveryNodeRole;
import org.havenask.cluster.node.DiscoveryNodes;
import org.havenask.cluster.routing.RoutingTable;
import org.havenask.cluster.routing.allocation.AllocationService;
import org.havenask.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.havenask.cluster.routing.allocation.decider.AllocationDeciders;
import org.havenask.cluster.routing.allocation.decider.MaxRetryAllocationDecider;
import org.havenask.cluster.service.ClusterService;
import org.havenask.common.Strings;
import org.havenask.common.collect.ImmutableOpenMap;
import org.havenask.common.compress.CompressedXContent;
import org.havenask.common.settings.IndexScopedSettings;
import org.havenask.common.settings.Setting;
import org.havenask.common.settings.Settings;
import org.havenask.common.unit.TimeValue;
import org.havenask.common.util.BigArrays;
import org.havenask.common.xcontent.NamedXContentRegistry;
import org.havenask.common.xcontent.XContentFactory;
import org.havenask.index.Index;
import org.havenask.index.IndexNotFoundException;
import org.havenask.index.IndexSettings;
import org.havenask.index.mapper.MapperService;
import org.havenask.index.query.QueryShardContext;
import org.havenask.indices.InvalidAliasNameException;
import org.havenask.indices.InvalidIndexNameException;
import org.havenask.indices.ShardLimitValidator;
import org.havenask.indices.SystemIndexDescriptor;
import org.havenask.indices.SystemIndices;
import org.havenask.snapshots.EmptySnapshotsInfoService;
import org.havenask.test.ClusterServiceUtils;
import org.havenask.test.VersionUtils;
import org.havenask.test.gateway.TestGatewayAllocator;
import org.havenask.threadpool.TestThreadPool;
import org.havenask.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.havenask.index.IndexSettings.INDEX_SOFT_DELETES_SETTING;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasValue;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.havenask.cluster.metadata.IndexMetadata.INDEX_NUMBER_OF_ROUTING_SHARDS_SETTING;
import static org.havenask.cluster.metadata.IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING;
import static org.havenask.cluster.metadata.IndexMetadata.INDEX_READ_ONLY_BLOCK;
import static org.havenask.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.havenask.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.havenask.cluster.metadata.IndexMetadata.SETTING_READ_ONLY;
import static org.havenask.cluster.metadata.IndexMetadata.SETTING_VERSION_CREATED;
import static org.havenask.cluster.metadata.MetadataCreateIndexService.aggregateIndexSettings;
import static org.havenask.cluster.metadata.MetadataCreateIndexService.buildIndexMetadata;
import static org.havenask.cluster.metadata.MetadataCreateIndexService.clusterStateCreateIndex;
import static org.havenask.cluster.metadata.MetadataCreateIndexService.getIndexNumberOfRoutingShards;
import static org.havenask.cluster.metadata.MetadataCreateIndexService.parseV1Mappings;
import static org.havenask.cluster.metadata.MetadataCreateIndexService.resolveAndValidateAliases;
import static org.havenask.indices.ShardLimitValidatorTests.createTestShardLimitService;

public class MetadataCreateIndexServiceTests extends HavenaskSingleNodeTestCase {

    private AliasValidator aliasValidator;
    private CreateIndexClusterStateUpdateRequest request;
    private QueryShardContext queryShardContext;

    @Before
    public void setupCreateIndexRequestAndAliasValidator() {
        aliasValidator = new AliasValidator();
        request = new CreateIndexClusterStateUpdateRequest("create index", "test", "test");
        Settings indexSettings = Settings.builder().put(SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1).build();
        queryShardContext = new QueryShardContext(0,
            new IndexSettings(IndexMetadata.builder("test").settings(indexSettings).build(), indexSettings),
            BigArrays.NON_RECYCLING_INSTANCE, null, null, null, null, null, xContentRegistry(), writableRegistry(),
            null, null, () -> randomNonNegativeLong(), null, null, () -> true, null);
    }

    private ClusterState createClusterState(String name, int numShards, int numReplicas, Settings settings) {
        int numRoutingShards = settings.getAsInt(IndexMetadata.INDEX_NUMBER_OF_ROUTING_SHARDS_SETTING.getKey(), numShards);
        Metadata.Builder metaBuilder = Metadata.builder();
        IndexMetadata indexMetadata = IndexMetadata.builder(name).settings(settings(Version.CURRENT)
            .put(settings))
            .numberOfShards(numShards).numberOfReplicas(numReplicas)
            .setRoutingNumShards(numRoutingShards).build();
        metaBuilder.put(indexMetadata, false);
        Metadata metadata = metaBuilder.build();
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        routingTableBuilder.addAsNew(metadata.index(name));

        RoutingTable routingTable = routingTableBuilder.build();
        ClusterState clusterState = ClusterState.builder(org.havenask.cluster.ClusterName.CLUSTER_NAME_SETTING
            .getDefault(Settings.EMPTY))
            .metadata(metadata).routingTable(routingTable).blocks(ClusterBlocks.builder().addBlocks(indexMetadata)).build();
        return clusterState;
    }

    public static boolean isShrinkable(int source, int target) {
        int x = source / target;
        assert source > target : source  + " <= " + target;
        return target * x == source;
    }

    public static boolean isSplitable(int source, int target) {
        int x = target / source;
        assert source < target : source  + " >= " + target;
        return source * x == target;
    }

    public void testNumberOfShards() {
        {
            final Version versionCreated = VersionUtils.randomVersionBetween(
                random(),
                LegacyESVersion.V_6_0_0_alpha1, VersionUtils.getPreviousVersion(LegacyESVersion.V_7_0_0));
            final Settings.Builder indexSettingsBuilder = Settings.builder().put(SETTING_VERSION_CREATED, versionCreated);
            assertThat(MetadataCreateIndexService.getNumberOfShards(indexSettingsBuilder), equalTo(5));
        }
        {
            final Version versionCreated = VersionUtils.randomVersionBetween(random(), LegacyESVersion.V_7_0_0, Version.CURRENT);
            final Settings.Builder indexSettingsBuilder = Settings.builder().put(SETTING_VERSION_CREATED, versionCreated);
            assertThat(MetadataCreateIndexService.getNumberOfShards(indexSettingsBuilder), equalTo(1));
        }
    }

    public void testValidateShrinkIndex() {
        int numShards = randomIntBetween(2, 42);
        ClusterState state = createClusterState("source", numShards, randomIntBetween(0, 10),
            Settings.builder().put("index.blocks.write", true).build());

        assertEquals("index [source] already exists",
            expectThrows(ResourceAlreadyExistsException.class, () ->
                MetadataCreateIndexService.validateShrinkIndex(state, "target", "source", Settings.EMPTY)
            ).getMessage());

        assertEquals("no such index [no_such_index]",
            expectThrows(IndexNotFoundException.class, () ->
                MetadataCreateIndexService.validateShrinkIndex(state, "no_such_index", "target", Settings.EMPTY)
            ).getMessage());

        Settings targetSettings = Settings.builder().put("index.number_of_shards", 1).build();
        assertEquals("can't shrink an index with only one shard",
            expectThrows(IllegalArgumentException.class, () -> MetadataCreateIndexService.validateShrinkIndex(createClusterState("source",
                1, 0, Settings.builder().put("index.blocks.write", true).build()), "source", "target", targetSettings)).getMessage());

        assertEquals("the number of target shards [10] must be less that the number of source shards [5]",
            expectThrows(IllegalArgumentException.class, () -> MetadataCreateIndexService.validateShrinkIndex(createClusterState("source",
                5, 0, Settings.builder().put("index.blocks.write", true).build()), "source",
                "target", Settings.builder().put("index.number_of_shards", 10).build())).getMessage());


        assertEquals("index source must be read-only to resize index. use \"index.blocks.write=true\"",
            expectThrows(IllegalStateException.class, () ->
                    MetadataCreateIndexService.validateShrinkIndex(
                        createClusterState("source", randomIntBetween(2, 100), randomIntBetween(0, 10), Settings.EMPTY),
                        "source", "target", targetSettings)
            ).getMessage());

        assertEquals("index source must have all shards allocated on the same node to shrink index",
            expectThrows(IllegalStateException.class, () ->
                MetadataCreateIndexService.validateShrinkIndex(state, "source", "target", targetSettings)

            ).getMessage());
        assertEquals("the number of source shards [8] must be a multiple of [3]",
            expectThrows(IllegalArgumentException.class, () ->
                    MetadataCreateIndexService.validateShrinkIndex(createClusterState("source", 8, randomIntBetween(0, 10),
                        Settings.builder().put("index.blocks.write", true).build()), "source", "target",
                        Settings.builder().put("index.number_of_shards", 3).build())
            ).getMessage());

        // create one that won't fail
        ClusterState clusterState = ClusterState.builder(createClusterState("source", numShards, 0,
            Settings.builder().put("index.blocks.write", true).build())).nodes(DiscoveryNodes.builder().add(newNode("node1")))
            .build();
        AllocationService service = new AllocationService(new AllocationDeciders(
            singleton(new MaxRetryAllocationDecider())),
            new TestGatewayAllocator(), new BalancedShardsAllocator(Settings.EMPTY), EmptyClusterInfoService.INSTANCE,
            EmptySnapshotsInfoService.INSTANCE);

        RoutingTable routingTable = service.reroute(clusterState, "reroute").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        // now we start the shard
        routingTable = HavenaskAllocationTestCase.startInitializingShardsAndReroute(service, clusterState, "source").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        int targetShards;
        do {
            targetShards = randomIntBetween(1, numShards/2);
        } while (isShrinkable(numShards, targetShards) == false);
        MetadataCreateIndexService.validateShrinkIndex(clusterState, "source", "target",
            Settings.builder().put("index.number_of_shards", targetShards).build());
    }

    public void testValidateSplitIndex() {
        int numShards = randomIntBetween(1, 42);
        Settings targetSettings = Settings.builder().put("index.number_of_shards", numShards * 2).build();
        ClusterState state = createClusterState("source", numShards, randomIntBetween(0, 10),
            Settings.builder().put("index.blocks.write", true).build());

        assertEquals("index [source] already exists",
            expectThrows(ResourceAlreadyExistsException.class, () ->
                MetadataCreateIndexService.validateSplitIndex(state, "target", "source", targetSettings)
            ).getMessage());

        assertEquals("no such index [no_such_index]",
            expectThrows(IndexNotFoundException.class, () ->
                MetadataCreateIndexService.validateSplitIndex(state, "no_such_index", "target", targetSettings)
            ).getMessage());

        assertEquals("the number of source shards [10] must be less that the number of target shards [5]",
            expectThrows(IllegalArgumentException.class, () -> MetadataCreateIndexService.validateSplitIndex(createClusterState("source",
                10, 0, Settings.builder().put("index.blocks.write", true).build()), "source",
                "target", Settings.builder().put("index.number_of_shards", 5).build())
            ).getMessage());


        assertEquals("index source must be read-only to resize index. use \"index.blocks.write=true\"",
            expectThrows(IllegalStateException.class, () ->
                MetadataCreateIndexService.validateSplitIndex(
                    createClusterState("source", randomIntBetween(2, 100), randomIntBetween(0, 10), Settings.EMPTY)
                    , "source", "target", targetSettings)
            ).getMessage());


        assertEquals("the number of source shards [3] must be a factor of [4]",
            expectThrows(IllegalArgumentException.class, () ->
                MetadataCreateIndexService.validateSplitIndex(createClusterState("source", 3, randomIntBetween(0, 10),
                    Settings.builder().put("index.blocks.write", true).build()), "source", "target",
                    Settings.builder().put("index.number_of_shards", 4).build())
            ).getMessage());

        int targetShards;
        do {
            targetShards = randomIntBetween(numShards+1, 100);
        } while (isSplitable(numShards, targetShards) == false);
        ClusterState clusterState = ClusterState.builder(createClusterState("source", numShards, 0,
            Settings.builder().put("index.blocks.write", true).put("index.number_of_routing_shards", targetShards).build()))
            .nodes(DiscoveryNodes.builder().add(newNode("node1"))).build();
        AllocationService service = new AllocationService(new AllocationDeciders(
            singleton(new MaxRetryAllocationDecider())),
            new TestGatewayAllocator(), new BalancedShardsAllocator(Settings.EMPTY), EmptyClusterInfoService.INSTANCE,
            EmptySnapshotsInfoService.INSTANCE);

        RoutingTable routingTable = service.reroute(clusterState, "reroute").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        // now we start the shard
        routingTable = HavenaskAllocationTestCase.startInitializingShardsAndReroute(service, clusterState, "source").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        MetadataCreateIndexService.validateSplitIndex(clusterState, "source", "target",
            Settings.builder().put("index.number_of_shards", targetShards).build());
    }

    public void testPrepareResizeIndexSettings() {
        final List<Version> versions = Arrays.asList(VersionUtils.randomVersion(random()), VersionUtils.randomVersion(random()));
        versions.sort(Comparator.comparingLong(l -> l.id));
        final Version version = versions.get(0);
        final Version upgraded = versions.get(1);
        final Settings.Builder indexSettingsBuilder =
                Settings.builder()
                        .put("index.version.created", version)
                        .put("index.version.upgraded", upgraded)
                        .put("index.similarity.default.type", "BM25")
                        .put("index.analysis.analyzer.default.tokenizer", "keyword")
                        .put("index.soft_deletes.enabled", "true");
        if (randomBoolean()) {
            indexSettingsBuilder.put("index.allocation.max_retries", randomIntBetween(1, 1000));
        }
        runPrepareResizeIndexSettingsTest(
                indexSettingsBuilder.build(),
                Settings.EMPTY,
                emptyList(),
                randomBoolean(),
                settings -> {
                    assertThat("similarity settings must be copied", settings.get("index.similarity.default.type"), equalTo("BM25"));
                    assertThat(
                            "analysis settings must be copied",
                            settings.get("index.analysis.analyzer.default.tokenizer"),
                            equalTo("keyword"));
                    assertThat(settings.get("index.routing.allocation.initial_recovery._id"), equalTo("node1"));
                    assertThat(settings.get("index.allocation.max_retries"), nullValue());
                    assertThat(settings.getAsVersion("index.version.created", null), equalTo(version));
                    assertThat(settings.getAsVersion("index.version.upgraded", null), equalTo(upgraded));
                    assertThat(settings.get("index.soft_deletes.enabled"), equalTo("true"));
                });
    }

    public void testPrepareResizeIndexSettingsCopySettings() {
        final int maxMergeCount = randomIntBetween(1, 16);
        final int maxThreadCount = randomIntBetween(1, 16);
        final Setting<String> nonCopyableExistingIndexSetting =
                Setting.simpleString("index.non_copyable.existing", Setting.Property.IndexScope, Setting.Property.NotCopyableOnResize);
        final Setting<String> nonCopyableRequestIndexSetting =
                Setting.simpleString("index.non_copyable.request", Setting.Property.IndexScope, Setting.Property.NotCopyableOnResize);
        runPrepareResizeIndexSettingsTest(
                Settings.builder()
                        .put("index.merge.scheduler.max_merge_count", maxMergeCount)
                        .put("index.non_copyable.existing", "existing")
                        .build(),
                Settings.builder()
                        .put("index.blocks.write", (String) null)
                        .put("index.merge.scheduler.max_thread_count", maxThreadCount)
                        .put("index.non_copyable.request", "request")
                        .build(),
                Arrays.asList(nonCopyableExistingIndexSetting, nonCopyableRequestIndexSetting),
                true,
                settings -> {
                    assertNull(settings.getAsBoolean("index.blocks.write", null));
                    assertThat(settings.get("index.routing.allocation.require._name"), equalTo("node1"));
                    assertThat(settings.getAsInt("index.merge.scheduler.max_merge_count", null), equalTo(maxMergeCount));
                    assertThat(settings.getAsInt("index.merge.scheduler.max_thread_count", null), equalTo(maxThreadCount));
                    assertNull(settings.get("index.non_copyable.existing"));
                    assertThat(settings.get("index.non_copyable.request"), equalTo("request"));
                });
    }

    public void testPrepareResizeIndexSettingsAnalysisSettings() {
        // analysis settings from the request are not overwritten
        runPrepareResizeIndexSettingsTest(
                Settings.EMPTY,
                Settings.builder().put("index.analysis.analyzer.default.tokenizer", "whitespace").build(),
                emptyList(),
                randomBoolean(),
                settings ->
                    assertThat(
                            "analysis settings are not overwritten",
                            settings.get("index.analysis.analyzer.default.tokenizer"),
                            equalTo("whitespace"))
                );

    }

    public void testPrepareResizeIndexSettingsSimilaritySettings() {
        // similarity settings from the request are not overwritten
        runPrepareResizeIndexSettingsTest(
                Settings.EMPTY,
                Settings.builder().put("index.similarity.sim.type", "DFR").build(),
                emptyList(),
                randomBoolean(),
                settings ->
                        assertThat("similarity settings are not overwritten", settings.get("index.similarity.sim.type"), equalTo("DFR")));

    }

    public void testDoNotOverrideSoftDeletesSettingOnResize() {
        runPrepareResizeIndexSettingsTest(
            Settings.builder().put("index.soft_deletes.enabled", "false").build(),
            Settings.builder().put("index.soft_deletes.enabled", "true").build(),
            emptyList(),
            randomBoolean(),
            settings -> assertThat(settings.get("index.soft_deletes.enabled"), equalTo("true")));
    }

    private void runPrepareResizeIndexSettingsTest(
            final Settings sourceSettings,
            final Settings requestSettings,
            final Collection<Setting<?>> additionalIndexScopedSettings,
            final boolean copySettings,
            final Consumer<Settings> consumer) {
        final String indexName = randomAlphaOfLength(10);

        final Settings indexSettings = Settings.builder()
                .put("index.blocks.write", true)
                .put("index.routing.allocation.require._name", "node1")
                .put(sourceSettings)
                .build();

        final ClusterState initialClusterState =
                ClusterState
                        .builder(createClusterState(indexName, randomIntBetween(2, 10), 0, indexSettings))
                        .nodes(DiscoveryNodes.builder().add(newNode("node1")))
                        .build();

        final AllocationService service = new AllocationService(
                new AllocationDeciders(singleton(new MaxRetryAllocationDecider())),
                new TestGatewayAllocator(),
                new BalancedShardsAllocator(Settings.EMPTY),
                EmptyClusterInfoService.INSTANCE,
                EmptySnapshotsInfoService.INSTANCE);

        final RoutingTable initialRoutingTable = service.reroute(initialClusterState, "reroute").routingTable();
        final ClusterState routingTableClusterState = ClusterState.builder(initialClusterState).routingTable(initialRoutingTable).build();

        // now we start the shard
        final RoutingTable routingTable
            = HavenaskAllocationTestCase.startInitializingShardsAndReroute(service, routingTableClusterState, indexName).routingTable();
        final ClusterState clusterState = ClusterState.builder(routingTableClusterState).routingTable(routingTable).build();

        final Settings.Builder indexSettingsBuilder = Settings.builder().put("index.number_of_shards", 1).put(requestSettings);
        final Set<Setting<?>> settingsSet =
                Stream.concat(
                        IndexScopedSettings.BUILT_IN_INDEX_SETTINGS.stream(),
                        additionalIndexScopedSettings.stream())
                        .collect(Collectors.toSet());
        MetadataCreateIndexService.prepareResizeIndexSettings(
                clusterState,
                indexSettingsBuilder,
                clusterState.metadata().index(indexName).getIndex(),
                "target",
                ResizeType.SHRINK,
                copySettings,
                new IndexScopedSettings(Settings.EMPTY, settingsSet));
        consumer.accept(indexSettingsBuilder.build());
    }

    private DiscoveryNode newNode(String nodeId) {
        final Set<DiscoveryNodeRole> roles =
                Collections.unmodifiableSet(new HashSet<>(Arrays.asList(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.DATA_ROLE)));
        return new DiscoveryNode(nodeId, buildNewFakeTransportAddress(), emptyMap(), roles, Version.CURRENT);
    }

    public void testValidateIndexName() throws Exception {
        withTemporaryClusterService(((clusterService, threadPool) -> {
            MetadataCreateIndexService checkerService = new MetadataCreateIndexService(
                Settings.EMPTY,
                clusterService,
                null,
                null,
                null,
                createTestShardLimitService(randomIntBetween(1, 1000), clusterService), null,
                null,
                threadPool,
                null,
                new SystemIndices(Collections.emptyMap()),
                false
            );
            validateIndexName(checkerService, "index?name", "must not contain the following characters " + Strings.INVALID_FILENAME_CHARS);

            validateIndexName(checkerService, "index#name", "must not contain '#'");

            validateIndexName(checkerService, "_indexname", "must not start with '_', '-', or '+'");
            validateIndexName(checkerService, "-indexname", "must not start with '_', '-', or '+'");
            validateIndexName(checkerService, "+indexname", "must not start with '_', '-', or '+'");

            validateIndexName(checkerService, "INDEXNAME", "must be lowercase");

            validateIndexName(checkerService, "..", "must not be '.' or '..'");

            validateIndexName(checkerService, "foo:bar", "must not contain ':'");
        }));
    }

    private void validateIndexName(MetadataCreateIndexService metadataCreateIndexService, String indexName, String errorMessage) {
        InvalidIndexNameException e = expectThrows(InvalidIndexNameException.class,
            () -> metadataCreateIndexService.validateIndexName(indexName, ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING
                .getDefault(Settings.EMPTY)).build()));
        assertThat(e.getMessage(), endsWith(errorMessage));
    }

    public void testCalculateNumRoutingShards() {
        assertEquals(1024, MetadataCreateIndexService.calculateNumRoutingShards(1, Version.CURRENT));
        assertEquals(1024, MetadataCreateIndexService.calculateNumRoutingShards(2, Version.CURRENT));
        assertEquals(768, MetadataCreateIndexService.calculateNumRoutingShards(3, Version.CURRENT));
        assertEquals(576, MetadataCreateIndexService.calculateNumRoutingShards(9, Version.CURRENT));
        assertEquals(1024, MetadataCreateIndexService.calculateNumRoutingShards(512, Version.CURRENT));
        assertEquals(2048, MetadataCreateIndexService.calculateNumRoutingShards(1024, Version.CURRENT));
        assertEquals(4096, MetadataCreateIndexService.calculateNumRoutingShards(2048, Version.CURRENT));

        Version latestV6 = VersionUtils.getPreviousVersion(LegacyESVersion.V_7_0_0);
        int numShards = randomIntBetween(1, 1000);
        assertEquals(numShards, MetadataCreateIndexService.calculateNumRoutingShards(numShards, latestV6));
        assertEquals(numShards, MetadataCreateIndexService.calculateNumRoutingShards(numShards,
            VersionUtils.randomVersionBetween(random(), VersionUtils.getFirstVersion(), latestV6)));

        for (int i = 0; i < 1000; i++) {
            int randomNumShards = randomIntBetween(1, 10000);
            int numRoutingShards = MetadataCreateIndexService.calculateNumRoutingShards(randomNumShards, Version.CURRENT);
            if (numRoutingShards <= 1024) {
                assertTrue("numShards: " + randomNumShards, randomNumShards < 513);
                assertTrue("numRoutingShards: " + numRoutingShards, numRoutingShards > 512);
            } else {
                assertEquals("numShards: " + randomNumShards, numRoutingShards / 2, randomNumShards);
            }

            double ratio = numRoutingShards / randomNumShards;
            int intRatio = (int) ratio;
            assertEquals(ratio, (double)(intRatio), 0.0d);
            assertTrue(1 < ratio);
            assertTrue(ratio <= 1024);
            assertEquals(0, intRatio % 2);
            assertEquals("ratio is not a power of two", intRatio, Integer.highestOneBit(intRatio));
        }
    }

    public void testValidateDotIndex() {
        List<SystemIndexDescriptor> systemIndexDescriptors = new ArrayList<>();
        systemIndexDescriptors.add(new SystemIndexDescriptor(".test", "test"));
        systemIndexDescriptors.add(new SystemIndexDescriptor(".test3", "test"));
        systemIndexDescriptors.add(new SystemIndexDescriptor(".pattern-test*", "test-1"));

        withTemporaryClusterService(((clusterService, threadPool) -> {
            MetadataCreateIndexService checkerService = new MetadataCreateIndexService(
                Settings.EMPTY,
                clusterService,
                null,
                null,
                null,
                createTestShardLimitService(randomIntBetween(1, 1000), clusterService), null,
                null,
                threadPool,
                null,
                new SystemIndices(Collections.singletonMap("foo", systemIndexDescriptors)),
                false
            );
            // Check deprecations
            assertFalse(checkerService.validateDotIndex(".test2", false));
            assertWarnings("index name [.test2] starts with a dot '.', in the next major version, index " +
                "names starting with a dot are reserved for hidden indices and system indices");

            // Check non-system hidden indices don't trigger a warning
            assertFalse(checkerService.validateDotIndex(".test2", true));

            // Check NO deprecation warnings if we give the index name
            assertTrue(checkerService.validateDotIndex(".test", false));
            assertTrue(checkerService.validateDotIndex(".test3", false));

            // Check that patterns with wildcards work
            assertTrue(checkerService.validateDotIndex(".pattern-test", false));
            assertTrue(checkerService.validateDotIndex(".pattern-test-with-suffix", false));
            assertTrue(checkerService.validateDotIndex(".pattern-test-other-suffix", false));
        }));
    }

    public void testParseMappingsAppliesDataFromTemplateAndRequest() throws Exception {
        IndexTemplateMetadata templateMetadata = addMatchingTemplate(templateBuilder -> {
            templateBuilder.putAlias(AliasMetadata.builder("alias1"));
            templateBuilder.putMapping("type", createMapping("mapping_from_template", "text"));
        });
        request.mappings(singletonMap("type", createMapping("mapping_from_request", "text").string()));

        Map<String, Map<String, Object>> parsedMappings = MetadataCreateIndexService.parseV1Mappings(request.mappings(),
            Collections.singletonList(convertMappings(templateMetadata.getMappings())), NamedXContentRegistry.EMPTY);

        assertThat(parsedMappings, hasKey("type"));
        Map<String, Object> mappingType = parsedMappings.get("type");
        assertThat(mappingType, hasKey("type"));
        Map<String, Object> type = (Map<String, Object>) mappingType.get("type");
        assertThat(type, hasKey("properties"));
        Map<String, Object> mappingsProperties = (Map<String, Object>) type.get("properties");
        assertThat(mappingsProperties, hasKey("mapping_from_request"));
        assertThat(mappingsProperties, hasKey("mapping_from_template"));
    }

    public void testAggregateSettingsAppliesSettingsFromTemplatesAndRequest() {
        IndexTemplateMetadata templateMetadata = addMatchingTemplate(builder -> {
            builder.settings(Settings.builder().put("template_setting", "value1"));
        });
        ImmutableOpenMap.Builder<String, IndexTemplateMetadata> templatesBuilder = ImmutableOpenMap.builder();
        templatesBuilder.put("template_1", templateMetadata);
        Metadata metadata = new Metadata.Builder().templates(templatesBuilder.build()).build();
        ClusterState clusterState = ClusterState.builder(org.havenask.cluster.ClusterName.CLUSTER_NAME_SETTING
            .getDefault(Settings.EMPTY))
            .metadata(metadata)
            .build();
        request.settings(Settings.builder().put("request_setting", "value2").build());

        Settings aggregatedIndexSettings = aggregateIndexSettings(clusterState, request, templateMetadata.settings(),
            null, Settings.EMPTY, IndexScopedSettings.DEFAULT_SCOPED_SETTINGS, randomShardLimitService(),
            Collections.emptySet());

        assertThat(aggregatedIndexSettings.get("template_setting"), equalTo("value1"));
        assertThat(aggregatedIndexSettings.get("request_setting"), equalTo("value2"));
    }

    public void testInvalidAliasName() {
        final String[] invalidAliasNames = new String[] { "-alias1", "+alias2", "_alias3", "a#lias", "al:ias", ".", ".." };
        String aliasName = randomFrom(invalidAliasNames);
        request.aliases(singleton(new Alias(aliasName)));

        expectThrows(InvalidAliasNameException.class, () ->
            resolveAndValidateAliases(request.index(), request.aliases(), emptyList(), Metadata.builder().build(), aliasValidator,
                xContentRegistry(), queryShardContext)
        );
    }

    public void testRequestDataHavePriorityOverTemplateData() throws Exception {
        CompressedXContent templateMapping = createMapping("test", "text");
        CompressedXContent reqMapping = createMapping("test", "keyword");

        IndexTemplateMetadata templateMetadata = addMatchingTemplate(builder -> builder
            .putAlias(AliasMetadata.builder("alias").searchRouting("fromTemplate").build())
            .putMapping("type", templateMapping)
            .settings(Settings.builder().put("key1", "templateValue"))
        );

        request.mappings(singletonMap("type", reqMapping.string()));
        request.aliases(singleton(new Alias("alias").searchRouting("fromRequest")));
        request.settings(Settings.builder().put("key1", "requestValue").build());

        Map<String, Map<String, Object>> parsedMappings = MetadataCreateIndexService.parseV1Mappings(request.mappings(),
            Collections.singletonList(convertMappings(templateMetadata.mappings())), xContentRegistry());
        List<AliasMetadata> resolvedAliases = resolveAndValidateAliases(request.index(), request.aliases(),
            MetadataIndexTemplateService.resolveAliases(Collections.singletonList(templateMetadata)),
            Metadata.builder().build(), aliasValidator, xContentRegistry(), queryShardContext);
        Settings aggregatedIndexSettings = aggregateIndexSettings(ClusterState.EMPTY_STATE, request, templateMetadata.settings(),
            null, Settings.EMPTY, IndexScopedSettings.DEFAULT_SCOPED_SETTINGS, randomShardLimitService(),
            Collections.emptySet());

        assertThat(resolvedAliases.get(0).getSearchRouting(), equalTo("fromRequest"));
        assertThat(aggregatedIndexSettings.get("key1"), equalTo("requestValue"));
        assertThat(parsedMappings, hasKey("type"));
        Map<String, Object> mappingType = parsedMappings.get("type");
        assertThat(mappingType, hasKey("type"));
        Map<String, Object> type = (Map<String, Object>) mappingType.get("type");
        assertThat(type, hasKey("properties"));
        Map<String, Object> mappingsProperties = (Map<String, Object>) type.get("properties");
        assertThat(mappingsProperties, hasKey("test"));
        assertThat((Map<String, Object>) mappingsProperties.get("test"), hasValue("keyword"));
    }

    public void testDefaultSettings() {
        Settings aggregatedIndexSettings = aggregateIndexSettings(ClusterState.EMPTY_STATE, request, Settings.EMPTY,
            null, Settings.EMPTY, IndexScopedSettings.DEFAULT_SCOPED_SETTINGS, randomShardLimitService(),
            Collections.emptySet());

        assertThat(aggregatedIndexSettings.get(SETTING_NUMBER_OF_SHARDS), equalTo("1"));
    }

    public void testSettingsFromClusterState() {
        Settings aggregatedIndexSettings = aggregateIndexSettings(ClusterState.EMPTY_STATE, request, Settings.EMPTY,
            null, Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 15).build(), IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
            randomShardLimitService(), Collections.emptySet());

        assertThat(aggregatedIndexSettings.get(SETTING_NUMBER_OF_SHARDS), equalTo("15"));
    }

    public void testTemplateOrder() throws Exception {
        List<IndexTemplateMetadata> templates = new ArrayList<>(3);
        templates.add(addMatchingTemplate(builder -> builder
            .order(3)
            .settings(Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 12))
            .putAlias(AliasMetadata.builder("alias1").writeIndex(true).searchRouting("3").build())
        ));
        templates.add(addMatchingTemplate(builder -> builder
            .order(2)
            .settings(Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 11))
            .putAlias(AliasMetadata.builder("alias1").searchRouting("2").build())
        ));
        templates.add(addMatchingTemplate(builder -> builder
            .order(1)
            .settings(Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 10))
            .putAlias(AliasMetadata.builder("alias1").searchRouting("1").build())
        ));
        Settings aggregatedIndexSettings = aggregateIndexSettings(ClusterState.EMPTY_STATE, request,
            MetadataIndexTemplateService.resolveSettings(templates), null, Settings.EMPTY,
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS, randomShardLimitService(), Collections.emptySet());
        List<AliasMetadata> resolvedAliases = resolveAndValidateAliases(request.index(), request.aliases(),
            MetadataIndexTemplateService.resolveAliases(templates),
            Metadata.builder().build(), aliasValidator, xContentRegistry(), queryShardContext);
        assertThat(aggregatedIndexSettings.get(SETTING_NUMBER_OF_SHARDS), equalTo("12"));
        AliasMetadata alias = resolvedAliases.get(0);
        assertThat(alias.getSearchRouting(), equalTo("3"));
        assertThat(alias.writeIndex(), is(true));
    }

    public void testAggregateIndexSettingsIgnoresTemplatesOnCreateFromSourceIndex() throws Exception {
        CompressedXContent templateMapping = createMapping("test", "text");

        IndexTemplateMetadata templateMetadata = addMatchingTemplate(builder -> builder
            .putAlias(AliasMetadata.builder("alias").searchRouting("fromTemplate").build())
            .putMapping("_doc", templateMapping)
            .settings(Settings.builder().put("templateSetting", "templateValue"))
        );

        request.settings(Settings.builder().put("requestSetting", "requestValue").build());
        request.resizeType(ResizeType.SPLIT);
        request.recoverFrom(new Index("sourceIndex", UUID.randomUUID().toString()));
        ClusterState clusterState =
            createClusterState("sourceIndex", 1, 0,
                Settings.builder().put("index.blocks.write", true).build());

        Settings aggregatedIndexSettings = aggregateIndexSettings(clusterState, request, templateMetadata.settings(),
            clusterState.metadata().index("sourceIndex"), Settings.EMPTY, IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
            randomShardLimitService(), Collections.emptySet());

        assertThat(aggregatedIndexSettings.get("templateSetting"), is(nullValue()));
        assertThat(aggregatedIndexSettings.get("requestSetting"), is("requestValue"));
    }

    public void testClusterStateCreateIndexThrowsWriteIndexValidationException() throws Exception {
        IndexMetadata existingWriteIndex = IndexMetadata.builder("test2")
            .settings(settings(Version.CURRENT)).putAlias(AliasMetadata.builder("alias1").writeIndex(true).build())
            .numberOfShards(1).numberOfReplicas(0).build();
        ClusterState currentClusterState =
            ClusterState.builder(ClusterState.EMPTY_STATE).metadata(Metadata.builder().put(existingWriteIndex, false).build()).build();

        IndexMetadata newIndex = IndexMetadata.builder("test")
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putAlias(AliasMetadata.builder("alias1").writeIndex(true).build())
            .build();

        assertThat(
            expectThrows(IllegalStateException.class,
                () -> clusterStateCreateIndex(currentClusterState, org.havenask.common.collect.Set.of(), newIndex,
                    (state, reason) -> state, null)).getMessage(),
            startsWith("alias [alias1] has more than one write index [")
        );
    }

    public void testClusterStateCreateIndex() {
        ClusterState currentClusterState =
            ClusterState.builder(ClusterState.EMPTY_STATE).build();

        IndexMetadata newIndexMetadata = IndexMetadata.builder("test")
            .settings(settings(Version.CURRENT).put(SETTING_READ_ONLY, true))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putAlias(AliasMetadata.builder("alias1").writeIndex(true).build())
            .build();

        // used as a value container, not for the concurrency and visibility guarantees
        AtomicBoolean allocationRerouted = new AtomicBoolean(false);
        BiFunction<ClusterState, String, ClusterState> rerouteRoutingTable = (clusterState, reason) -> {
            allocationRerouted.compareAndSet(false, true);
            return clusterState;
        };

        ClusterState updatedClusterState = clusterStateCreateIndex(currentClusterState,
            org.havenask.common.collect.Set.of(INDEX_READ_ONLY_BLOCK), newIndexMetadata, rerouteRoutingTable, null);
        assertThat(updatedClusterState.blocks().getIndexBlockWithId("test", INDEX_READ_ONLY_BLOCK.id()), is(INDEX_READ_ONLY_BLOCK));
        assertThat(updatedClusterState.routingTable().index("test"), is(notNullValue()));
        assertThat(allocationRerouted.get(), is(true));
    }

    public void testClusterStateCreateIndexWithMetadataTransaction() {
        ClusterState currentClusterState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .metadata(Metadata.builder()
                .put(IndexMetadata.builder("my-index")
                    .settings(settings(Version.CURRENT).put(SETTING_READ_ONLY, true))
                    .numberOfShards(1)
                    .numberOfReplicas(0)))
            .build();

        IndexMetadata newIndexMetadata = IndexMetadata.builder("test")
            .settings(settings(Version.CURRENT).put(SETTING_READ_ONLY, true))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putAlias(AliasMetadata.builder("alias1").writeIndex(true).build())
            .build();

        // adds alias from new index to existing index
        BiConsumer<Metadata.Builder, IndexMetadata> metadataTransformer = (builder, indexMetadata) -> {
            AliasMetadata newAlias = indexMetadata.getAliases().iterator().next().value;
            IndexMetadata myIndex = builder.get("my-index");
            builder.put(IndexMetadata.builder(myIndex).putAlias(AliasMetadata.builder(newAlias.getAlias()).build()));
        };

        ClusterState updatedClusterState = clusterStateCreateIndex(currentClusterState, org.havenask.common.collect.Set.of(
            INDEX_READ_ONLY_BLOCK), newIndexMetadata, (clusterState, y) -> clusterState, metadataTransformer);
        assertTrue(updatedClusterState.metadata().findAllAliases(new String[]{"my-index"}).containsKey("my-index"));
        assertNotNull(updatedClusterState.metadata().findAllAliases(new String[]{"my-index"}).get("my-index"));
        assertNotNull(updatedClusterState.metadata().findAllAliases(new String[]{"my-index"}).get("my-index").get(0).alias(),
            equalTo("alias1"));
    }

    public void testParseMappingsWithTypedTemplateAndTypelessIndexMapping() throws Exception {
        IndexTemplateMetadata templateMetadata = addMatchingTemplate(builder -> {
            try {
                builder.putMapping("type", "{\"type\": {}}");
            } catch (IOException e) {
                ExceptionsHelper.reThrowIfNotNull(e);
            }
        });

        Map<String, Map<String, Object>> mappings = parseV1Mappings(singletonMap(MapperService.SINGLE_MAPPING_NAME, "{\"_doc\":{}}"),
            Collections.singletonList(convertMappings(templateMetadata.mappings())), xContentRegistry());
        assertThat(mappings, Matchers.hasKey(MapperService.SINGLE_MAPPING_NAME));
    }

    public void testParseMappingsWithTypedTemplate() throws Exception {
        IndexTemplateMetadata templateMetadata = addMatchingTemplate(builder -> {
            try {
                builder.putMapping("type",
                    "{\"type\":{\"properties\":{\"field\":{\"type\":\"keyword\"}}}}");
            } catch (IOException e) {
                ExceptionsHelper.reThrowIfNotNull(e);
            }
        });
        Map<String, Map<String, Object>> mappings = parseV1Mappings(emptyMap(),
            Collections.singletonList(convertMappings(templateMetadata.mappings())), xContentRegistry());
        assertThat(mappings, Matchers.hasKey("type"));
    }

    public void testParseMappingsWithTypelessTemplate() throws Exception {
        IndexTemplateMetadata templateMetadata = addMatchingTemplate(builder -> {
            try {
                builder.putMapping(MapperService.SINGLE_MAPPING_NAME, "{\"_doc\": {}}");
            } catch (IOException e) {
                ExceptionsHelper.reThrowIfNotNull(e);
            }
        });
        Map<String, Map<String, Object>> mappings = parseV1Mappings(emptyMap(),
            Collections.singletonList(convertMappings(templateMetadata.mappings())), xContentRegistry());
        assertThat(mappings, Matchers.hasKey(MapperService.SINGLE_MAPPING_NAME));
    }

    public void testBuildIndexMetadata() {
        IndexMetadata sourceIndexMetadata = IndexMetadata.builder("parent")
            .settings(Settings.builder()
                .put("index.version.created", Version.CURRENT)
                .build())
            .numberOfShards(1)
            .numberOfReplicas(0)
            .primaryTerm(0, 3L)
            .build();

        Settings indexSettings = Settings.builder()
            .put("index.version.created", Version.CURRENT)
            .put(INDEX_SOFT_DELETES_SETTING.getKey(), false)
            .put(SETTING_NUMBER_OF_REPLICAS, 0)
            .put(SETTING_NUMBER_OF_SHARDS, 1)
            .build();
        List<AliasMetadata> aliases = singletonList(AliasMetadata.builder("alias1").build());
        IndexMetadata indexMetadata =
            buildIndexMetadata("test", aliases, () -> null, () -> null, indexSettings, 4, sourceIndexMetadata, false);

        assertThat(indexMetadata.getSettings().getAsBoolean(INDEX_SOFT_DELETES_SETTING.getKey(), true), is(false));
        assertThat(indexMetadata.getAliases().size(), is(1));
        assertThat(indexMetadata.getAliases().keys().iterator().next().value, is("alias1"));
        assertThat("The source index primary term must be used", indexMetadata.primaryTerm(0), is(3L));
    }

    public void testGetIndexNumberOfRoutingShardsWithNullSourceIndex() {
        Settings indexSettings = Settings.builder()
            .put("index.version.created", Version.CURRENT)
            .put(INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 3)
            .build();
        int targetRoutingNumberOfShards = getIndexNumberOfRoutingShards(indexSettings, null);
        assertThat("When the target routing number of shards is not specified the expected value is the configured number of shards " +
            "multiplied by 2 at most ten times (ie. 3 * 2^8)", targetRoutingNumberOfShards, is(768));
    }

    public void testGetIndexNumberOfRoutingShardsWhenExplicitlyConfigured() {
        Settings indexSettings = Settings.builder()
            .put(INDEX_NUMBER_OF_ROUTING_SHARDS_SETTING.getKey(), 9)
            .put(INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 3)
            .build();
        int targetRoutingNumberOfShards = getIndexNumberOfRoutingShards(indexSettings, null);
        assertThat(targetRoutingNumberOfShards, is(9));
    }

    public void testGetIndexNumberOfRoutingShardsYieldsSourceNumberOfShards() {
        Settings indexSettings = Settings.builder()
            .put(INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 3)
            .build();

        IndexMetadata sourceIndexMetadata = IndexMetadata.builder("parent")
            .settings(Settings.builder()
                .put("index.version.created", Version.CURRENT)
                .build())
            .numberOfShards(6)
            .numberOfReplicas(0)
            .build();

        int targetRoutingNumberOfShards = getIndexNumberOfRoutingShards(indexSettings, sourceIndexMetadata);
        assertThat(targetRoutingNumberOfShards, is(6));
    }

    public void testSoftDeletesDisabledDeprecation() {
        request = new CreateIndexClusterStateUpdateRequest("create index", "test", "test");
        request.settings(Settings.builder().put(INDEX_SOFT_DELETES_SETTING.getKey(), false).build());
        aggregateIndexSettings(ClusterState.EMPTY_STATE, request, Settings.EMPTY,
            null, Settings.EMPTY, IndexScopedSettings.DEFAULT_SCOPED_SETTINGS, randomShardLimitService(),
            Collections.emptySet());
        assertWarnings("Creating indices with soft-deletes disabled is deprecated and will be removed in future Havenask versions. "
            + "Please do not specify value for setting [index.soft_deletes.enabled] of index [test].");
        request = new CreateIndexClusterStateUpdateRequest("create index", "test", "test");
        if (randomBoolean()) {
            request.settings(Settings.builder().put(INDEX_SOFT_DELETES_SETTING.getKey(), true).build());
        }
        aggregateIndexSettings(ClusterState.EMPTY_STATE, request, Settings.EMPTY,
            null, Settings.EMPTY, IndexScopedSettings.DEFAULT_SCOPED_SETTINGS, randomShardLimitService(),
            Collections.emptySet());
    }

    public void testValidateTranslogRetentionSettings() {
        request = new CreateIndexClusterStateUpdateRequest("create index", "test", "test");
        final Settings.Builder settings = Settings.builder();
        if (randomBoolean()) {
            settings.put(IndexSettings.INDEX_TRANSLOG_RETENTION_AGE_SETTING.getKey(), TimeValue.timeValueMillis(between(1, 120)));
        } else {
            settings.put(IndexSettings.INDEX_TRANSLOG_RETENTION_SIZE_SETTING.getKey(), between(1, 128) + "mb");
        }
        request.settings(settings.build());
        aggregateIndexSettings(ClusterState.EMPTY_STATE, request, Settings.EMPTY,
            null, Settings.EMPTY, IndexScopedSettings.DEFAULT_SCOPED_SETTINGS, randomShardLimitService(),
            Collections.emptySet());
        assertWarnings("Translog retention settings [index.translog.retention.age] "
            + "and [index.translog.retention.size] are deprecated and effectively ignored. They will be removed in a future version.");
    }

    private IndexTemplateMetadata addMatchingTemplate(Consumer<IndexTemplateMetadata.Builder> configurator) {
        IndexTemplateMetadata.Builder builder = templateMetadataBuilder("template1", "te*");
        configurator.accept(builder);
        return builder.build();
    }

    private IndexTemplateMetadata.Builder templateMetadataBuilder(String name, String pattern) {
        return IndexTemplateMetadata
            .builder(name)
            .patterns(singletonList(pattern));
    }

    private CompressedXContent createMapping(String fieldName, String fieldType) {
        try {
            final String mapping = Strings.toString(XContentFactory.jsonBuilder()
                .startObject()
                .startObject("type")
                .startObject("properties")
                .startObject(fieldName)
                .field("type", fieldType)
                .endObject()
                .endObject()
                .endObject()
                .endObject());

            return new CompressedXContent(mapping);
        } catch (IOException e) {
            throw ExceptionsHelper.convertToRuntime(e);
        }
    }

    private static Map<String, CompressedXContent> convertMappings(ImmutableOpenMap<String, CompressedXContent> mappings) {
        Map<String, CompressedXContent> converted = new HashMap<>(mappings.size());
        for (ObjectObjectCursor<String, CompressedXContent> cursor : mappings) {
            converted.put(cursor.key, cursor.value);
        }
        return converted;
    }
    private ShardLimitValidator randomShardLimitService() {
        return createTestShardLimitService(randomIntBetween(10,10000));
    }

    private void withTemporaryClusterService(BiConsumer<ClusterService, ThreadPool> consumer) {
        ThreadPool threadPool = new TestThreadPool(getTestName());
        try {
            final ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);
            consumer.accept(clusterService, threadPool);
        } finally {
            threadPool.shutdown();
        }
    }

    private IndexMappingProvider getIndexMappingProvider(XContentBuilder xContentBuilder) {
        String mappings = Strings.toString(xContentBuilder);
        Map<String, Object> mapping = XContentHelper.convertToMap(XContentType.JSON.xContent(), mappings, true);
        Map<String, Object> type = (Map<String, Object>) mapping.get("_doc");
        return new IndexMappingProvider() {
            @Override
            public Map<String, Object> getAdditionalIndexMapping(Settings settings) {
                return type;
            }

            @Override
            public void validateIndexMapping(String table, Settings indexSettings, MapperService mapperService) throws UnsupportedOperationException {
                if (mapperService.hasNested()) {
                    throw new UnsupportedOperationException("nested field not support");
                }
            }

        };
    }

    public void testAdditionalIndexMapping() throws IOException {
        IndexService indexService = createIndex("test");
        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .field("dynamic", "strict")
            .startObject("properties")
            .startObject("field")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        IndexMappingProvider provider = getIndexMappingProvider(builder);
        MetadataCreateIndexService.updateIndexMappingsAndBuildSortOrder(
            indexService,
            new CreateIndexClusterStateUpdateRequest("cause", "test", "test"),
            Collections.emptyList(),
            null,
            Settings.EMPTY,
            Collections.singleton(provider)
        );
        assertEquals(Strings.toString(builder), indexService.mapperService().documentMapper().mappingSource().toString());
    }

    // test additional mappings are merged with existing mappings
    public void testAdditionalIndexMappingMerge() throws IOException {
        IndexService indexService = createIndex("test");
        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .field("dynamic", "strict")
            .startObject("properties")
            .startObject("field")
            .field("type", "keyword")
            .endObject()
            .startObject("field2")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        IndexMappingProvider provider = getIndexMappingProvider(builder);

        List<Map<String, Map<String, Object>>> mappings = new ArrayList<>();
        Map<String, Map<String, Object>> mapping = new HashMap<>();

        Map<String, Object> properties = new HashMap<>();
        Map<String, Object> field = new HashMap<>();
        field.put("type", "text");
        properties.put("field", field);
        mapping.put("_doc", Collections.singletonMap("properties", properties));
        mappings.add(mapping);
        MetadataCreateIndexService.updateIndexMappingsAndBuildSortOrder(
            indexService,
            new CreateIndexClusterStateUpdateRequest("cause", "test", "test"),
            mappings,
            null,
            Settings.EMPTY,
            Collections.singleton(provider)
        );
        String expect = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject("_doc")
                .field("dynamic", "strict")
                .startObject("properties")
                .startObject("field")
                .field("type", "text")
                .endObject()
                .startObject("field2")
                .field("type", "keyword")
                .endObject()
                .endObject()
                .endObject()
                .endObject()
        );
        assertEquals(expect, indexService.mapperService().documentMapper().mappingSource().toString());
    }

    // test valid nested mapping
    public void testAdditionalIndexMappingValidNested() throws IOException {
        IndexService indexService = createIndex("test");
        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .field("dynamic", "strict")
            .startObject("properties")
            .startObject("field")
            .field("type", "keyword")
            .endObject()
            .startObject("nested")
            .field("type", "nested")
            .startObject("properties")
            .startObject("field")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        IndexMappingProvider provider = getIndexMappingProvider(builder);

        // except UnsupportedOperationException
        UnsupportedOperationException e = expectThrows(UnsupportedOperationException.class, () -> MetadataCreateIndexService.updateIndexMappingsAndBuildSortOrder(
            indexService,
            new CreateIndexClusterStateUpdateRequest("cause", "test", "test"),
            Collections.emptyList(),
            null,
            Settings.EMPTY,
            Collections.singleton(provider)
        ));
        assertEquals("nested field not support", e.getMessage());
    }
}

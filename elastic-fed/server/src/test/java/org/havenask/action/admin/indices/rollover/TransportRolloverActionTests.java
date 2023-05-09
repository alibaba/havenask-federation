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

package org.havenask.action.admin.indices.rollover;

import org.havenask.Version;
import org.havenask.action.ActionListener;
import org.havenask.action.ActionRequest;
import org.havenask.action.admin.indices.stats.CommonStats;
import org.havenask.action.admin.indices.stats.IndexStats;
import org.havenask.action.admin.indices.stats.IndicesStatsAction;
import org.havenask.action.admin.indices.stats.IndicesStatsResponse;
import org.havenask.action.admin.indices.stats.IndicesStatsTests;
import org.havenask.action.admin.indices.stats.ShardStats;
import org.havenask.action.support.ActionFilters;
import org.havenask.action.support.PlainActionFuture;
import org.havenask.client.Client;
import org.havenask.cluster.ClusterName;
import org.havenask.cluster.ClusterState;
import org.havenask.cluster.metadata.AliasMetadata;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.cluster.metadata.IndexNameExpressionResolver;
import org.havenask.cluster.metadata.Metadata;
import org.havenask.cluster.metadata.MetadataCreateIndexService;
import org.havenask.cluster.metadata.MetadataIndexAliasesService;
import org.havenask.cluster.node.DiscoveryNode;
import org.havenask.cluster.routing.RecoverySource;
import org.havenask.cluster.routing.ShardRouting;
import org.havenask.cluster.routing.UnassignedInfo;
import org.havenask.cluster.service.ClusterService;
import org.havenask.common.UUIDs;
import org.havenask.common.settings.Settings;
import org.havenask.common.unit.ByteSizeUnit;
import org.havenask.common.unit.ByteSizeValue;
import org.havenask.common.unit.TimeValue;
import org.havenask.common.util.set.Sets;
import org.havenask.index.cache.query.QueryCacheStats;
import org.havenask.index.cache.request.RequestCacheStats;
import org.havenask.index.engine.SegmentsStats;
import org.havenask.index.fielddata.FieldDataStats;
import org.havenask.index.flush.FlushStats;
import org.havenask.index.get.GetStats;
import org.havenask.index.merge.MergeStats;
import org.havenask.index.refresh.RefreshStats;
import org.havenask.index.search.stats.SearchStats;
import org.havenask.index.shard.DocsStats;
import org.havenask.index.shard.IndexingStats;
import org.havenask.index.shard.ShardId;
import org.havenask.index.shard.ShardPath;
import org.havenask.index.store.StoreStats;
import org.havenask.index.warmer.WarmerStats;
import org.havenask.search.suggest.completion.CompletionStats;
import org.havenask.tasks.Task;
import org.havenask.test.HavenaskTestCase;
import org.havenask.threadpool.ThreadPool;
import org.havenask.transport.TransportService;
import org.mockito.ArgumentCaptor;
import org.havenask.action.admin.indices.rollover.Condition;
import org.havenask.action.admin.indices.rollover.MaxAgeCondition;
import org.havenask.action.admin.indices.rollover.MaxDocsCondition;
import org.havenask.action.admin.indices.rollover.MaxSizeCondition;
import org.havenask.action.admin.indices.rollover.MetadataRolloverService;
import org.havenask.action.admin.indices.rollover.RolloverRequest;
import org.havenask.action.admin.indices.rollover.RolloverResponse;
import org.havenask.action.admin.indices.rollover.TransportRolloverAction;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptyList;
import static org.havenask.action.admin.indices.rollover.TransportRolloverAction.evaluateConditions;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class TransportRolloverActionTests extends HavenaskTestCase {

    public void testDocStatsSelectionFromPrimariesOnly() {
        long docsInPrimaryShards = 100;
        long docsInShards = 200;

        final Condition<?> condition = createTestCondition();
        String indexName = randomAlphaOfLengthBetween(5, 7);
        evaluateConditions(Sets.newHashSet(condition), createMetadata(indexName),
                createIndicesStatResponse(indexName, docsInShards, docsInPrimaryShards));
        final ArgumentCaptor<Condition.Stats> argument = ArgumentCaptor.forClass(Condition.Stats.class);
        verify(condition).evaluate(argument.capture());

        assertEquals(docsInPrimaryShards, argument.getValue().numDocs);
    }

    public void testEvaluateConditions() {
        MaxDocsCondition maxDocsCondition = new MaxDocsCondition(100L);
        MaxAgeCondition maxAgeCondition = new MaxAgeCondition(TimeValue.timeValueHours(2));
        MaxSizeCondition maxSizeCondition = new MaxSizeCondition(new ByteSizeValue(randomIntBetween(10, 100), ByteSizeUnit.MB));

        long matchMaxDocs = randomIntBetween(100, 1000);
        long notMatchMaxDocs = randomIntBetween(0, 99);
        ByteSizeValue notMatchMaxSize = new ByteSizeValue(randomIntBetween(0, 9), ByteSizeUnit.MB);
        final Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();
        final IndexMetadata metadata = IndexMetadata.builder(randomAlphaOfLength(10))
            .creationDate(System.currentTimeMillis() - TimeValue.timeValueHours(3).getMillis())
            .settings(settings)
            .build();
        final Set<Condition<?>> conditions = Sets.newHashSet(maxDocsCondition, maxAgeCondition, maxSizeCondition);
        Map<String, Boolean> results = evaluateConditions(conditions,
            new DocsStats(matchMaxDocs, 0L, ByteSizeUnit.MB.toBytes(120)), metadata);
        assertThat(results.size(), equalTo(3));
        for (Boolean matched : results.values()) {
            assertThat(matched, equalTo(true));
        }

        results = evaluateConditions(conditions, new DocsStats(notMatchMaxDocs, 0, notMatchMaxSize.getBytes()), metadata);
        assertThat(results.size(), equalTo(3));
        for (Map.Entry<String, Boolean> entry : results.entrySet()) {
            if (entry.getKey().equals(maxAgeCondition.toString())) {
                assertThat(entry.getValue(), equalTo(true));
            } else if (entry.getKey().equals(maxDocsCondition.toString())) {
                assertThat(entry.getValue(), equalTo(false));
            } else if (entry.getKey().equals(maxSizeCondition.toString())) {
                assertThat(entry.getValue(), equalTo(false));
            } else {
                fail("unknown condition result found " + entry.getKey());
            }
        }
    }

    public void testEvaluateWithoutDocStats() {
        MaxDocsCondition maxDocsCondition = new MaxDocsCondition(randomNonNegativeLong());
        MaxAgeCondition maxAgeCondition = new MaxAgeCondition(TimeValue.timeValueHours(randomIntBetween(1, 3)));
        MaxSizeCondition maxSizeCondition = new MaxSizeCondition(new ByteSizeValue(randomNonNegativeLong()));

        Set<Condition<?>> conditions = Sets.newHashSet(maxDocsCondition, maxAgeCondition, maxSizeCondition);
        final Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, randomIntBetween(1, 1000))
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, randomInt(10))
            .build();

        final IndexMetadata metadata = IndexMetadata.builder(randomAlphaOfLength(10))
            .creationDate(System.currentTimeMillis() - TimeValue.timeValueHours(randomIntBetween(5, 10)).getMillis())
            .settings(settings)
            .build();
        Map<String, Boolean> results = evaluateConditions(conditions, null, metadata);
        assertThat(results.size(), equalTo(3));

        for (Map.Entry<String, Boolean> entry : results.entrySet()) {
            if (entry.getKey().equals(maxAgeCondition.toString())) {
                assertThat(entry.getValue(), equalTo(true));
            } else if (entry.getKey().equals(maxDocsCondition.toString())) {
                assertThat(entry.getValue(), equalTo(false));
            } else if (entry.getKey().equals(maxSizeCondition.toString())) {
                assertThat(entry.getValue(), equalTo(false));
            } else {
                fail("unknown condition result found " + entry.getKey());
            }
        }
    }

    public void testEvaluateWithoutMetadata() {
        MaxDocsCondition maxDocsCondition = new MaxDocsCondition(100L);
        MaxAgeCondition maxAgeCondition = new MaxAgeCondition(TimeValue.timeValueHours(2));
        MaxSizeCondition maxSizeCondition = new MaxSizeCondition(new ByteSizeValue(randomIntBetween(10, 100), ByteSizeUnit.MB));

        long matchMaxDocs = randomIntBetween(100, 1000);
        final Set<Condition<?>> conditions = Sets.newHashSet(maxDocsCondition, maxAgeCondition, maxSizeCondition);
        Map<String, Boolean> results = evaluateConditions(conditions,
            new DocsStats(matchMaxDocs, 0L, ByteSizeUnit.MB.toBytes(120)), null);
        assertThat(results.size(), equalTo(3));
        results.forEach((k, v) -> assertFalse(v));

        final Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, randomIntBetween(1, 1000))
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, randomInt(10))
            .build();

        final IndexMetadata metadata = IndexMetadata.builder(randomAlphaOfLength(10))
            .creationDate(System.currentTimeMillis() - TimeValue.timeValueHours(randomIntBetween(5, 10)).getMillis())
            .settings(settings)
            .build();
        IndicesStatsResponse indicesStats = randomIndicesStatsResponse(new IndexMetadata[]{metadata});
        Map<String, Boolean> results2 = evaluateConditions(conditions, null, indicesStats);
        assertThat(results2.size(), equalTo(3));
        results2.forEach((k, v) -> assertFalse(v));
    }


    public void testConditionEvaluationWhenAliasToWriteAndReadIndicesConsidersOnlyPrimariesFromWriteIndex() throws Exception {
        final TransportService mockTransportService = mock(TransportService.class);
        final ClusterService mockClusterService = mock(ClusterService.class);
        final DiscoveryNode mockNode = mock(DiscoveryNode.class);
        when(mockNode.getId()).thenReturn("mocknode");
        when(mockClusterService.localNode()).thenReturn(mockNode);
        final ThreadPool mockThreadPool = mock(ThreadPool.class);
        final MetadataCreateIndexService mockCreateIndexService = mock(MetadataCreateIndexService.class);
        final IndexNameExpressionResolver mockIndexNameExpressionResolver = mock(IndexNameExpressionResolver.class);
        when(mockIndexNameExpressionResolver.resolveDateMathExpression(any())).thenReturn("logs-index-000003");
        final ActionFilters mockActionFilters = mock(ActionFilters.class);
        final MetadataIndexAliasesService mdIndexAliasesService = mock(MetadataIndexAliasesService.class);

        final Client mockClient = mock(Client.class);

        final Map<String, IndexStats> indexStats = new HashMap<>();
        int total = randomIntBetween(500, 1000);
        indexStats.put("logs-index-000001", createIndexStats(200L, total));
        indexStats.put("logs-index-000002", createIndexStats(300L, total));
        final IndicesStatsResponse statsResponse = createAliasToMultipleIndicesStatsResponse(indexStats);

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assert args.length == 3;
            ActionListener<IndicesStatsResponse> listener = (ActionListener<IndicesStatsResponse>) args[2];
            listener.onResponse(statsResponse);
            return null;
        }).when(mockClient).execute(eq(IndicesStatsAction.INSTANCE), any(ActionRequest.class), any(ActionListener.class));

        assert statsResponse.getPrimaries().getDocs().getCount() == 500L;
        assert statsResponse.getTotal().getDocs().getCount() == (total + total);

        final IndexMetadata.Builder indexMetadata = IndexMetadata.builder("logs-index-000001")
                .putAlias(AliasMetadata.builder("logs-alias").writeIndex(false).build()).settings(settings(Version.CURRENT))
                .numberOfShards(1).numberOfReplicas(1);
        final IndexMetadata.Builder indexMetadata2 = IndexMetadata.builder("logs-index-000002")
                .putAlias(AliasMetadata.builder("logs-alias").writeIndex(true).build()).settings(settings(Version.CURRENT))
                .numberOfShards(1).numberOfReplicas(1);
        final ClusterState stateBefore = ClusterState.builder(ClusterName.DEFAULT)
                .metadata(Metadata.builder().put(indexMetadata).put(indexMetadata2)).build();

        when(mockCreateIndexService.applyCreateIndexRequest(any(), any(), anyBoolean())).thenReturn(stateBefore);
        when(mdIndexAliasesService.applyAliasActions(any(), any())).thenReturn(stateBefore);
        MetadataRolloverService rolloverService = new MetadataRolloverService(mockThreadPool, mockCreateIndexService,
            mdIndexAliasesService, mockIndexNameExpressionResolver);
        final TransportRolloverAction transportRolloverAction = new TransportRolloverAction(mockTransportService, mockClusterService,
                mockThreadPool, mockActionFilters, mockIndexNameExpressionResolver, rolloverService, mockClient);

        // For given alias, verify that condition evaluation fails when the condition doc count is greater than the primaries doc count
        // (primaries from only write index is considered)
        PlainActionFuture<RolloverResponse> future = new PlainActionFuture<>();
        RolloverRequest rolloverRequest = new RolloverRequest("logs-alias", "logs-index-000003");
        rolloverRequest.addMaxIndexDocsCondition(500L);
        rolloverRequest.dryRun(true);
        transportRolloverAction.masterOperation(mock(Task.class), rolloverRequest, stateBefore, future);

        RolloverResponse response = future.actionGet();
        assertThat(response.getOldIndex(), equalTo("logs-index-000002"));
        assertThat(response.getNewIndex(), equalTo("logs-index-000003"));
        assertThat(response.isDryRun(), equalTo(true));
        assertThat(response.isRolledOver(), equalTo(false));
        assertThat(response.getConditionStatus().size(), equalTo(1));
        assertThat(response.getConditionStatus().get("[max_docs: 500]"), is(false));

        // For given alias, verify that the condition evaluation is successful when condition doc count is less than the primaries doc count
        // (primaries from only write index is considered)
        future = new PlainActionFuture<>();
        rolloverRequest = new RolloverRequest("logs-alias", "logs-index-000003");
        rolloverRequest.addMaxIndexDocsCondition(300L);
        rolloverRequest.dryRun(true);
        transportRolloverAction.masterOperation(mock(Task.class), rolloverRequest, stateBefore, future);

        response = future.actionGet();
        assertThat(response.getOldIndex(), equalTo("logs-index-000002"));
        assertThat(response.getNewIndex(), equalTo("logs-index-000003"));
        assertThat(response.isDryRun(), equalTo(true));
        assertThat(response.isRolledOver(), equalTo(false));
        assertThat(response.getConditionStatus().size(), equalTo(1));
        assertThat(response.getConditionStatus().get("[max_docs: 300]"), is(true));
    }

    private IndicesStatsResponse createIndicesStatResponse(String indexName, long totalDocs, long primariesDocs) {
        final CommonStats primaryStats = mock(CommonStats.class);
        when(primaryStats.getDocs()).thenReturn(new DocsStats(primariesDocs, 0, between(1, 10000)));

        final CommonStats totalStats = mock(CommonStats.class);
        when(totalStats.getDocs()).thenReturn(new DocsStats(totalDocs, 0, between(1, 10000)));

        final IndicesStatsResponse response = mock(IndicesStatsResponse.class);
        when(response.getPrimaries()).thenReturn(primaryStats);
        when(response.getTotal()).thenReturn(totalStats);
        final IndexStats indexStats = mock(IndexStats.class);
        when(response.getIndex(indexName)).thenReturn(indexStats);
        when(indexStats.getPrimaries()).thenReturn(primaryStats);
        when(indexStats.getTotal()).thenReturn(totalStats);
        return response;
    }

    private IndicesStatsResponse createAliasToMultipleIndicesStatsResponse(Map<String, IndexStats> indexStats) {
        final IndicesStatsResponse response = mock(IndicesStatsResponse.class);
        final CommonStats primariesStats = new CommonStats();
        final CommonStats totalStats = new CommonStats();
        for (String indexName : indexStats.keySet()) {
            when(response.getIndex(indexName)).thenReturn(indexStats.get(indexName));
            primariesStats.add(indexStats.get(indexName).getPrimaries());
            totalStats.add(indexStats.get(indexName).getTotal());
        }

        when(response.getPrimaries()).thenReturn(primariesStats);
        when(response.getTotal()).thenReturn(totalStats);
        return response;
    }

    private IndexStats createIndexStats(long primaries, long total) {
        final CommonStats primariesCommonStats = mock(CommonStats.class);
        when(primariesCommonStats.getDocs()).thenReturn(new DocsStats(primaries, 0, between(1, 10000)));

        final CommonStats totalCommonStats = mock(CommonStats.class);
        when(totalCommonStats.getDocs()).thenReturn(new DocsStats(total, 0, between(1, 10000)));

        IndexStats indexStats = mock(IndexStats.class);
        when(indexStats.getPrimaries()).thenReturn(primariesCommonStats);
        when(indexStats.getTotal()).thenReturn(totalCommonStats);
        return indexStats;
    }

    private static IndexMetadata createMetadata(String indexName) {
        final Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();
        return IndexMetadata.builder(indexName)
            .creationDate(System.currentTimeMillis() - TimeValue.timeValueHours(3).getMillis())
            .settings(settings)
            .build();
    }

    private static Condition<?> createTestCondition() {
        final Condition<?> condition = mock(Condition.class);
        when(condition.evaluate(any())).thenReturn(new Condition.Result(condition, true));
        return condition;
    }

    public static IndicesStatsResponse randomIndicesStatsResponse(final IndexMetadata[] indices) {
        List<ShardStats> shardStats = new ArrayList<>();
        for (final IndexMetadata index : indices) {
            int numShards = randomIntBetween(1, 3);
            int primaryIdx = randomIntBetween(-1, numShards - 1); // -1 means there is no primary shard.
            for (int i = 0; i < numShards; i++) {
                ShardId shardId = new ShardId(index.getIndex(), i);
                boolean primary = (i == primaryIdx);
                Path path = createTempDir().resolve("indices").resolve(index.getIndexUUID()).resolve(String.valueOf(i));
                ShardRouting shardRouting = ShardRouting.newUnassigned(shardId, primary,
                    primary ? RecoverySource.EmptyStoreRecoverySource.INSTANCE : RecoverySource.PeerRecoverySource.INSTANCE,
                    new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null)
                );
                shardRouting = shardRouting.initialize("node-0", null, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
                shardRouting = shardRouting.moveToStarted();
                CommonStats stats = new CommonStats();
                stats.fieldData = new FieldDataStats();
                stats.queryCache = new QueryCacheStats();
                stats.docs = new DocsStats();
                stats.store = new StoreStats();
                stats.indexing = new IndexingStats();
                stats.search = new SearchStats();
                stats.segments = new SegmentsStats();
                stats.merge = new MergeStats();
                stats.refresh = new RefreshStats();
                stats.completion = new CompletionStats();
                stats.requestCache = new RequestCacheStats();
                stats.get = new GetStats();
                stats.flush = new FlushStats();
                stats.warmer = new WarmerStats();
                shardStats.add(new ShardStats(shardRouting, new ShardPath(false, path, path, shardId), stats, null, null, null));
            }
        }
        return IndicesStatsTests.newIndicesStatsResponse(
            shardStats.toArray(new ShardStats[shardStats.size()]), shardStats.size(), shardStats.size(), 0, emptyList()
        );
    }
}

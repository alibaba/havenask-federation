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

package org.havenask.action.admin.indices.create;

import org.havenask.action.ActionListener;
import org.havenask.action.UnavailableShardsException;
import org.havenask.action.admin.cluster.state.ClusterStateResponse;
import org.havenask.action.admin.indices.alias.Alias;
import org.havenask.action.admin.indices.mapping.get.GetMappingsResponse;
import org.havenask.action.search.SearchResponse;
import org.havenask.action.support.ActiveShardCount;
import org.havenask.action.support.IndicesOptions;
import org.havenask.action.support.master.AcknowledgedResponse;
import org.havenask.cluster.ClusterState;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.cluster.metadata.MappingMetadata;
import org.havenask.cluster.metadata.Metadata;
import org.havenask.common.collect.ImmutableOpenMap;
import org.havenask.common.settings.Settings;
import org.havenask.common.unit.TimeValue;
import org.havenask.common.xcontent.XContentFactory;
import org.havenask.index.IndexNotFoundException;
import org.havenask.index.IndexService;
import org.havenask.index.mapper.MapperParsingException;
import org.havenask.index.query.RangeQueryBuilder;
import org.havenask.indices.IndicesService;
import org.havenask.test.HavenaskIntegTestCase;
import org.havenask.test.HavenaskIntegTestCase.ClusterScope;
import org.havenask.test.HavenaskIntegTestCase.Scope;

import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

import static org.havenask.cluster.metadata.IndexMetadata.SETTING_WAIT_FOR_ACTIVE_SHARDS;
import static org.havenask.common.xcontent.XContentFactory.jsonBuilder;
import static org.havenask.test.hamcrest.HavenaskAssertions.assertAcked;
import static org.havenask.test.hamcrest.HavenaskAssertions.assertBlocked;
import static org.havenask.test.hamcrest.HavenaskAssertions.assertRequestBuilderThrows;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.core.IsNull.notNullValue;

@ClusterScope(scope = Scope.TEST)
public class CreateIndexIT extends HavenaskIntegTestCase {

    public void testCreationDateGivenFails() {
        try {
            prepareCreate("test").setSettings(Settings.builder().put(IndexMetadata.SETTING_CREATION_DATE, 4L)).get();
            fail();
        } catch (IllegalArgumentException ex) {
            assertEquals("unknown setting [index.creation_date] please check that any required plugins are installed, or check the " +
                "breaking changes documentation for removed settings", ex.getMessage());
        }
    }

    public void testCreationDateGenerated() {
        long timeBeforeRequest = System.currentTimeMillis();
        prepareCreate("test").get();
        long timeAfterRequest = System.currentTimeMillis();
        ClusterStateResponse response = client().admin().cluster().prepareState().get();
        ClusterState state = response.getState();
        assertThat(state, notNullValue());
        Metadata metadata = state.getMetadata();
        assertThat(metadata, notNullValue());
        ImmutableOpenMap<String, IndexMetadata> indices = metadata.getIndices();
        assertThat(indices, notNullValue());
        assertThat(indices.size(), equalTo(1));
        IndexMetadata index = indices.get("test");
        assertThat(index, notNullValue());
        assertThat(index.getCreationDate(), allOf(lessThanOrEqualTo(timeAfterRequest), greaterThanOrEqualTo(timeBeforeRequest)));
    }

    public void testDoubleAddMapping() throws Exception {
        try {
            prepareCreate("test")
                    .addMapping("type1", "date", "type=date")
                    .addMapping("type1", "num", "type=integer");
            fail("did not hit expected exception");
        } catch (IllegalStateException ise) {
            // expected
        }
        try {
            prepareCreate("test")
                    .addMapping("type1", new HashMap<String,Object>())
                    .addMapping("type1", new HashMap<String,Object>());
            fail("did not hit expected exception");
        } catch (IllegalStateException ise) {
            // expected
        }
        try {
            prepareCreate("test")
                    .addMapping("type1", jsonBuilder().startObject().endObject())
                    .addMapping("type1", jsonBuilder().startObject().endObject());
            fail("did not hit expected exception");
        } catch (IllegalStateException ise) {
            // expected
        }
    }

    public void testNonNestedMappings() throws Exception {
        assertAcked(prepareCreate("test")
            .addMapping("_doc", XContentFactory.jsonBuilder().startObject()
                .startObject("properties")
                    .startObject("date")
                        .field("type", "date")
                    .endObject()
                .endObject()
            .endObject()));

        GetMappingsResponse response = client().admin().indices().prepareGetMappings("test").get();

        ImmutableOpenMap<String, MappingMetadata> mappings = response.mappings().get("test");
        assertNotNull(mappings);

        MappingMetadata metadata = mappings.get("_doc");
        assertNotNull(metadata);
        assertFalse(metadata.sourceAsMap().isEmpty());
    }

    public void testEmptyNestedMappings() throws Exception {
        assertAcked(prepareCreate("test")
            .addMapping("_doc", XContentFactory.jsonBuilder().startObject().endObject()));

        GetMappingsResponse response = client().admin().indices().prepareGetMappings("test").get();

        ImmutableOpenMap<String, MappingMetadata> mappings = response.mappings().get("test");
        assertNotNull(mappings);

        MappingMetadata metadata = mappings.get("_doc");
        assertNotNull(metadata);
        assertTrue(metadata.sourceAsMap().isEmpty());
    }

    public void testMappingParamAndNestedMismatch() throws Exception {
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> prepareCreate("test")
                .addMapping("type1", XContentFactory.jsonBuilder().startObject()
                        .startObject("type2").endObject()
                    .endObject()).get());
        assertThat(e.getMessage(), startsWith("Failed to parse mapping [type1]: Root mapping definition has unsupported parameters"));
    }

    public void testEmptyMappings() throws Exception {
        assertAcked(prepareCreate("test")
            .addMapping("_doc", XContentFactory.jsonBuilder().startObject()
                .startObject("_doc").endObject()
            .endObject()));

        GetMappingsResponse response = client().admin().indices().prepareGetMappings("test").get();

        ImmutableOpenMap<String, MappingMetadata> mappings = response.mappings().get("test");
        assertNotNull(mappings);

        MappingMetadata metadata = mappings.get("_doc");
        assertNotNull(metadata);
        assertTrue(metadata.sourceAsMap().isEmpty());
    }

    public void testInvalidShardCountSettings() throws Exception {
        int value = randomIntBetween(-10, 0);
        try {
            prepareCreate("test").setSettings(Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, value)
                    .build())
            .get();
            fail("should have thrown an exception about the primary shard count");
        } catch (IllegalArgumentException e) {
            assertEquals("Failed to parse value [" + value + "] for setting [index.number_of_shards] must be >= 1", e.getMessage());
        }
        value = randomIntBetween(-10, -1);
        try {
            prepareCreate("test").setSettings(Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, value)
                    .build())
                    .get();
            fail("should have thrown an exception about the replica shard count");
        } catch (IllegalArgumentException e) {
            assertEquals("Failed to parse value [" + value + "] for setting [index.number_of_replicas] must be >= 0", e.getMessage());
        }

    }

    public void testCreateIndexWithBlocks() {
        try {
            setClusterReadOnly(true);
            assertBlocked(prepareCreate("test"));
        } finally {
            setClusterReadOnly(false);
        }
    }

    public void testCreateIndexWithMetadataBlocks() {
        assertAcked(prepareCreate("test").setSettings(Settings.builder().put(IndexMetadata.SETTING_BLOCKS_METADATA, true)));
        assertBlocked(client().admin().indices().prepareGetSettings("test"), IndexMetadata.INDEX_METADATA_BLOCK);
        disableIndexBlock("test", IndexMetadata.SETTING_BLOCKS_METADATA);
    }

    public void testUnknownSettingFails() {
        try {
            prepareCreate("test").setSettings(Settings.builder()
                .put("index.unknown.value", "this must fail")
                .build())
                .get();
            fail("should have thrown an exception about the shard count");
        } catch (IllegalArgumentException e) {
            assertEquals("unknown setting [index.unknown.value] please check that any required plugins are installed, or check the" +
                " breaking changes documentation for removed settings", e.getMessage());
        }
    }

    public void testInvalidShardCountSettingsWithoutPrefix() throws Exception {
        int value = randomIntBetween(-10, 0);
        try {
            prepareCreate("test").setSettings(Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS.substring(IndexMetadata.INDEX_SETTING_PREFIX.length()), value)
                .build())
                .get();
            fail("should have thrown an exception about the shard count");
        } catch (IllegalArgumentException e) {
            assertEquals("Failed to parse value [" + value + "] for setting [index.number_of_shards] must be >= 1", e.getMessage());
        }
        value = randomIntBetween(-10, -1);
        try {
            prepareCreate("test").setSettings(Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS.substring(IndexMetadata.INDEX_SETTING_PREFIX.length()), value)
                .build())
                .get();
            fail("should have thrown an exception about the shard count");
        } catch (IllegalArgumentException e) {
            assertEquals("Failed to parse value [" + value + "] for setting [index.number_of_replicas] must be >= 0", e.getMessage());
        }
    }

    public void testCreateAndDeleteIndexConcurrently() throws InterruptedException {
        createIndex("test");
        final AtomicInteger indexVersion = new AtomicInteger(0);
        final Object indexVersionLock = new Object();
        final CountDownLatch latch = new CountDownLatch(1);
        int numDocs = randomIntBetween(1, 10);
        for (int i = 0; i < numDocs; i++) {
            client().prepareIndex("test", "test").setSource("index_version", indexVersion.get()).get();
        }
        synchronized (indexVersionLock) { // not necessarily needed here but for completeness we lock here too
            indexVersion.incrementAndGet();
        }
        client().admin().indices().prepareDelete("test").execute(new ActionListener<AcknowledgedResponse>() { // this happens async!!!
                @Override
                public void onResponse(AcknowledgedResponse deleteIndexResponse) {
                    Thread thread = new Thread() {
                     @Override
                    public void run() {
                         try {
                             // recreate that index
                             client().prepareIndex("test", "test").setSource("index_version", indexVersion.get()).get();
                             synchronized (indexVersionLock) {
                                 // we sync here since we have to ensure that all indexing operations below for a given ID are done before
                                 // we increment the index version otherwise a doc that is in-flight could make it into an index that it
                                 // was supposed to be deleted for and our assertion fail...
                                 indexVersion.incrementAndGet();
                             }
                             // from here on all docs with index_version == 0|1 must be gone!!!! only 2 are ok;
                             assertAcked(client().admin().indices().prepareDelete("test").get());
                         } finally {
                             latch.countDown();
                         }
                     }
                    };
                    thread.start();
                }

                @Override
                public void onFailure(Exception e) {
                    throw new RuntimeException(e);
                }
            }
        );
        numDocs = randomIntBetween(100, 200);
        for (int i = 0; i < numDocs; i++) {
            try {
                synchronized (indexVersionLock) {
                    client().prepareIndex("test", "test").setSource("index_version", indexVersion.get())
                        .setTimeout(TimeValue.timeValueSeconds(10)).get();
                }
            } catch (IndexNotFoundException inf) {
                // fine
            } catch (UnavailableShardsException ex) {
                assertEquals(ex.getCause().getClass(), IndexNotFoundException.class);
                // fine we run into a delete index while retrying
            }
        }
        latch.await();
        refresh();

        // we only really assert that we never reuse segments of old indices or anything like this here and that nothing fails with
        // crazy exceptions
        SearchResponse expected = client().prepareSearch("test").setIndicesOptions(IndicesOptions.lenientExpandOpen())
            .setQuery(new RangeQueryBuilder("index_version").from(indexVersion.get(), true)).get();
        SearchResponse all = client().prepareSearch("test").setIndicesOptions(IndicesOptions.lenientExpandOpen()).get();
        assertEquals(expected + " vs. " + all, expected.getHits().getTotalHits().value, all.getHits().getTotalHits().value);
        logger.info("total: {}", expected.getHits().getTotalHits().value);
    }

    public void testRestartIndexCreationAfterFullClusterRestart() throws Exception {
        client().admin().cluster().prepareUpdateSettings().setTransientSettings(Settings.builder().put("cluster.routing.allocation.enable",
            "none")).get();
        client().admin().indices().prepareCreate("test").setWaitForActiveShards(ActiveShardCount.NONE).setSettings(indexSettings()).get();
        internalCluster().fullRestart();
        ensureGreen("test");
    }

    public void testFailureToCreateIndexCleansUpIndicesService() {
        final int numReplicas = internalCluster().numDataNodes();
        Settings settings = Settings.builder()
            .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
            .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), numReplicas)
            .build();
        assertAcked(client().admin().indices().prepareCreate("test-idx-1")
            .setSettings(settings)
            .addAlias(new Alias("alias1").writeIndex(true))
            .get());

        assertRequestBuilderThrows(client().admin().indices().prepareCreate("test-idx-2")
                .setSettings(settings)
                .addAlias(new Alias("alias1").writeIndex(true)),
            IllegalStateException.class);

        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, internalCluster().getMasterName());
        for (IndexService indexService : indicesService) {
            assertThat(indexService.index().getName(), not("test-idx-2"));
        }
    }

    /**
     * This test ensures that index creation adheres to the {@link IndexMetadata#SETTING_WAIT_FOR_ACTIVE_SHARDS}.
     */
    public void testDefaultWaitForActiveShardsUsesIndexSetting() throws Exception {
        final int numReplicas = internalCluster().numDataNodes();
        Settings settings = Settings.builder()
                                .put(SETTING_WAIT_FOR_ACTIVE_SHARDS.getKey(), Integer.toString(numReplicas))
                                .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                                .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), numReplicas)
                                .build();
        assertAcked(client().admin().indices().prepareCreate("test-idx-1").setSettings(settings).get());

        // all should fail
        settings = Settings.builder()
                       .put(settings)
                       .put(SETTING_WAIT_FOR_ACTIVE_SHARDS.getKey(), "all")
                       .build();
        assertFalse(client().admin().indices().prepareCreate("test-idx-2").setSettings(settings).setTimeout("100ms").get()
                .isShardsAcknowledged());

        // the numeric equivalent of all should also fail
        settings = Settings.builder()
                       .put(settings)
                       .put(SETTING_WAIT_FOR_ACTIVE_SHARDS.getKey(), Integer.toString(numReplicas + 1))
                       .build();
        assertFalse(client().admin().indices().prepareCreate("test-idx-3").setSettings(settings).setTimeout("100ms").get()
                .isShardsAcknowledged());
    }

    public void testInvalidPartitionSize() {
        BiFunction<Integer, Integer, Boolean> createPartitionedIndex = (shards, partitionSize) -> {
            CreateIndexResponse response;

            try {
                response = prepareCreate("test_" + shards + "_" + partitionSize)
                    .setSettings(Settings.builder()
                        .put("index.number_of_shards", shards)
                        .put("index.number_of_routing_shards", shards)
                        .put("index.routing_partition_size", partitionSize))
                    .execute().actionGet();
            } catch (IllegalStateException | IllegalArgumentException e) {
                return false;
            }

            return response.isAcknowledged();
        };

        assertFalse(createPartitionedIndex.apply(3, 6));
        assertFalse(createPartitionedIndex.apply(3, 0));
        assertFalse(createPartitionedIndex.apply(3, 3));

        assertTrue(createPartitionedIndex.apply(3, 1));
        assertTrue(createPartitionedIndex.apply(3, 2));

        assertTrue(createPartitionedIndex.apply(1, 1));
    }

    public void testIndexNameInResponse() {
        CreateIndexResponse response = prepareCreate("foo")
            .setSettings(Settings.builder().build())
            .get();

        assertEquals("Should have index name in response", "foo", response.index());
    }

}

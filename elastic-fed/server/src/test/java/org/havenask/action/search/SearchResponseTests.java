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

package org.havenask.action.search;

import org.apache.lucene.search.TotalHits;
import org.havenask.Version;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.common.Strings;
import org.havenask.common.bytes.BytesReference;
import org.havenask.common.io.stream.NamedWriteableRegistry;
import org.havenask.common.settings.Settings;
import org.havenask.common.text.Text;
import org.havenask.common.xcontent.NamedXContentRegistry;
import org.havenask.common.xcontent.ToXContent;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.common.xcontent.XContentHelper;
import org.havenask.common.xcontent.XContentParser;
import org.havenask.common.xcontent.XContentType;
import org.havenask.rest.action.search.RestSearchAction;
import org.havenask.search.SearchHit;
import org.havenask.search.SearchHits;
import org.havenask.search.SearchHitsTests;
import org.havenask.search.SearchModule;
import org.havenask.search.aggregations.AggregationsTests;
import org.havenask.search.aggregations.InternalAggregations;
import org.havenask.search.internal.InternalSearchResponse;
import org.havenask.search.profile.SearchProfileShardResults;
import org.havenask.search.profile.SearchProfileShardResultsTests;
import org.havenask.search.suggest.Suggest;
import org.havenask.search.suggest.SuggestTests;
import org.havenask.test.HavenaskTestCase;
import org.havenask.test.InternalAggregationTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonMap;
import static org.havenask.test.XContentTestUtils.insertRandomFields;
import static org.havenask.test.hamcrest.HavenaskAssertions.assertToXContentEquivalent;

public class SearchResponseTests extends HavenaskTestCase {

    private static final NamedXContentRegistry xContentRegistry;
    static {
        List<NamedXContentRegistry.Entry> namedXContents = new ArrayList<>(InternalAggregationTestCase.getDefaultNamedXContents());
        namedXContents.addAll(SuggestTests.getDefaultNamedXContents());
        xContentRegistry = new NamedXContentRegistry(namedXContents);
    }

    private final NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(
            new SearchModule(Settings.EMPTY, false, emptyList()).getNamedWriteables());
    private AggregationsTests aggregationsTests = new AggregationsTests();

    @Before
    public void init() throws Exception {
        aggregationsTests.init();
    }

    @After
    public void cleanUp() throws Exception {
        aggregationsTests.cleanUp();
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return xContentRegistry;
    }

    private SearchResponse createTestItem(ShardSearchFailure... shardSearchFailures) {
        return createTestItem(false, shardSearchFailures);
    }

    /**
     * This SearchResponse doesn't include SearchHits, Aggregations, Suggestions, ShardSearchFailures, SearchProfileShardResults
     * to make it possible to only test properties of the SearchResponse itself
     */
    private SearchResponse createMinimalTestItem() {
        return createTestItem(true);
    }

    /**
     * if minimal is set, don't include search hits, aggregations, suggest etc... to make test simpler
     */
    private SearchResponse createTestItem(boolean minimal, ShardSearchFailure... shardSearchFailures) {
        boolean timedOut = randomBoolean();
        Boolean terminatedEarly = randomBoolean() ? null : randomBoolean();
        int numReducePhases = randomIntBetween(1, 10);
        long tookInMillis = randomNonNegativeLong();
        int totalShards = randomIntBetween(1, Integer.MAX_VALUE);
        int successfulShards = randomIntBetween(0, totalShards);
        int skippedShards = randomIntBetween(0, totalShards);
        InternalSearchResponse internalSearchResponse;
        if (minimal == false) {
            SearchHits hits = SearchHitsTests.createTestItem(true, true);
            InternalAggregations aggregations = aggregationsTests.createTestInstance();
            Suggest suggest = SuggestTests.createTestItem();
            SearchProfileShardResults profileShardResults = SearchProfileShardResultsTests.createTestItem();
            internalSearchResponse = new InternalSearchResponse(hits, aggregations, suggest, profileShardResults,
                timedOut, terminatedEarly, numReducePhases);
        } else {
            internalSearchResponse = InternalSearchResponse.empty();
        }

        return new SearchResponse(internalSearchResponse, null, totalShards, successfulShards, skippedShards, tookInMillis,
            shardSearchFailures, randomBoolean() ? randomClusters() : SearchResponse.Clusters.EMPTY);
    }

    static SearchResponse.Clusters randomClusters() {
        int totalClusters = randomIntBetween(0, 10);
        int successfulClusters = randomIntBetween(0, totalClusters);
        int skippedClusters = totalClusters - successfulClusters;
        return new SearchResponse.Clusters(totalClusters, successfulClusters, skippedClusters);
    }

    /**
     * the "_shard/total/failures" section makes it impossible to directly
     * compare xContent, so we omit it here
     */
    public void testFromXContent() throws IOException {
        doFromXContentTestWithRandomFields(createTestItem(), false);
    }

    /**
     * This test adds random fields and objects to the xContent rendered out to
     * ensure we can parse it back to be forward compatible with additions to
     * the xContent. We test this with a "minimal" SearchResponse, adding random
     * fields to SearchHits, Aggregations etc... is tested in their own tests
     */
    public void testFromXContentWithRandomFields() throws IOException {
        doFromXContentTestWithRandomFields(createMinimalTestItem(), true);
    }

    private void doFromXContentTestWithRandomFields(SearchResponse response, boolean addRandomFields) throws IOException {
        XContentType xcontentType = randomFrom(XContentType.values());
        boolean humanReadable = randomBoolean();
        final ToXContent.Params params = new ToXContent.MapParams(singletonMap(RestSearchAction.TYPED_KEYS_PARAM, "true"));
        BytesReference originalBytes = toShuffledXContent(response, xcontentType, params, humanReadable);
        BytesReference mutated;
        if (addRandomFields) {
            mutated = insertRandomFields(xcontentType, originalBytes, null, random());
        } else {
            mutated = originalBytes;
        }
        try (XContentParser parser = createParser(xcontentType.xContent(), mutated)) {
            SearchResponse parsed = SearchResponse.fromXContent(parser);
            assertToXContentEquivalent(originalBytes, XContentHelper.toXContent(parsed, xcontentType, params, humanReadable), xcontentType);
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
            assertNull(parser.nextToken());
        }
    }

    /**
     * The "_shard/total/failures" section makes if impossible to directly compare xContent, because
     * the failures in the parsed SearchResponse are wrapped in an extra HavenaskException on the client side.
     * Because of this, in this special test case we compare the "top level" fields for equality
     * and the subsections xContent equivalence independently
     */
    public void testFromXContentWithFailures() throws IOException {
        int numFailures = randomIntBetween(1, 5);
        ShardSearchFailure[] failures = new ShardSearchFailure[numFailures];
        for (int i = 0; i < failures.length; i++) {
            failures[i] = ShardSearchFailureTests.createTestItem(IndexMetadata.INDEX_UUID_NA_VALUE);
        }
        SearchResponse response = createTestItem(failures);
        XContentType xcontentType = randomFrom(XContentType.values());
        final ToXContent.Params params = new ToXContent.MapParams(singletonMap(RestSearchAction.TYPED_KEYS_PARAM, "true"));
        BytesReference originalBytes = toShuffledXContent(response, xcontentType, params, randomBoolean());
        try (XContentParser parser = createParser(xcontentType.xContent(), originalBytes)) {
            SearchResponse parsed = SearchResponse.fromXContent(parser);
            for (int i = 0; i < parsed.getShardFailures().length; i++) {
                ShardSearchFailure parsedFailure = parsed.getShardFailures()[i];
                ShardSearchFailure originalFailure = failures[i];
                assertEquals(originalFailure.index(), parsedFailure.index());
                assertEquals(originalFailure.shard(), parsedFailure.shard());
                assertEquals(originalFailure.shardId(), parsedFailure.shardId());
                String originalMsg = originalFailure.getCause().getMessage();
                assertEquals(parsedFailure.getCause().getMessage(), "Havenask exception [type=parsing_exception, reason=" +
                        originalMsg + "]");
                String nestedMsg = originalFailure.getCause().getCause().getMessage();
                assertEquals(parsedFailure.getCause().getCause().getMessage(),
                        "Havenask exception [type=illegal_argument_exception, reason=" + nestedMsg + "]");
            }
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
            assertNull(parser.nextToken());
        }
    }

    public void testToXContent() {
        SearchHit hit = new SearchHit(1, "id1", new Text("type"), Collections.emptyMap(), Collections.emptyMap());
        hit.score(2.0f);
        SearchHit[] hits = new SearchHit[] { hit };
        {
            SearchResponse response = new SearchResponse(
                    new InternalSearchResponse(new SearchHits(hits, new TotalHits(100, TotalHits.Relation.EQUAL_TO), 1.5f), null, null,
                        null, false, null, 1),
                        null, 0
            , 0, 0, 0,
                    ShardSearchFailure.EMPTY_ARRAY, SearchResponse.Clusters.EMPTY);
            StringBuilder expectedString = new StringBuilder();
            expectedString.append("{");
            {
                expectedString.append("\"took\":0,");
                expectedString.append("\"timed_out\":false,");
                expectedString.append("\"_shards\":");
                {
                    expectedString.append("{\"total\":0,");
                    expectedString.append("\"successful\":0,");
                    expectedString.append("\"skipped\":0,");
                    expectedString.append("\"failed\":0},");
                }
                expectedString.append("\"hits\":");
                {
                    expectedString.append("{\"total\":{\"value\":100,\"relation\":\"eq\"},");
                    expectedString.append("\"max_score\":1.5,");
                    expectedString.append("\"hits\":[{\"_type\":\"type\",\"_id\":\"id1\",\"_score\":2.0}]}");
                }
            }
            expectedString.append("}");
            assertEquals(expectedString.toString(), Strings.toString(response));
        }
        {
            SearchResponse response = new SearchResponse(
                    new InternalSearchResponse(
                        new SearchHits(hits, new TotalHits(100, TotalHits.Relation.EQUAL_TO), 1.5f), null, null, null, false, null, 1
                    ),
                null, 0, 0, 0, 0, ShardSearchFailure.EMPTY_ARRAY,
                new SearchResponse.Clusters(5, 3, 2));
            StringBuilder expectedString = new StringBuilder();
            expectedString.append("{");
            {
                expectedString.append("\"took\":0,");
                expectedString.append("\"timed_out\":false,");
                expectedString.append("\"_shards\":");
                {
                    expectedString.append("{\"total\":0,");
                    expectedString.append("\"successful\":0,");
                    expectedString.append("\"skipped\":0,");
                    expectedString.append("\"failed\":0},");
                }
                expectedString.append("\"_clusters\":");
                {
                    expectedString.append("{\"total\":5,");
                    expectedString.append("\"successful\":3,");
                    expectedString.append("\"skipped\":2},");
                }
                expectedString.append("\"hits\":");
                {
                    expectedString.append("{\"total\":{\"value\":100,\"relation\":\"eq\"},");
                    expectedString.append("\"max_score\":1.5,");
                    expectedString.append("\"hits\":[{\"_type\":\"type\",\"_id\":\"id1\",\"_score\":2.0}]}");
                }
            }
            expectedString.append("}");
            assertEquals(expectedString.toString(), Strings.toString(response));
        }
    }

    public void testSerialization() throws IOException {
        SearchResponse searchResponse = createTestItem(false);
        SearchResponse deserialized = copyWriteable(searchResponse, namedWriteableRegistry, SearchResponse::new, Version.CURRENT);
        if (searchResponse.getHits().getTotalHits() == null) {
            assertNull(deserialized.getHits().getTotalHits());
        } else {
            assertEquals(searchResponse.getHits().getTotalHits().value, deserialized.getHits().getTotalHits().value);
            assertEquals(searchResponse.getHits().getTotalHits().relation, deserialized.getHits().getTotalHits().relation);
        }
        assertEquals(searchResponse.getHits().getHits().length, deserialized.getHits().getHits().length);
        assertEquals(searchResponse.getNumReducePhases(), deserialized.getNumReducePhases());
        assertEquals(searchResponse.getFailedShards(), deserialized.getFailedShards());
        assertEquals(searchResponse.getTotalShards(), deserialized.getTotalShards());
        assertEquals(searchResponse.getSkippedShards(), deserialized.getSkippedShards());
        assertEquals(searchResponse.getClusters(), deserialized.getClusters());
    }

    public void testToXContentEmptyClusters() throws IOException {
        SearchResponse searchResponse = new SearchResponse(InternalSearchResponse.empty(), null, 1, 1, 0, 1,
            ShardSearchFailure.EMPTY_ARRAY, SearchResponse.Clusters.EMPTY);
        SearchResponse deserialized = copyWriteable(searchResponse, namedWriteableRegistry, SearchResponse::new, Version.CURRENT);
        XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent());
        deserialized.getClusters().toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertEquals(0, Strings.toString(builder).length());
    }
}

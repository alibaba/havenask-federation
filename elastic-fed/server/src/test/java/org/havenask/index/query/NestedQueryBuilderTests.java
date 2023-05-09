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

package org.havenask.index.query;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;

import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.ScoreMode;
import org.havenask.HavenaskException;
import org.havenask.Version;
import org.havenask.action.admin.indices.mapping.put.PutMappingRequest;
import org.havenask.common.Strings;
import org.havenask.common.compress.CompressedXContent;
import org.havenask.common.settings.Settings;
import org.havenask.index.IndexSettings;
import org.havenask.index.mapper.MapperService;
import org.havenask.index.query.functionscore.FunctionScoreQueryBuilder;
import org.havenask.index.search.HavenaskToParentBlockJoinQuery;
import org.havenask.search.fetch.subphase.InnerHitsContext;
import org.havenask.search.internal.SearchContext;
import org.havenask.search.sort.FieldSortBuilder;
import org.havenask.search.sort.SortOrder;
import org.havenask.test.AbstractQueryTestCase;
import org.havenask.test.VersionUtils;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.havenask.index.IndexSettingsTests.newIndexMeta;
import static org.havenask.index.query.InnerHitBuilderTests.randomNestedInnerHits;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NestedQueryBuilderTests extends AbstractQueryTestCase<NestedQueryBuilder> {

    @Override
    protected void initializeAdditionalMappings(MapperService mapperService) throws IOException {
        mapperService.merge("_doc", new CompressedXContent(Strings.toString(PutMappingRequest.buildFromSimplifiedDef("_doc",
                TEXT_FIELD_NAME, "type=text",
                INT_FIELD_NAME, "type=integer",
                DOUBLE_FIELD_NAME, "type=double",
                BOOLEAN_FIELD_NAME, "type=boolean",
                DATE_FIELD_NAME, "type=date",
                OBJECT_FIELD_NAME, "type=object",
                GEO_POINT_FIELD_NAME, "type=geo_point",
                "nested1", "type=nested"
        ))), MapperService.MergeReason.MAPPING_UPDATE);
    }

    /**
     * @return a {@link NestedQueryBuilder} with random values all over the place
     */
    @Override
    protected NestedQueryBuilder doCreateTestQueryBuilder() {
        QueryBuilder innerQueryBuilder = RandomQueryBuilder.createQuery(random());
        NestedQueryBuilder nqb = new NestedQueryBuilder("nested1", innerQueryBuilder,
                RandomPicks.randomFrom(random(), ScoreMode.values()));
        nqb.ignoreUnmapped(randomBoolean());
        if (randomBoolean()) {
            nqb.innerHit(new InnerHitBuilder(randomAlphaOfLengthBetween(1, 10))
                    .setSize(randomIntBetween(0, 100))
                    .addSort(new FieldSortBuilder(INT_FIELD_NAME).order(SortOrder.ASC))
                    .setIgnoreUnmapped(nqb.ignoreUnmapped()));
        }
        return nqb;
    }

    @Override
    protected void doAssertLuceneQuery(NestedQueryBuilder queryBuilder, Query query, QueryShardContext context) throws IOException {
        assertThat(query, instanceOf(HavenaskToParentBlockJoinQuery.class));
        // TODO how to assert this?
        if (queryBuilder.innerHit() != null) {
            // have to rewrite again because the provided queryBuilder hasn't been rewritten (directly returned from
            // doCreateTestQueryBuilder)
            queryBuilder = (NestedQueryBuilder) queryBuilder.rewrite(context);

            assertNotNull(context);
            Map<String, InnerHitContextBuilder> innerHitInternals = new HashMap<>();
            InnerHitContextBuilder.extractInnerHits(queryBuilder, innerHitInternals);
            assertTrue(innerHitInternals.containsKey(queryBuilder.innerHit().getName()));
            InnerHitContextBuilder innerHits = innerHitInternals.get(queryBuilder.innerHit().getName());
            assertEquals(innerHits.innerHitBuilder(), queryBuilder.innerHit());
        }
    }

    /**
     * Test (de)serialization on all previous released versions
     */
    public void testSerializationBWC() throws IOException {
        for (Version version : VersionUtils.allReleasedVersions()) {
            NestedQueryBuilder testQuery = createTestQueryBuilder();
            assertSerialization(testQuery, version);
        }
    }

    public void testValidate() {
        QueryBuilder innerQuery = RandomQueryBuilder.createQuery(random());
        IllegalArgumentException e =
                expectThrows(IllegalArgumentException.class, () -> QueryBuilders.nestedQuery(null, innerQuery, ScoreMode.Avg));
        assertThat(e.getMessage(), equalTo("[nested] requires 'path' field"));

        e = expectThrows(IllegalArgumentException.class, () -> QueryBuilders.nestedQuery("foo", null, ScoreMode.Avg));
        assertThat(e.getMessage(), equalTo("[nested] requires 'query' field"));

        e = expectThrows(IllegalArgumentException.class, () -> QueryBuilders.nestedQuery("foo", innerQuery, null));
        assertThat(e.getMessage(), equalTo("[nested] requires 'score_mode' field"));
    }

    public void testFromJson() throws IOException {
        String json =
                "{\n" +
                "  \"nested\" : {\n" +
                "    \"query\" : {\n" +
                "      \"bool\" : {\n" +
                "        \"must\" : [ {\n" +
                "          \"match\" : {\n" +
                "            \"obj1.name\" : {\n" +
                "              \"query\" : \"blue\",\n" +
                "              \"operator\" : \"OR\",\n" +
                "              \"prefix_length\" : 0,\n" +
                "              \"max_expansions\" : 50,\n" +
                "              \"fuzzy_transpositions\" : true,\n" +
                "              \"lenient\" : false,\n" +
                "              \"zero_terms_query\" : \"NONE\",\n" +
                "              \"auto_generate_synonyms_phrase_query\" : true,\n" +
                "              \"boost\" : 1.0\n" +
                "            }\n" +
                "          }\n" +
                "        }, {\n" +
                "          \"range\" : {\n" +
                "            \"obj1.count\" : {\n" +
                "              \"from\" : 5,\n" +
                "              \"to\" : null,\n" +
                "              \"include_lower\" : false,\n" +
                "              \"include_upper\" : true,\n" +
                "              \"boost\" : 1.0\n" +
                "            }\n" +
                "          }\n" +
                "        } ],\n" +
                "        \"adjust_pure_negative\" : true,\n" +
                "        \"boost\" : 1.0\n" +
                "      }\n" +
                "    },\n" +
                "    \"path\" : \"obj1\",\n" +
                "    \"ignore_unmapped\" : false,\n" +
                "    \"score_mode\" : \"avg\",\n" +
                "    \"boost\" : 1.0\n" +
                "  }\n" +
                "}";

        NestedQueryBuilder parsed = (NestedQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);

        assertEquals(json, ScoreMode.Avg, parsed.scoreMode());
    }

    @Override
    public void testMustRewrite() throws IOException {
        QueryShardContext context = createShardContext();
        context.setAllowUnmappedFields(true);
        TermQueryBuilder innerQueryBuilder = new TermQueryBuilder("nested1.unmapped_field", "foo");
        NestedQueryBuilder nestedQueryBuilder = new NestedQueryBuilder("nested1", innerQueryBuilder,
                RandomPicks.randomFrom(random(), ScoreMode.values()));
        IllegalStateException e = expectThrows(IllegalStateException.class,
                () -> nestedQueryBuilder.toQuery(context));
        assertEquals("Rewrite first", e.getMessage());
    }

    public void testIgnoreUnmapped() throws IOException {
        final NestedQueryBuilder queryBuilder = new NestedQueryBuilder("unmapped", new MatchAllQueryBuilder(), ScoreMode.None);
        queryBuilder.ignoreUnmapped(true);
        Query query = queryBuilder.toQuery(createShardContext());
        assertThat(query, notNullValue());
        assertThat(query, instanceOf(MatchNoDocsQuery.class));

        final NestedQueryBuilder failingQueryBuilder = new NestedQueryBuilder("unmapped", new MatchAllQueryBuilder(), ScoreMode.None);
        failingQueryBuilder.ignoreUnmapped(false);
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> failingQueryBuilder.toQuery(createShardContext()));
        assertThat(e.getMessage(), containsString("[" + NestedQueryBuilder.NAME + "] failed to find nested object under path [unmapped]"));
    }

    public void testIgnoreUnmappedWithRewrite() throws IOException {
        // WrapperQueryBuilder makes sure we always rewrite
        final NestedQueryBuilder queryBuilder =
            new NestedQueryBuilder("unmapped", new WrapperQueryBuilder(new MatchAllQueryBuilder().toString()), ScoreMode.None);
        queryBuilder.ignoreUnmapped(true);
        QueryShardContext queryShardContext = createShardContext();
        Query query = queryBuilder.rewrite(queryShardContext).toQuery(queryShardContext);
        assertThat(query, notNullValue());
        assertThat(query, instanceOf(MatchNoDocsQuery.class));
    }

    public void testMinFromString() {
        assertThat("fromString(min) != MIN", ScoreMode.Min, equalTo(NestedQueryBuilder.parseScoreMode("min")));
        assertThat("min", equalTo(NestedQueryBuilder.scoreModeAsString(ScoreMode.Min)));
    }

    public void testMaxFromString() {
        assertThat("fromString(max) != MAX", ScoreMode.Max, equalTo(NestedQueryBuilder.parseScoreMode("max")));
        assertThat("max", equalTo(NestedQueryBuilder.scoreModeAsString(ScoreMode.Max)));
    }

    public void testAvgFromString() {
        assertThat("fromString(avg) != AVG", ScoreMode.Avg, equalTo(NestedQueryBuilder.parseScoreMode("avg")));
        assertThat("avg", equalTo(NestedQueryBuilder.scoreModeAsString(ScoreMode.Avg)));
    }

    public void testSumFromString() {
        assertThat("fromString(total) != SUM", ScoreMode.Total, equalTo(NestedQueryBuilder.parseScoreMode("sum")));
        assertThat("sum", equalTo(NestedQueryBuilder.scoreModeAsString(ScoreMode.Total)));
    }

    public void testNoneFromString() {
        assertThat("fromString(none) != NONE", ScoreMode.None, equalTo(NestedQueryBuilder.parseScoreMode("none")));
        assertThat("none", equalTo(NestedQueryBuilder.scoreModeAsString(ScoreMode.None)));
    }

    /**
     * Should throw {@link IllegalArgumentException} instead of NPE.
     */
    public void testThatNullFromStringThrowsException() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> NestedQueryBuilder.parseScoreMode(null));
        assertEquals("No score mode for child query [null] found", e.getMessage());
    }

    /**
     * Failure should not change (and the value should never match anything...).
     */
    public void testThatUnrecognizedFromStringThrowsException() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> NestedQueryBuilder.parseScoreMode("unrecognized value"));
        assertEquals("No score mode for child query [unrecognized value] found", e.getMessage());
    }

    public void testInlineLeafInnerHitsNestedQuery() throws Exception {
        InnerHitBuilder leafInnerHits = randomNestedInnerHits();
        NestedQueryBuilder nestedQueryBuilder = new NestedQueryBuilder("path", new MatchAllQueryBuilder(), ScoreMode.None);
        nestedQueryBuilder.innerHit(leafInnerHits);
        Map<String, InnerHitContextBuilder> innerHitBuilders = new HashMap<>();
        nestedQueryBuilder.extractInnerHitBuilders(innerHitBuilders);
        assertThat(innerHitBuilders.get(leafInnerHits.getName()), Matchers.notNullValue());
    }

    public void testInlineLeafInnerHitsNestedQueryViaBoolQuery() {
        InnerHitBuilder leafInnerHits = randomNestedInnerHits();
        NestedQueryBuilder nestedQueryBuilder = new NestedQueryBuilder("path", new MatchAllQueryBuilder(), ScoreMode.None)
            .innerHit(leafInnerHits);
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder().should(nestedQueryBuilder);
        Map<String, InnerHitContextBuilder> innerHitBuilders = new HashMap<>();
        boolQueryBuilder.extractInnerHitBuilders(innerHitBuilders);
        assertThat(innerHitBuilders.get(leafInnerHits.getName()), Matchers.notNullValue());
    }

    public void testInlineLeafInnerHitsNestedQueryViaConstantScoreQuery() {
        InnerHitBuilder leafInnerHits = randomNestedInnerHits();
        NestedQueryBuilder nestedQueryBuilder = new NestedQueryBuilder("path", new MatchAllQueryBuilder(), ScoreMode.None)
            .innerHit(leafInnerHits);
        ConstantScoreQueryBuilder constantScoreQueryBuilder = new ConstantScoreQueryBuilder(nestedQueryBuilder);
        Map<String, InnerHitContextBuilder> innerHitBuilders = new HashMap<>();
        constantScoreQueryBuilder.extractInnerHitBuilders(innerHitBuilders);
        assertThat(innerHitBuilders.get(leafInnerHits.getName()), Matchers.notNullValue());
    }

    public void testInlineLeafInnerHitsNestedQueryViaBoostingQuery() {
        InnerHitBuilder leafInnerHits1 = randomNestedInnerHits();
        NestedQueryBuilder nestedQueryBuilder1 = new NestedQueryBuilder("path", new MatchAllQueryBuilder(), ScoreMode.None)
            .innerHit(leafInnerHits1);
        InnerHitBuilder leafInnerHits2 = randomNestedInnerHits();
        NestedQueryBuilder nestedQueryBuilder2 = new NestedQueryBuilder("path", new MatchAllQueryBuilder(), ScoreMode.None)
            .innerHit(leafInnerHits2);
        BoostingQueryBuilder constantScoreQueryBuilder = new BoostingQueryBuilder(nestedQueryBuilder1, nestedQueryBuilder2);
        Map<String, InnerHitContextBuilder> innerHitBuilders = new HashMap<>();
        constantScoreQueryBuilder.extractInnerHitBuilders(innerHitBuilders);
        assertThat(innerHitBuilders.get(leafInnerHits1.getName()), Matchers.notNullValue());
        assertThat(innerHitBuilders.get(leafInnerHits2.getName()), Matchers.notNullValue());
    }

    public void testInlineLeafInnerHitsNestedQueryViaFunctionScoreQuery() {
        InnerHitBuilder leafInnerHits = randomNestedInnerHits();
        NestedQueryBuilder nestedQueryBuilder = new NestedQueryBuilder("path", new MatchAllQueryBuilder(), ScoreMode.None)
            .innerHit(leafInnerHits);
        FunctionScoreQueryBuilder functionScoreQueryBuilder = new FunctionScoreQueryBuilder(nestedQueryBuilder);
        Map<String, InnerHitContextBuilder> innerHitBuilders = new HashMap<>();
        ((AbstractQueryBuilder<?>) functionScoreQueryBuilder).extractInnerHitBuilders(innerHitBuilders);
        assertThat(innerHitBuilders.get(leafInnerHits.getName()), Matchers.notNullValue());
    }

    public void testBuildIgnoreUnmappedNestQuery() throws Exception {
        QueryShardContext queryShardContext = mock(QueryShardContext.class);
        when(queryShardContext.getObjectMapper("path")).thenReturn(null);
        SearchContext searchContext = mock(SearchContext.class);
        when(searchContext.getQueryShardContext()).thenReturn(queryShardContext);

        MapperService mapperService = mock(MapperService.class);
        IndexSettings settings = new IndexSettings(newIndexMeta("index", Settings.EMPTY), Settings.EMPTY);
        when(mapperService.getIndexSettings()).thenReturn(settings);
        when(searchContext.mapperService()).thenReturn(mapperService);

        InnerHitBuilder leafInnerHits = randomNestedInnerHits();
        NestedQueryBuilder query1 = new NestedQueryBuilder("path", new MatchAllQueryBuilder(), ScoreMode.None);
        query1.innerHit(leafInnerHits);
        final Map<String, InnerHitContextBuilder> innerHitBuilders = new HashMap<>();
        final InnerHitsContext innerHitsContext = new InnerHitsContext();
        expectThrows(IllegalStateException.class, () -> {
            query1.extractInnerHitBuilders(innerHitBuilders);
            assertThat(innerHitBuilders.size(), Matchers.equalTo(1));
            assertTrue(innerHitBuilders.containsKey(leafInnerHits.getName()));
            innerHitBuilders.get(leafInnerHits.getName()).build(searchContext, innerHitsContext);
        });
        innerHitBuilders.clear();
        NestedQueryBuilder query2 = new NestedQueryBuilder("path", new MatchAllQueryBuilder(), ScoreMode.None);
        query2.ignoreUnmapped(true);
        query2.innerHit(leafInnerHits);
        query2.extractInnerHitBuilders(innerHitBuilders);
        assertThat(innerHitBuilders.size(), Matchers.equalTo(1));
        assertTrue(innerHitBuilders.containsKey(leafInnerHits.getName()));
        assertThat(innerHitBuilders.get(leafInnerHits.getName()), instanceOf(NestedQueryBuilder.NestedInnerHitContextBuilder.class));
        NestedQueryBuilder.NestedInnerHitContextBuilder nestedContextBuilder =
            (NestedQueryBuilder.NestedInnerHitContextBuilder) innerHitBuilders.get(leafInnerHits.getName());
        nestedContextBuilder.build(searchContext, innerHitsContext);
        assertThat(innerHitsContext.getInnerHits().size(), Matchers.equalTo(0));
    }

    public void testExtractInnerHitBuildersWithDuplicate() {
        final NestedQueryBuilder queryBuilder
            = new NestedQueryBuilder("path", new WrapperQueryBuilder(new MatchAllQueryBuilder().toString()), ScoreMode.None);
        queryBuilder.innerHit(new InnerHitBuilder("some_name"));
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> InnerHitContextBuilder.extractInnerHits(queryBuilder,Collections.singletonMap("some_name", null)));
        assertEquals("[inner_hits] already contains an entry for key [some_name]", e.getMessage());
    }

    public void testDisallowExpensiveQueries() {
        QueryShardContext queryShardContext = mock(QueryShardContext.class);
        when(queryShardContext.allowExpensiveQueries()).thenReturn(false);

        NestedQueryBuilder queryBuilder = new NestedQueryBuilder("path", new MatchAllQueryBuilder(), ScoreMode.None);
        HavenaskException e = expectThrows(HavenaskException.class,
                () -> queryBuilder.toQuery(queryShardContext));
        assertEquals("[joining] queries cannot be executed when 'search.allow_expensive_queries' is set to false.",
                e.getMessage());
    }
}

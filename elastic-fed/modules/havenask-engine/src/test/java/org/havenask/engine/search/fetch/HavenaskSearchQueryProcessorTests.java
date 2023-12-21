/*
 * Copyright (c) 2021, Alibaba Group;
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.havenask.engine.search.fetch;

import org.havenask.common.collect.List;
import org.havenask.engine.index.query.KnnQueryBuilder;
import org.havenask.engine.rpc.QrsClient;
import org.havenask.engine.search.HavenaskSearchQueryProcessor;
import org.havenask.index.query.QueryBuilders;
import org.havenask.search.builder.KnnSearchBuilder;
import org.havenask.search.builder.SearchSourceBuilder;
import org.havenask.test.HavenaskTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;

public class HavenaskSearchQueryProcessorTests extends HavenaskTestCase {
    private QrsClient qrsClient = mock(QrsClient.class);
    private Map<String, Object> indexMapping = new HashMap<>();

    @Before
    public void setup() {
        Map<String, Object> propertiesMapping = new HashMap<>();
        Map<String, Object> fieldMapping = new HashMap<>();
        Map<String, Object> field1Mapping = new HashMap<>();
        Map<String, Object> field2Mapping = new HashMap<>();
        fieldMapping.put("similarity", "L2_NORM");
        field1Mapping.put("similarity", "L2_NORM");
        field2Mapping.put("similarity", "DOT_PRODUCT");
        propertiesMapping.put("field", fieldMapping);
        propertiesMapping.put("field1", field1Mapping);
        propertiesMapping.put("field2", field2Mapping);
        indexMapping.put("properties", propertiesMapping);
    }

    public void testMatchAllDocsQuery() throws IOException {
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders.matchAllQuery());

        HavenaskSearchQueryProcessor havenaskSearchQueryProcessor = new HavenaskSearchQueryProcessor(qrsClient);
        String sql = havenaskSearchQueryProcessor.transferSearchRequest2HavenaskSql("table", builder, null);
        assertEquals("select _id from `table` limit 10 offset 0", sql);
    }

    public void testProximaQuery() throws IOException {
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(new KnnQueryBuilder("field", new float[] { 1.0f, 2.0f }, 20));

        HavenaskSearchQueryProcessor havenaskSearchQueryProcessor = new HavenaskSearchQueryProcessor(qrsClient);
        String sql = havenaskSearchQueryProcessor.transferSearchRequest2HavenaskSql("table", builder, indexMapping);
        assertEquals(
            "select _id, (1/(1+vector_score('field'))) as _score from `table` where "
                + "MATCHINDEX('field', '1.0,2.0&n=20') order by _score desc limit 10 offset 0",
            sql
        );
    }

    public void testUnsupportedDSL() {
        try {
            SearchSourceBuilder builder = new SearchSourceBuilder();
            builder.query(QueryBuilders.existsQuery("field"));
            HavenaskSearchQueryProcessor havenaskSearchQueryProcessor = new HavenaskSearchQueryProcessor(qrsClient);
            havenaskSearchQueryProcessor.transferSearchRequest2HavenaskSql("table", builder, null);
            fail();
        } catch (IOException e) {
            assertEquals(e.getMessage(), "unsupported DSL: {\"query\":{\"exists\":{\"field\":\"field\",\"boost\":1.0}}}");
        }
    }

    public void testMatchQuery() throws IOException {
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders.matchQuery("field", "value"));
        HavenaskSearchQueryProcessor havenaskSearchQueryProcessor = new HavenaskSearchQueryProcessor(qrsClient);
        String sql = havenaskSearchQueryProcessor.transferSearchRequest2HavenaskSql("table", builder, null);
        assertEquals("select _id from `table` where MATCHINDEX('field', 'value') limit 10 offset 0", sql);
    }

    public void testLimit() throws IOException {
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders.matchAllQuery());
        builder.from(10);
        builder.size(10);
        HavenaskSearchQueryProcessor havenaskSearchQueryProcessor = new HavenaskSearchQueryProcessor(qrsClient);
        String sql = havenaskSearchQueryProcessor.transferSearchRequest2HavenaskSql("table", builder, null);
        assertEquals("select _id from `table` limit 10 offset 10", sql);
    }

    public void testNoFrom() throws IOException {
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders.matchAllQuery());
        builder.size(10);
        HavenaskSearchQueryProcessor havenaskSearchQueryProcessor = new HavenaskSearchQueryProcessor(qrsClient);
        String sql = havenaskSearchQueryProcessor.transferSearchRequest2HavenaskSql("table", builder, null);
        assertEquals(sql, "select _id from `table` limit 10 offset 0");
    }

    public void testNoSize() throws IOException {
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders.matchAllQuery());
        builder.from(10);
        HavenaskSearchQueryProcessor havenaskSearchQueryProcessor = new HavenaskSearchQueryProcessor(qrsClient);
        String sql = havenaskSearchQueryProcessor.transferSearchRequest2HavenaskSql("table", builder, null);
        assertEquals(sql, "select _id from `table` limit 10 offset 10");
    }

    // test knn dsl
    public void testKnnDsl() throws IOException {
        SearchSourceBuilder l2NormBuilder = new SearchSourceBuilder();
        l2NormBuilder.query(QueryBuilders.matchAllQuery());
        l2NormBuilder.knnSearch(List.of(new KnnSearchBuilder("field1", new float[] { 1.0f, 2.0f }, 20, 20, null)));
        HavenaskSearchQueryProcessor havenaskSearchQueryProcessor = new HavenaskSearchQueryProcessor(qrsClient);
        String l2NormSql = havenaskSearchQueryProcessor.transferSearchRequest2HavenaskSql("table", l2NormBuilder, indexMapping);
        assertEquals(
            "select _id, ((1/(1+vector_score('field1')))) as _score from `table` "
                + "where MATCHINDEX('field1', '1.0,2.0&n=20') order by _score desc limit 10 offset 0",
            l2NormSql
        );

        SearchSourceBuilder dotProductBuilder = new SearchSourceBuilder();
        dotProductBuilder.query(QueryBuilders.matchAllQuery());
        dotProductBuilder.knnSearch(List.of(new KnnSearchBuilder("field2", new float[] { 0.6f, 0.8f }, 20, 20, null)));
        String dotProductSql = havenaskSearchQueryProcessor.transferSearchRequest2HavenaskSql("table", dotProductBuilder, indexMapping);
        assertEquals(
            "select _id, (((1+vector_score('field2'))/2)) as _score from `table` "
                + "where MATCHINDEX('field2', '0.6,0.8&n=20') order by _score desc limit 10 offset 0",
            dotProductSql
        );
    }

    // test multi knn dsl
    public void testMultiKnnDsl() throws IOException {
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders.matchAllQuery());
        builder.knnSearch(
            List.of(
                new KnnSearchBuilder("field1", new float[] { 1.0f, 2.0f }, 20, 20, null),
                new KnnSearchBuilder("field2", new float[] { 0.6f, 0.8f }, 10, 10, null)
            )
        );
        HavenaskSearchQueryProcessor havenaskSearchQueryProcessor = new HavenaskSearchQueryProcessor(qrsClient);
        String sql = havenaskSearchQueryProcessor.transferSearchRequest2HavenaskSql("table", builder, indexMapping);
        assertEquals(
            "select _id, ((1/(1+vector_score('field1'))) + ((1+vector_score('field2'))/2)) as _score from `table` "
                + "where MATCHINDEX('field1', '1.0,2.0&n=20') or MATCHINDEX('field2', '0.6,0.8&n=10') "
                + "order by _score desc limit 10 offset 0",
            sql
        );
    }

    public void testIllegalKnnParams() throws IOException {
        SearchSourceBuilder dotProductBuilder = new SearchSourceBuilder();
        dotProductBuilder.query(QueryBuilders.matchAllQuery());
        dotProductBuilder.knnSearch(List.of(new KnnSearchBuilder("field2", new float[] { 1.0f, 2.0f }, 20, 20, null)));
        try {
            HavenaskSearchQueryProcessor havenaskSearchQueryProcessor = new HavenaskSearchQueryProcessor(qrsClient);
            havenaskSearchQueryProcessor.transferSearchRequest2HavenaskSql("table", dotProductBuilder, indexMapping);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertEquals("The [dot_product] similarity can only be used with unit-length vectors.", e.getMessage());
        }
    }

    // test unsupported knn dsl
    public void testUnsupportedKnnDsl() {
        try {
            SearchSourceBuilder builder = new SearchSourceBuilder();
            builder.query(QueryBuilders.matchAllQuery());
            builder.knnSearch(List.of(new KnnSearchBuilder("field", new float[] { 1.0f, 2.0f }, 20, 20, 1.0f)));
            HavenaskSearchQueryProcessor havenaskSearchQueryProcessor = new HavenaskSearchQueryProcessor(qrsClient);
            havenaskSearchQueryProcessor.transferSearchRequest2HavenaskSql("table", builder, indexMapping);
            fail();
        } catch (IOException e) {
            assertEquals(
                e.getMessage(),
                "unsupported knn parameter: {\"query\":{\"match_all\":{\"boost\":1.0}},"
                    + "\"knn\":[{\"field\":\"field\",\"k\":20,\"num_candidates\":20,\"query_vector\":[1.0,2.0],"
                    + "\"similarity\":1.0}]}"
            );
        }

        // unsupported getFilterQueries
        try {
            SearchSourceBuilder builder = new SearchSourceBuilder();
            builder.query(QueryBuilders.matchAllQuery());
            KnnSearchBuilder knnSearchBuilder = new KnnSearchBuilder("field", new float[] { 1.0f, 2.0f }, 20, 20, null);
            knnSearchBuilder.addFilterQuery(QueryBuilders.matchAllQuery());
            builder.knnSearch(List.of(knnSearchBuilder));
            HavenaskSearchQueryProcessor havenaskSearchQueryProcessor = new HavenaskSearchQueryProcessor(qrsClient);
            havenaskSearchQueryProcessor.transferSearchRequest2HavenaskSql("table", builder, indexMapping);
            fail();
        } catch (IOException e) {
            assertEquals(
                e.getMessage(),
                "unsupported knn parameter: {\"query\":{\"match_all\":{\"boost\":1.0}},"
                    + "\"knn\":[{\"field\":\"field\",\"k\":20,\"num_candidates\":20,\"query_vector\":[1.0,2.0],"
                    + "\"filter\":[{\"match_all\":{\"boost\":1.0}}]}]}"
            );
        }
    }
}
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

package org.havenask.engine.index.engine;

import org.havenask.common.collect.List;
import org.havenask.common.settings.Settings;
import org.havenask.engine.HavenaskEnginePlugin;
import org.havenask.engine.index.mapper.DenseVectorFieldMapper;
import org.havenask.engine.index.query.KnnQueryBuilder;
import org.havenask.index.mapper.MapperService;
import org.havenask.index.mapper.MapperServiceTestCase;
import org.havenask.index.query.QueryBuilders;
import org.havenask.plugins.Plugin;
import org.havenask.search.builder.KnnSearchBuilder;
import org.havenask.search.builder.SearchSourceBuilder;
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;

import static java.util.Collections.singletonList;

public class QueryTransformerTests extends MapperServiceTestCase {
    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return singletonList(new HavenaskEnginePlugin(Settings.EMPTY));
    }

    private MapperService mapperService;
    // mock
    private SearchSourceBuilder builder = new SearchSourceBuilder();

    @Before
    public void setup() throws IOException {
        mapperService = createMapperService(mapping(b -> {
            {
                b.startObject("field");
                {
                    b.field("type", DenseVectorFieldMapper.CONTENT_TYPE);
                    b.field("dims", 2);
                    b.field("similarity", "l2_norm");
                }
                b.endObject();
                b.startObject("field1");
                {
                    b.field("type", DenseVectorFieldMapper.CONTENT_TYPE);
                    b.field("dims", 2);
                    b.field("similarity", "l2_norm");
                }
                b.endObject();
                b.startObject("field2");
                {
                    b.field("type", DenseVectorFieldMapper.CONTENT_TYPE);
                    b.field("dims", 2);
                    b.field("similarity", "dot_product");
                }
                b.endObject();
            }
        }));
    }

    public void testMatchAllDocsQuery() throws IOException {
        builder.query(QueryBuilders.matchAllQuery());
        String sql = QueryTransformer.toSql("table", builder, mapperService);
        assertEquals(sql, "select _id from table");
    }

    public void testProximaQuery() throws IOException {
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(new KnnQueryBuilder("field", new float[] { 1.0f, 2.0f }, 20));
        String sql = QueryTransformer.toSql("table", builder, mapperService);
        assertEquals(
            "select _id, (1/(1+vector_score('field'))) as _score from table where MATCHINDEX('field', '1.0,2.0&n=20') order by _score desc",
            sql
        );
    }

    public void testUnsupportedDSL() {
        try {
            SearchSourceBuilder builder = new SearchSourceBuilder();
            builder.query(QueryBuilders.existsQuery("field"));
            QueryTransformer.toSql("table", builder, mapperService);
            fail();
        } catch (IOException e) {
            assertEquals(e.getMessage(), "unsupported DSL: {\"query\":{\"exists\":{\"field\":\"field\",\"boost\":1.0}}}");
        }
    }

    // test term query
    public void testTermQuery() throws IOException {
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders.termQuery("field", "value"));
        String sql = QueryTransformer.toSql("table", builder, mapperService);
        assertEquals(sql, "select _id from table where field='value'");
    }

    // test match query
    public void testMatchQuery() throws IOException {
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders.matchQuery("field", "value"));
        String sql = QueryTransformer.toSql("table", builder, mapperService);
        assertEquals(sql, "select _id from table where MATCHINDEX('field', 'value')");
    }

    // test limit
    public void testLimit() throws IOException {
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders.matchAllQuery());
        builder.from(10);
        builder.size(10);
        String sql = QueryTransformer.toSql("table", builder, mapperService);
        assertEquals("select _id from table limit 20", sql);
    }

    // test no from
    public void testNoFrom() throws IOException {
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders.matchAllQuery());
        builder.size(10);
        String sql = QueryTransformer.toSql("table", builder, mapperService);
        assertEquals(sql, "select _id from table limit 10");
    }

    // test no size
    public void testNoSize() throws IOException {
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders.matchAllQuery());
        builder.from(10);
        String sql = QueryTransformer.toSql("table", builder, mapperService);
        assertEquals(sql, "select _id from table");
    }

    // test knn dsl
    public void testKnnDsl() throws IOException {
        SearchSourceBuilder l2NormBuilder = new SearchSourceBuilder();
        l2NormBuilder.query(QueryBuilders.matchAllQuery());
        l2NormBuilder.knnSearch(List.of(new KnnSearchBuilder("field1", new float[] { 1.0f, 2.0f }, 20, 20, null)));
        String l2NormSql = QueryTransformer.toSql("table", l2NormBuilder, mapperService);
        assertEquals(
            "select _id, ((1/(1+vector_score('field1')))) as _score from table "
                + "where MATCHINDEX('field1', '1.0,2.0&n=20') order by _score desc",
            l2NormSql
        );

        SearchSourceBuilder dotProductBuilder = new SearchSourceBuilder();
        dotProductBuilder.query(QueryBuilders.matchAllQuery());
        dotProductBuilder.knnSearch(List.of(new KnnSearchBuilder("field2", new float[] { 0.6f, 0.8f }, 20, 20, null)));
        String dotProductSql = QueryTransformer.toSql("table", dotProductBuilder, mapperService);
        assertEquals(
            "select _id, (((1+vector_score('field2'))/2)) as _score from table "
                + "where MATCHINDEX('field2', '0.6,0.8&n=20') order by _score desc",
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
        String sql = QueryTransformer.toSql("table", builder, mapperService);
        assertEquals(
            "select _id, ((1/(1+vector_score('field1'))) + ((1+vector_score('field2'))/2)) as _score from table "
                + "where MATCHINDEX('field1', '1.0,2.0&n=20') or MATCHINDEX('field2', '0.6,0.8&n=10') order by _score desc",
            sql
        );
    }

    public void testIllegalKnnParams() throws IOException {
        SearchSourceBuilder dotProductBuilder = new SearchSourceBuilder();
        dotProductBuilder.query(QueryBuilders.matchAllQuery());
        dotProductBuilder.knnSearch(List.of(new KnnSearchBuilder("field2", new float[] { 1.0f, 2.0f }, 20, 20, null)));
        try {
            String dotProductSql = QueryTransformer.toSql("table", dotProductBuilder, mapperService);
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
            QueryTransformer.toSql("table", builder, mapperService);
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
            QueryTransformer.toSql("table", builder, mapperService);
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

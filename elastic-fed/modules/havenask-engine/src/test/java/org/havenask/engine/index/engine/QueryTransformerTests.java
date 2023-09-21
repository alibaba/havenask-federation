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

import java.io.IOException;

import org.havenask.common.collect.List;
import org.havenask.engine.index.query.HnswQueryBuilder;
import org.havenask.engine.index.query.KnnSearchBuilder;
import org.havenask.index.query.QueryBuilders;
import org.havenask.search.builder.SearchSourceBuilder;
import org.havenask.test.HavenaskTestCase;

public class QueryTransformerTests extends HavenaskTestCase {
    public void testMatchAllDocsQuery() throws IOException {
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders.matchAllQuery());
        String sql = QueryTransformer.toSql("table", builder);
        assertEquals(sql, "select _id from table");
    }

    public void testProximaQuery() throws IOException {
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(new HnswQueryBuilder("field", new float[] { 1.0f, 2.0f }, 20));
        String sql = QueryTransformer.toSql("table", builder);
        assertEquals(sql, "select _id from table where MATCHINDEX('field', '1.0,2.0&n=20')");
    }

    public void testUnsupportedDSL() {
        try {
            SearchSourceBuilder builder = new SearchSourceBuilder();
            builder.query(QueryBuilders.existsQuery("field"));
            QueryTransformer.toSql("table", builder);
            fail();
        } catch (IOException e) {
            assertEquals(e.getMessage(), "unsupported DSL: {\"query\":{\"exists\":{\"field\":\"field\",\"boost\":1.0}}}");
        }
    }

    // test term query
    public void testTermQuery() throws IOException {
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders.termQuery("field", "value"));
        String sql = QueryTransformer.toSql("table", builder);
        assertEquals(sql, "select _id from table where field='value'");
    }

    // test match query
    public void testMatchQuery() throws IOException {
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders.matchQuery("field", "value"));
        String sql = QueryTransformer.toSql("table", builder);
        assertEquals(sql, "select _id from table where MATCHINDEX('field', 'value')");
    }

    // test limit
    public void testLimit() throws IOException {
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders.matchAllQuery());
        builder.from(10);
        builder.size(10);
        String sql = QueryTransformer.toSql("table", builder);
        assertEquals(sql, "select _id from table limit 20");
    }

    // test no from
    public void testNoFrom() throws IOException {
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders.matchAllQuery());
        builder.size(10);
        String sql = QueryTransformer.toSql("table", builder);
        assertEquals(sql, "select _id from table limit 10");
    }

    // test no size
    public void testNoSize() throws IOException {
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders.matchAllQuery());
        builder.from(10);
        String sql = QueryTransformer.toSql("table", builder);
        assertEquals(sql, "select _id from table");
    }

    // test knn dsl
    public void testKnnDsl() throws IOException {
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders.matchAllQuery());
        builder.ext(List.of(new KnnSearchBuilder("field", new float[] { 1.0f, 2.0f }, 20, 20, null)));
        String sql = QueryTransformer.toSql("table", builder);
        assertEquals(sql, "select _id from table where MATCHINDEX('field', '1.0,2.0&n=20')");
    }

    /**
     * public class QueryTransformer {
     *     public static String toSql(String table, SearchSourceBuilder dsl) throws IOException {
     *         StringBuilder sqlQuery = new StringBuilder();
     *         sqlQuery.append("select _id from " + table);
     *         QueryBuilder queryBuilder = dsl.query();
     *         StringBuilder where = new StringBuilder();
     *         if (dsl.ext().size() > 0) {
     *             for (SearchExtBuilder ext : dsl.ext()) {
     *                 if (ext instanceof KnnSearchBuilder) {
     *                     KnnSearchBuilder knnSearchBuilder = (KnnSearchBuilder) ext;
     *                     if (knnSearchBuilder.getFilterQueries().size() > 0 || knnSearchBuilder.getSimilarity() !=
     *                     null) {
     *                         throw new IOException("unsupported knn parameter: " + dsl);
     *                     }
     *
     *                     where.append(" where MATCHINDEX('" + knnSearchBuilder.getField() + "', '");
     *                     for (int i = 0; i < knnSearchBuilder.getQueryVector().length; i++) {
     *                         where.append(knnSearchBuilder.getQueryVector()[i]);
     *                         if (i < knnSearchBuilder.getQueryVector().length - 1) {
     *                             where.append(",");
     *                         }
     *                     }
     *                     where.append("&n=" + knnSearchBuilder.k() + "')");
     *                     break;
     *                 }
     *             }
     *         } else if (queryBuilder != null) {
     *             if (queryBuilder instanceof MatchAllQueryBuilder) {
     *
     *             } else if (queryBuilder instanceof ProximaQueryBuilder) {
     *                 ProximaQueryBuilder<?> proximaQueryBuilder = (ProximaQueryBuilder<?>) queryBuilder;
     *                 where.append(" where MATCHINDEX('" + proximaQueryBuilder.getFieldName() + "', '");
     *                 for (int i = 0; i < proximaQueryBuilder.getVector().length; i++) {
     *                     where.append(proximaQueryBuilder.getVector()[i]);
     *                     if (i < proximaQueryBuilder.getVector().length - 1) {
     *                         where.append(",");
     *                     }
     *                 }
     *                 where.append("&n=" + proximaQueryBuilder.getSize() + "')");
     *             } else if (queryBuilder instanceof TermQueryBuilder) {
     *                 TermQueryBuilder termQueryBuilder = (TermQueryBuilder) queryBuilder;
     *                 where.append(" where " + termQueryBuilder.fieldName() + "='" + termQueryBuilder.value() + "'");
     *             } else if (queryBuilder instanceof MatchQueryBuilder) {
     *                 MatchQueryBuilder matchQueryBuilder = (MatchQueryBuilder) queryBuilder;
     *                 where.append(" where MATCHINDEX('" + matchQueryBuilder.fieldName() + "', '" +
     *                 matchQueryBuilder.value() + "')");
     *             } else {
     *                 // TODO reject unsupported DSL
     *                 throw new IOException("unsupported DSL: " + dsl);
     *             }
     *         }
     *         sqlQuery.append(where);
     *         int size = 0;
     *         if (dsl.size() >= 0) {
     *             size += dsl.size();
     *             if (dsl.from() >= 0) {
     *                 size += dsl.from();
     *             }
     *         }
     *
     *         if (size > 0) {
     *             sqlQuery.append(" limit " + size);
     *         }
     *         return sqlQuery.toString();
     *     }
     * }
     */

    // test unsupported knn dsl
    public void testUnsupportedKnnDsl() {
        try {
            SearchSourceBuilder builder = new SearchSourceBuilder();
            builder.query(QueryBuilders.matchAllQuery());
            builder.ext(List.of(new KnnSearchBuilder("field", new float[] { 1.0f, 2.0f }, 20, 20, 1.0f)));
            QueryTransformer.toSql("table", builder);
            fail();
        } catch (IOException e) {
            assertEquals(e.getMessage(), "unsupported knn parameter: {\"query\":{\"match_all\":{\"boost\":1.0}},"
                + "\"ext\":{\"field\":\"field\",\"k\":20,\"num_candidates\":20,\"query_vector\":[1.0,2.0],"
                + "\"similarity\":1.0}}");
        }

        // unsupported getFilterQueries
        try {
            SearchSourceBuilder builder = new SearchSourceBuilder();
            builder.query(QueryBuilders.matchAllQuery());
            KnnSearchBuilder knnSearchBuilder = new KnnSearchBuilder("field", new float[] { 1.0f, 2.0f }, 20, 20, null);
            knnSearchBuilder.addFilterQuery(QueryBuilders.matchAllQuery());
            builder.ext(List.of(knnSearchBuilder));
            QueryTransformer.toSql("table", builder);
            fail();
        } catch (IOException e) {
            assertEquals(e.getMessage(), "unsupported knn parameter: {\"query\":{\"match_all\":{\"boost\":1.0}},"
                + "\"ext\":{\"field\":\"field\",\"k\":20,\"num_candidates\":20,\"query_vector\":[1.0,2.0],"
                + "\"filter\":[{\"match_all\":{\"boost\":1.0}}]}}");
        }
    }
}

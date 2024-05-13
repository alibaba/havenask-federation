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

package org.havenask.engine.search.dsl.expression;

import org.havenask.common.collect.List;
import org.havenask.index.query.QueryBuilders;
import org.havenask.search.builder.KnnSearchBuilder;
import org.havenask.search.builder.SearchSourceBuilder;
import org.havenask.search.slice.SliceBuilder;
import org.havenask.test.HavenaskTestCase;
import org.junit.Before;

import java.util.HashMap;
import java.util.Map;

public class KnnQueryTests extends HavenaskTestCase {
    private Map<String, Object> indexMapping = new HashMap<>();
    private Map<String, Object> ObjectMapping = new HashMap<>();

    @Before
    public void setup() {
        Map<String, Object> propertiesMapping = new HashMap<>();
        Map<String, Object> fieldMapping = new HashMap<>();
        Map<String, Object> field1Mapping = new HashMap<>();
        Map<String, Object> field2Mapping = new HashMap<>();
        fieldMapping.put("type", "vector");
        fieldMapping.put("similarity", "L2_NORM");
        field1Mapping.put("type", "vector");
        field1Mapping.put("similarity", "L2_NORM");
        field2Mapping.put("type", "vector");
        field2Mapping.put("similarity", "DOT_PRODUCT");
        propertiesMapping.put("field", fieldMapping);
        propertiesMapping.put("field1", field1Mapping);
        propertiesMapping.put("field2", field2Mapping);
        indexMapping.put("properties", propertiesMapping);

        Map<String, Object> propertiesObjectMapping = new HashMap<>();
        Map<String, Object> userMapping = new HashMap<>();
        Map<String, Object> userPropertiesMapping = new HashMap<>();
        Map<String, Object> userImageMapping = new HashMap<>();
        Map<String, Object> userFirstNameMapping = new HashMap<>();
        userImageMapping.put("type", "vector");
        userImageMapping.put("similarity", "L2_NORM");
        userPropertiesMapping.put("image_vector", userImageMapping);
        userFirstNameMapping.put("type", "keyword");
        userPropertiesMapping.put("first_name", userFirstNameMapping);
        userMapping.put("properties", userPropertiesMapping);
        userMapping.put("properties", userPropertiesMapping);
        propertiesObjectMapping.put("user", userMapping);
        ObjectMapping.put("properties", propertiesObjectMapping);
    }

    public void testBasic() {
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.knnSearch(List.of(new KnnSearchBuilder("field1", new float[] { 1.0f, 2.0f }, 20, 20, null)));

        SourceExpression sourceExpression = new SourceExpression(builder);
        String actualTranslate = sourceExpression.getQuerySQLExpression("table1", indexMapping).translate();
        assertEquals(
            "SELECT `_id`, ((1/(1+vector_score('field1')))) AS _knn_score FROM `table1` WHERE 1=1 "
                + "AND (MATCHINDEX('field1', '1.0,2.0&n=20')) ORDER BY _knn_score DESC LIMIT 10 ",
            actualTranslate
        );
    }

    public void testKnnFilter() {
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.knnSearch(
            List.of(
                new KnnSearchBuilder("field1", new float[] { 1.0f, 2.0f }, 20, 20, null).addFilterQuery(
                    QueryBuilders.termQuery("field2", "value2")
                )
            )
        );

        SourceExpression sourceExpression = new SourceExpression(builder);
        String actualTranslate = sourceExpression.getQuerySQLExpression("table1", indexMapping).translate();
        assertEquals(
            "SELECT /*+ SCAN_ATTR(forbidIndex='field2')*/ `_id`, ((1/(1+vector_score('field1')))) AS _knn_score FROM `table1`"
                + " WHERE 1=1 AND (MATCHINDEX('field1', '1.0,2.0&n=20') AND (`field2` = 'value2')) ORDER BY _knn_score DESC LIMIT 10 ",
            actualTranslate
        );
    }

    public void testKnnFilterAndSlice() {
        SearchSourceBuilder builder = new SearchSourceBuilder();
        int id = 0;
        int max = 3;
        int shardNum = 7;
        builder.slice(new SliceBuilder(id, max));
        builder.knnSearch(
            List.of(
                new KnnSearchBuilder("field1", new float[] { 1.0f, 2.0f }, 20, 20, null).addFilterQuery(
                    QueryBuilders.termQuery("field2", "value2")
                )
            )
        );

        ExpressionContext context = new ExpressionContext(null, null, shardNum);
        SourceExpression sourceExpression = new SourceExpression(builder, context);
        String actualTranslate = sourceExpression.getQuerySQLExpression("table1", indexMapping).translate();
        assertEquals(
            "SELECT /*+ SCAN_ATTR(partitionIds='0,1,2', forbidIndex='field2')*/ `_id`, ((1/(1+vector_score('field1'))))"
                + " AS _knn_score FROM `table1` WHERE 1=1 AND (MATCHINDEX('field1', '1.0,2.0&n=20') AND (`field2` = 'value2')) "
                + "ORDER BY _knn_score DESC LIMIT 10 ",
            actualTranslate
        );
    }

    public void testObjectKnnDsl() {
        SearchSourceBuilder l2NormBuilder = new SearchSourceBuilder();
        l2NormBuilder.query(QueryBuilders.matchAllQuery());
        l2NormBuilder.knnSearch(List.of(new KnnSearchBuilder("user.image_vector", new float[] { 1.0f, 2.0f }, 20, 20, null)));

        SourceExpression sourceExpression = new SourceExpression(l2NormBuilder);
        String l2NormSql = sourceExpression.getQuerySQLExpression("table", ObjectMapping).translate();
        assertEquals(
            "SELECT `_id`, ((1/(1+vector_score('user_image_vector')))) AS _knn_score FROM `table` WHERE 1=1 "
                + "AND (MATCHINDEX('user_image_vector', '1.0,2.0&n=20')) ORDER BY _knn_score DESC LIMIT 10 ",
            l2NormSql
        );
    }

    // test multi knn dsl
    public void testMultiKnnDsl() {
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders.matchAllQuery());
        builder.knnSearch(
            List.of(
                new KnnSearchBuilder("field1", new float[] { 1.0f, 2.0f }, 20, 20, null),
                new KnnSearchBuilder("field2", new float[] { 0.6f, 0.8f }, 10, 10, null)
            )
        );

        SourceExpression sourceExpression = new SourceExpression(builder);
        String sql = sourceExpression.getQuerySQLExpression("table", indexMapping).translate();
        assertEquals(
            "SELECT `_id`, (((1/(1+vector_score('field1')))) + (((1+vector_score('field2'))/2))) "
                + "AS _knn_score FROM `table` WHERE 1=1 AND ((MATCHINDEX('field1', '1.0,2.0&n=20')) "
                + "or (MATCHINDEX('field2', '0.6,0.8&n=10'))) ORDER BY _knn_score DESC LIMIT 10 ",
            sql
        );
    }

    public void testIllegalKnnParams() {
        SearchSourceBuilder dotProductBuilder = new SearchSourceBuilder();
        dotProductBuilder.query(QueryBuilders.matchAllQuery());
        dotProductBuilder.knnSearch(List.of(new KnnSearchBuilder("field2", new float[] { 1.0f, 2.0f }, 20, 20, null)));
        try {
            SourceExpression sourceExpression = new SourceExpression(dotProductBuilder);
            sourceExpression.getQuerySQLExpression("table", indexMapping).translate();
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

            SourceExpression sourceExpression = new SourceExpression(builder);
            sourceExpression.getQuerySQLExpression("table", indexMapping).translate();
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "unsupported knn similarity parameter");
        }
    }
}

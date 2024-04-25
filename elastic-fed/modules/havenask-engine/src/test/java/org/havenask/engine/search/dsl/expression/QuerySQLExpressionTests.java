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

import org.havenask.index.query.QueryBuilders;
import org.havenask.search.builder.SearchSourceBuilder;
import org.havenask.search.sort.ScoreSortBuilder;
import org.havenask.search.sort.SortOrder;
import org.havenask.test.HavenaskTestCase;

import java.util.Map;

public class QuerySQLExpressionTests extends HavenaskTestCase {

    public void testBasicTranslate() {
        SearchSourceBuilder builder = new SearchSourceBuilder().sort("field1", SortOrder.ASC)
            .sort("field2", SortOrder.DESC)
            .size(10)
            .from(20)
            .query(
                QueryBuilders.boolQuery()
                    .must(QueryBuilders.termQuery("field1", "value1"))
                    .must(QueryBuilders.termQuery("field2", "value2"))
                    .must(QueryBuilders.termQuery("field3", "value3"))
            );

        SourceExpression sourceExpression = new SourceExpression(builder);
        String actualTranslate = sourceExpression.getQuerySQLExpression("table1", Map.of()).translate();
        assertEquals(
            "SELECT `_id` FROM `table1` WHERE (`field1` = 'value1' AND `field2` = 'value2' AND `field3` = 'value3')"
                + " ORDER BY `field1` ASC, `field2` DESC LIMIT 10 OFFSET 20 ",
            actualTranslate
        );
    }

    public void testMatchAllDocsQuery() {
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders.matchAllQuery());

        SourceExpression sourceExpression = new SourceExpression(builder);
        String sql = sourceExpression.getQuerySQLExpression("table", Map.of()).translate();
        assertEquals("SELECT `_id` FROM `table` WHERE 1=1 LIMIT 10 ", sql);
    }

    public void testMatchQuery() {
        {
            SearchSourceBuilder builder = new SearchSourceBuilder();
            builder.query(QueryBuilders.matchQuery("field", "value"));
            builder.sort(new ScoreSortBuilder());
            SourceExpression sourceExpression = new SourceExpression(builder);
            String sql = sourceExpression.getQuerySQLExpression("table", Map.of()).translate();
            assertEquals(
                "SELECT `_id`, bm25_score() AS _score FROM `table` WHERE "
                    + "MATCHINDEX('field', 'value', 'default_op:OR') ORDER BY _score DESC LIMIT 10 ",
                sql
            );
        }

        {
            SearchSourceBuilder objectSearcherBuilder = new SearchSourceBuilder();
            objectSearcherBuilder.query(QueryBuilders.matchQuery("user_first_name", "alice"));
            SourceExpression sourceExpression = new SourceExpression(objectSearcherBuilder);
            String objectSql = sourceExpression.getQuerySQLExpression("table", Map.of()).translate();
            assertEquals("SELECT `_id` FROM `table` WHERE MATCHINDEX('user_first_name', 'alice', 'default_op:OR') LIMIT 10 ", objectSql);
        }

        {
            SearchSourceBuilder objectSearcherWithDotBuilder = new SearchSourceBuilder();
            objectSearcherWithDotBuilder.query(QueryBuilders.matchQuery("user.first_name", "bob"));

            SourceExpression sourceExpression = new SourceExpression(objectSearcherWithDotBuilder);
            String objectWithDotSql = sourceExpression.getQuerySQLExpression("table", Map.of()).translate();
            assertEquals(
                "SELECT `_id` FROM `table` WHERE MATCHINDEX('user_first_name', 'bob', 'default_op:OR') LIMIT 10 ",
                objectWithDotSql
            );
        }
    }

    public void testTermQuery() {
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders.termQuery("field", "value"));
        SourceExpression sourceExpression = new SourceExpression(builder);
        String sql = sourceExpression.getQuerySQLExpression("table", Map.of()).translate();
        assertEquals("SELECT `_id` FROM `table` WHERE `field` = 'value' LIMIT 10 ", sql);
    }

    public void testLimit() {
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders.matchAllQuery());
        builder.from(10);
        builder.size(10);

        SourceExpression sourceExpression = new SourceExpression(builder);
        String sql = sourceExpression.getQuerySQLExpression("table", Map.of()).translate();
        assertEquals("SELECT `_id` FROM `table` WHERE 1=1 LIMIT 10 OFFSET 10 ", sql);
    }

    public void testNoFrom() {
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders.matchAllQuery());
        builder.size(10);

        SourceExpression sourceExpression = new SourceExpression(builder);
        String sql = sourceExpression.getQuerySQLExpression("table", Map.of()).translate();
        assertEquals(sql, "SELECT `_id` FROM `table` WHERE 1=1 LIMIT 10 ");
    }

    public void testNoSize() {
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders.matchAllQuery());
        builder.from(10);

        SourceExpression sourceExpression = new SourceExpression(builder);
        String sql = sourceExpression.getQuerySQLExpression("table", Map.of()).translate();
        assertEquals(sql, "SELECT `_id` FROM `table` WHERE 1=1 LIMIT 10 OFFSET 10 ");
    }

    public void testRangeDocsQuery() {
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders.rangeQuery("field").gte(1).lt(2));

        SourceExpression sourceExpression = new SourceExpression(builder);
        String sql = sourceExpression.getQuerySQLExpression("table", Map.of()).translate();
        assertEquals("SELECT `_id` FROM `table` WHERE QUERY('', 'field:[1,2)') LIMIT 10 ", sql);
    }

    public void testSortQuery() {
        {
            SearchSourceBuilder builder = new SearchSourceBuilder();
            builder.sort("field", SortOrder.DESC);

            SourceExpression sourceExpression = new SourceExpression(builder);
            String sql = sourceExpression.getQuerySQLExpression("table", Map.of()).translate();
            assertEquals("SELECT `_id` FROM `table` WHERE 1=1 ORDER BY `field` DESC LIMIT 10 ", sql);
        }

        {
            SearchSourceBuilder builder = new SearchSourceBuilder();
            builder.sort("field1", SortOrder.DESC).sort("field2", SortOrder.ASC);

            SourceExpression sourceExpression = new SourceExpression(builder);
            String sql = sourceExpression.getQuerySQLExpression("table", Map.of()).translate();
            assertEquals("SELECT `_id` FROM `table` WHERE 1=1 ORDER BY `field1` DESC, `field2` ASC LIMIT 10 ", sql);
        }
    }

    public void testMatchPhraseQuery() {
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders.matchPhraseQuery("field", "value"));

        SourceExpression sourceExpression = new SourceExpression(builder);
        String sql = sourceExpression.getQuerySQLExpression("table", Map.of()).translate();
        assertEquals("SELECT `_id` FROM `table` WHERE QUERY('field', '\"value\"') LIMIT 10 ", sql);
    }

    public void testQueryStringQuery() {
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders.queryStringQuery("value"));

        SourceExpression sourceExpression = new SourceExpression(builder);
        String sql = sourceExpression.getQuerySQLExpression("table", Map.of()).translate();
        assertEquals("SELECT `_id` FROM `table` WHERE QUERY('', 'value') LIMIT 10 ", sql);
    }

    // test terms
    public void testTermsQuery() {
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders.termsQuery("field", "value1", "value2"));

        SourceExpression sourceExpression = new SourceExpression(builder);
        String sql = sourceExpression.getQuerySQLExpression("table", Map.of()).translate();
        assertEquals("SELECT `_id` FROM `table` WHERE contain(`field`, 'value1|value2') LIMIT 10 ", sql);
    }
}

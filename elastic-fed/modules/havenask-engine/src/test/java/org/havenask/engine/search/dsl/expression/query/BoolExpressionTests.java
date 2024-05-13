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

package org.havenask.engine.search.dsl.expression.query;

import org.havenask.common.xcontent.NamedXContentRegistry;
import org.havenask.engine.search.dsl.expression.Expression;
import org.havenask.engine.search.dsl.expression.ExpressionContext;
import org.havenask.engine.search.dsl.expression.WhereExpression;
import org.havenask.index.query.QueryBuilders;
import org.havenask.search.builder.SearchSourceBuilder;
import org.havenask.test.HavenaskTestCase;

public class BoolExpressionTests extends HavenaskTestCase {
    public static ExpressionContext context = new ExpressionContext(NamedXContentRegistry.EMPTY, null, -1);

    public void testTranslateMust() {
        SearchSourceBuilder builder = new SearchSourceBuilder().query(
            QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("field1", "value1"))
                .must(QueryBuilders.termQuery("field2", "value2"))
                .must(QueryBuilders.termQuery("field2", "value3"))
        );

        Expression expression = QueryExpression.visitQuery(builder.query(), context);
        WhereExpression whereExpression = new WhereExpression(expression);
        String actualTranslate = whereExpression.translate();
        assertEquals("WHERE (`field1` = 'value1' AND `field2` = 'value2' AND `field2` = 'value3')", actualTranslate);
    }

    public void testTranslateMustNot() {
        SearchSourceBuilder builder = new SearchSourceBuilder().query(
            QueryBuilders.boolQuery()
                .mustNot(QueryBuilders.termQuery("field1", "value1"))
                .mustNot(QueryBuilders.termQuery("field2", "value2"))
                .mustNot(QueryBuilders.termQuery("field2", "value3"))
        );

        Expression expression = QueryExpression.visitQuery(builder.query(), context);
        WhereExpression whereExpression = new WhereExpression(expression);
        String actualTranslate = whereExpression.translate();
        assertEquals("WHERE NOT (`field1` = 'value1' AND `field2` = 'value2' AND `field2` = 'value3')", actualTranslate);
    }

    public void testTranslateFilter() {
        SearchSourceBuilder builder = new SearchSourceBuilder().query(
            QueryBuilders.boolQuery()
                .filter(QueryBuilders.termQuery("field1", "value1"))
                .filter(QueryBuilders.termQuery("field2", "value2"))
                .filter(QueryBuilders.termQuery("field2", "value3"))
        );

        Expression expression = QueryExpression.visitQuery(builder.query(), context);
        WhereExpression whereExpression = new WhereExpression(expression);
        String actualTranslate = whereExpression.translate();
        assertEquals("WHERE (`field1` = 'value1' AND `field2` = 'value2' AND `field2` = 'value3')", actualTranslate);
    }

    public void testTranslateShould() {
        SearchSourceBuilder builder = new SearchSourceBuilder().query(
            QueryBuilders.boolQuery()
                .should(QueryBuilders.termQuery("field1", "value1"))
                .should(QueryBuilders.termQuery("field2", "value2"))
                .should(QueryBuilders.termQuery("field2", "value3"))
        );

        Expression expression = QueryExpression.visitQuery(builder.query(), context);
        WhereExpression whereExpression = new WhereExpression(expression);
        String actualTranslate = whereExpression.translate();
        assertEquals("WHERE (`field1` = 'value1' OR `field2` = 'value2' OR `field2` = 'value3')", actualTranslate);
    }

    // test bool query has inner bool query
    public void testTranslateInnerBool() {
        SearchSourceBuilder builder = new SearchSourceBuilder().query(
            QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("field1", "value1"))
                .must(
                    QueryBuilders.boolQuery()
                        .must(QueryBuilders.termQuery("field2", "value2"))
                        .must(QueryBuilders.termQuery("field2", "value3"))
                )
        );

        Expression expression = QueryExpression.visitQuery(builder.query(), context);
        WhereExpression whereExpression = new WhereExpression(expression);
        String actualTranslate = whereExpression.translate();
        assertEquals("WHERE (`field1` = 'value1' AND (`field2` = 'value2' AND `field2` = 'value3'))", actualTranslate);
    }

    // test bool query has many inner bool query
    public void testTranslateManyInnerBool() {
        SearchSourceBuilder builder = new SearchSourceBuilder().query(
            QueryBuilders.boolQuery()
                .filter(QueryBuilders.termQuery("field1", "value1"))
                .must(
                    QueryBuilders.boolQuery()
                        .mustNot(QueryBuilders.termQuery("field2", "value2"))
                        .must(
                            QueryBuilders.boolQuery()
                                .must(QueryBuilders.termQuery("field3", "value3"))
                                .filter(QueryBuilders.termQuery("field4", "value4"))
                        )
                )
        );

        Expression expression = QueryExpression.visitQuery(builder.query(), context);
        WhereExpression whereExpression = new WhereExpression(expression);
        String actualTranslate = whereExpression.translate();
        assertEquals(
            "WHERE (((`field3` = 'value3') AND (`field4` = 'value4')) AND NOT (`field2` = 'value2')) AND (`field1` = 'value1')",
            actualTranslate
        );
    }
}

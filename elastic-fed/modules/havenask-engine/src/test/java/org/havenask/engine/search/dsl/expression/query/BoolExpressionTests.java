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

import org.havenask.engine.search.dsl.expression.Expression;
import org.havenask.engine.search.dsl.expression.SourceExpression;
import org.havenask.engine.search.dsl.expression.WhereExpression;
import org.havenask.index.query.QueryBuilders;
import org.havenask.search.builder.SearchSourceBuilder;
import org.havenask.test.HavenaskTestCase;

public class BoolExpressionTests extends HavenaskTestCase {
    public void testTranslateMust() {
        SearchSourceBuilder builder = new SearchSourceBuilder().query(
            QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("field1", "value1"))
                .must(QueryBuilders.termQuery("field2", "value2"))
                .must(QueryBuilders.termQuery("field2", "value3"))
        );

        Expression expression = SourceExpression.visitQuery(builder.query());
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

        Expression expression = SourceExpression.visitQuery(builder.query());
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

        Expression expression = SourceExpression.visitQuery(builder.query());
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

        Expression expression = SourceExpression.visitQuery(builder.query());
        WhereExpression whereExpression = new WhereExpression(expression);
        String actualTranslate = whereExpression.translate();
        assertEquals("WHERE (`field1` = 'value1' OR `field2` = 'value2' OR `field2` = 'value3')", actualTranslate);
    }
}

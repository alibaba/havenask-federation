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
import org.havenask.search.aggregations.AggregationBuilders;
import org.havenask.search.builder.SearchSourceBuilder;
import org.havenask.search.sort.SortOrder;
import org.havenask.test.HavenaskTestCase;

import java.util.List;

public class AggregationSQLExpressionTests extends HavenaskTestCase {
    public void testBasic() {
        SearchSourceBuilder builder = new SearchSourceBuilder().sort("field1", SortOrder.ASC)
            .query(
                QueryBuilders.boolQuery()
                    .must(QueryBuilders.termQuery("field1", "value1"))
                    .must(QueryBuilders.termQuery("field2", "value2"))
            )
            .aggregation(AggregationBuilders.terms("group_by_field1").field("field1"))
            .aggregation(AggregationBuilders.sum("sum_field2").field("field2"));

        SourceExpression sourceExpression = new SourceExpression(builder);
        List<AggregationSQLExpression> aggregationSQLExpressionList = sourceExpression.getAggregationSQLExpressions("table1");
        assertEquals(2, aggregationSQLExpressionList.size());
        assertEquals(
            "SELECT COUNT(*) FROM `table1` WHERE (`field1` = 'value1' AND `field2` = 'value2')  GROUP BY field1 LIMIT 10 ",
            aggregationSQLExpressionList.get(0).translate()
        );
        assertEquals(
            "SELECT SUM(`field2`) FROM `table1` WHERE (`field1` = 'value1' AND `field2` = 'value2')  LIMIT 1 ",
            aggregationSQLExpressionList.get(1).translate()
        );
    }

    public void testMultiAggs() {
        SearchSourceBuilder builder = new SearchSourceBuilder().sort("field1", SortOrder.ASC)
            .query(
                QueryBuilders.boolQuery()
                    .must(QueryBuilders.termQuery("field1", "value1"))
                    .must(QueryBuilders.termQuery("field2", "value2"))
            )
            .aggregation(
                AggregationBuilders.terms("group_by_field1")
                    .field("field1")
                    .subAggregation(
                        AggregationBuilders.terms("group_by_field11")
                            .field("field11")
                            .subAggregation(AggregationBuilders.sum("sum_field111").field("field111"))
                    )
                    .subAggregation(AggregationBuilders.terms("group_by_field12").field("field12"))
                    .subAggregation(AggregationBuilders.sum("sum_field11").field("field11"))
                    .subAggregation(AggregationBuilders.avg("avg_field11").field("field11"))
            )
            .aggregation(
                AggregationBuilders.terms("group_by_field2")
                    .field("field2")
                    .subAggregation(AggregationBuilders.sum("sum_field21").field("field21"))
                    .subAggregation(AggregationBuilders.avg("avg_field21").field("field21"))
            )
            .aggregation(AggregationBuilders.sum("sum_field3").field("field3"))
            .aggregation(AggregationBuilders.avg("avg_field4").field("field4"));

        SourceExpression sourceExpression = new SourceExpression(builder);
        List<AggregationSQLExpression> aggregationSQLExpressionList = sourceExpression.getAggregationSQLExpressions("table1");
        assertEquals(5, aggregationSQLExpressionList.size());
        assertEquals(
            "SELECT SUM(`field11`), AVG(`field11`) FROM `table1` WHERE (`field1` = 'value1' AND `field2` = 'value2') "
                + " GROUP BY field1 LIMIT 10 ",
            aggregationSQLExpressionList.get(0).translate()
        );
        assertEquals(
            "SELECT SUM(`field111`) FROM `table1` WHERE (`field1` = 'value1' AND `field2` = 'value2')  GROUP BY field1, field11 LIMIT 100 ",
            aggregationSQLExpressionList.get(1).translate()
        );
        assertEquals(
            "SELECT COUNT(*) FROM `table1` WHERE (`field1` = 'value1' AND `field2` = 'value2')  GROUP BY field1, field12 LIMIT 100 ",
            aggregationSQLExpressionList.get(2).translate()
        );
        assertEquals(
            "SELECT SUM(`field21`), AVG(`field21`) FROM `table1` WHERE (`field1` = 'value1' AND `field2` = 'value2') "
                + " GROUP BY field2 LIMIT 10 ",
            aggregationSQLExpressionList.get(3).translate()
        );
        assertEquals(
            "SELECT SUM(`field3`), AVG(`field4`) FROM `table1` WHERE (`field1` = 'value1' AND `field2` = 'value2')  LIMIT 1 ",
            aggregationSQLExpressionList.get(4).translate()
        );
    }
}

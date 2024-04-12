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

package org.havenask.engine.search.dsl.plan;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.havenask.client.ha.SqlResponse;
import org.havenask.common.collect.Tuple;
import org.havenask.engine.rpc.QrsSqlRequest;
import org.havenask.engine.rpc.QrsSqlResponse;
import org.havenask.engine.search.dsl.DSLSession;
import org.havenask.engine.search.dsl.expression.AggregationSQLExpression;
import org.havenask.engine.search.dsl.expression.aggregation.MetricExpression;
import org.havenask.search.aggregations.InternalAggregation;
import org.havenask.search.aggregations.InternalAggregations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.havenask.engine.search.rest.RestHavenaskSqlAction.SQL_DATABASE;

public class AggExec implements Executable<InternalAggregations> {
    protected Logger logger = LogManager.getLogger(AggExec.class);

    private List<AggregationSQLExpression> aggregationSQLExpressions;

    public AggExec(List<AggregationSQLExpression> aggregationSQLExpressions) {
        this.aggregationSQLExpressions = aggregationSQLExpressions;
    }

    @Override
    public InternalAggregations execute(DSLSession session) throws IOException {
        // exec aggregation
        List<Tuple<AggregationSQLExpression, SqlResponse>> aggResults = new ArrayList<>();
        for (AggregationSQLExpression agg : aggregationSQLExpressions) {// exec aggregation
            String sql = agg.translate();

            logger.debug("query exec, session: {}, exec sql: {}", session.getSessionId(), sql);
            String kvpair = "format:full_json;timeout:10000;databaseName:" + SQL_DATABASE;

            QrsSqlResponse qrsSqlResponse = session.getClient().executeSql(new QrsSqlRequest(sql, kvpair));
            SqlResponse sqlResponse = SqlResponse.parse(qrsSqlResponse.getResult());
            aggResults.add(new Tuple<>(agg, sqlResponse));
        }

        // fetch aggregation results
        List<InternalAggregation> aggregations = new ArrayList<>();
        aggResults.forEach(aggResult -> {
            AggregationSQLExpression agg = aggResult.v1();
            SqlResponse sqlResponse = aggResult.v2();

            InternalAggregation internalAggregation;

            List<InternalAggregation> groupByAggregations = new ArrayList<>();
            Object[][] sqlData = sqlResponse.getSqlResult().getData();

            for (int i = 0; i < sqlData.length; i++) {
                Object[] row = sqlData[i];
                List<String> groupByValues = new ArrayList<>();
                List<Object> metricValues = new ArrayList<>();

                int pos = 0;
                for (int j = 0; j < agg.getGroupBy().size(); j++) {
                    groupByValues.add(row[pos++].toString());
                }
                for (int j = 0; j < agg.getMetrics().size(); j++) {
                    metricValues.add(row[pos++]);
                }

                // parse sqlResponse and build aggregations
                List<InternalAggregation> subAggregations = new ArrayList<>();
                for (int j = 0; j < agg.getMetrics().size(); j++) {
                    MetricExpression metric = agg.getMetrics().get(j);
                    InternalAggregation aggregation = metric.buildInternalAggregation(metricValues.get(j));
                    subAggregations.add(aggregation);
                }
            }
        });

        return InternalAggregations.EMPTY;
    }
}

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

import org.havenask.engine.search.dsl.expression.aggregation.BucketExpression;
import org.havenask.engine.search.dsl.expression.aggregation.MetricExpression;

import java.util.List;

public class AggregationSQLExpression extends Expression {
    public final String from;
    private final List<BucketExpression> groupBy;
    private final List<MetricExpression> metrics;
    private final WhereExpression where;
    private final int limit;

    public AggregationSQLExpression(
        List<BucketExpression> groupBy,
        List<MetricExpression> metrics,
        String from,
        WhereExpression where,
        int limit
    ) {
        this.groupBy = groupBy;
        this.metrics = metrics;
        this.where = where;
        this.limit = limit;
        this.from = from;
    }

    public List<BucketExpression> getGroupBy() {
        return groupBy;
    }

    public List<MetricExpression> getMetrics() {
        return metrics;
    }

    @Override
    public String translate() {
        // translate to sql
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ");
        for (BucketExpression field : groupBy) {
            sb.append(field.getField()).append(", ");
        }

        for (Expression field : metrics) {
            sb.append(field.translate()).append(", ");
        }
        sb.delete(sb.length() - 2, sb.length());

        sb.append(" FROM `").append(from).append("` ");
        if (where != null) {
            sb.append(where.translate()).append(" ");
        }
        if (groupBy != null && !groupBy.isEmpty()) {
            sb.append(" GROUP BY ");
            for (Expression field : groupBy) {
                sb.append(field.translate()).append(", ");
            }
            sb.delete(sb.length() - 2, sb.length());
        }
        if (limit > 0) {
            sb.append(" LIMIT ").append(limit).append(" ");
        }

        return sb.toString();
    }
}

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

package org.havenask.engine.search.dsl.expression.aggregation;

import org.havenask.common.collect.Map;
import org.havenask.search.DocValueFormat;
import org.havenask.search.aggregations.InternalAggregation;
import org.havenask.search.aggregations.metrics.InternalSum;
import org.havenask.search.aggregations.metrics.SumAggregationBuilder;

public class SumExpression extends MetricExpression {
    private final SumAggregationBuilder sumAggregationBuilder;

    public SumExpression(SumAggregationBuilder sumAggregationBuilder) {
        this.sumAggregationBuilder = sumAggregationBuilder;
    }

    public String getField() {
        return sumAggregationBuilder.field();
    }

    @Override
    public InternalAggregation buildInternalAggregation(Object value) {
        return new InternalSum(sumAggregationBuilder.getName(), (Double) value, DocValueFormat.RAW, Map.of());
    }

    @Override
    public String translate() {
        return "SUM(`" + sumAggregationBuilder.field() + "`) AS `" + sumAggregationBuilder.getName() + "`";
    }
}

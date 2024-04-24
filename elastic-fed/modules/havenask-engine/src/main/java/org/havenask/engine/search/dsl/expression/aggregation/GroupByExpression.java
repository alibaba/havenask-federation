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

import org.havenask.engine.search.dsl.expression.Expression;
import org.havenask.search.aggregations.AggregationBuilder;

import java.util.List;

public class GroupByExpression extends Expression {
    private final List<AggregationBuilder> aggregationBuilders;

    public GroupByExpression(List<AggregationBuilder> aggregationBuilders) {
        this.aggregationBuilders = aggregationBuilders;
    }

    public List<AggregationBuilder> getAggregationBuilders() {
        return aggregationBuilders;
    }

    public AggregationBuilder getLastAggregationBuilder() {
        if (aggregationBuilders != null && aggregationBuilders.size() > 0) {
            return aggregationBuilders.get(aggregationBuilders.size() - 1);
        } else {
            return null;
        }
    }

    @Override
    public String translate() {
        return null;
    }
}

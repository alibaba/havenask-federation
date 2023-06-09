/*
*Copyright (c) 2021, Alibaba Group;
*Licensed under the Apache License, Version 2.0 (the "License");
*you may not use this file except in compliance with the License.
*You may obtain a copy of the License at

*   http://www.apache.org/licenses/LICENSE-2.0

*Unless required by applicable law or agreed to in writing, software
*distributed under the License is distributed on an "AS IS" BASIS,
*WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*See the License for the specific language governing permissions and
*limitations under the License.
*/

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright Havenask Contributors. See
 * GitHub history for details.
 */

package org.havenask.search.aggregations.metrics;

import org.havenask.common.xcontent.ObjectParser;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.common.xcontent.XContentParser;

import java.io.IOException;

public class ParsedWeightedAvg extends ParsedSingleValueNumericMetricsAggregation implements WeightedAvg {

    @Override
    public double getValue() {
        return value();
    }

    @Override
    public String getType() {
        return WeightedAvgAggregationBuilder.NAME;
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        // InternalWeightedAvg renders value only if the avg normalizer (count) is not 0.
        // We parse back `null` as Double.POSITIVE_INFINITY so we check for that value here to get the same xContent output
        boolean hasValue = value != Double.POSITIVE_INFINITY;
        builder.field(CommonFields.VALUE.getPreferredName(), hasValue ? value : null);
        if (hasValue && valueAsString != null) {
            builder.field(CommonFields.VALUE_AS_STRING.getPreferredName(), valueAsString);
        }
        return builder;
    }

    private static final ObjectParser<ParsedWeightedAvg, Void> PARSER
        = new ObjectParser<>(ParsedWeightedAvg.class.getSimpleName(), true, ParsedWeightedAvg::new);

    static {
        declareSingleValueFields(PARSER, Double.POSITIVE_INFINITY);
    }

    public static ParsedWeightedAvg fromXContent(XContentParser parser, final String name) {
        ParsedWeightedAvg avg = PARSER.apply(parser, null);
        avg.setName(name);
        return avg;
    }
}

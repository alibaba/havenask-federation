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

package org.havenask.search.aggregations.pipeline;

import org.havenask.common.xcontent.ObjectParser;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.common.xcontent.XContentParser;
import org.havenask.search.aggregations.metrics.ParsedPercentiles;
import org.havenask.search.aggregations.metrics.Percentiles;

import java.io.IOException;
import java.util.Map.Entry;

public class ParsedPercentilesBucket extends ParsedPercentiles implements Percentiles {

    @Override
    public String getType() {
        return PercentilesBucketPipelineAggregationBuilder.NAME;
    }

    @Override
    public double percentile(double percent) throws IllegalArgumentException {
        Double value = percentiles.get(percent);
        if (value == null) {
            throw new IllegalArgumentException("Percent requested [" + String.valueOf(percent) + "] was not" +
                    " one of the computed percentiles. Available keys are: " + percentiles.keySet());
        }
        return value;
    }

    @Override
    public String percentileAsString(double percent) {
        double value = percentile(percent); // check availability as unformatted value
        String valueAsString = percentilesAsString.get(percent);
        if (valueAsString != null) {
            return valueAsString;
        } else {
            return Double.toString(value);
        }
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("values");
        for (Entry<Double, Double> percent : percentiles.entrySet()) {
            double value = percent.getValue();
            boolean hasValue = !(Double.isNaN(value));
            Double key = percent.getKey();
            builder.field(Double.toString(key), hasValue ? value : null);
            String valueAsString = percentilesAsString.get(key);
            if (hasValue && valueAsString != null) {
                builder.field(key + "_as_string", valueAsString);
            }
        }
        builder.endObject();
        return builder;
    }

    private static final ObjectParser<ParsedPercentilesBucket, Void> PARSER =
            new ObjectParser<>(ParsedPercentilesBucket.class.getSimpleName(), true, ParsedPercentilesBucket::new);

    static {
        ParsedPercentiles.declarePercentilesFields(PARSER);
    }

    public static ParsedPercentilesBucket fromXContent(XContentParser parser, String name) throws IOException {
        ParsedPercentilesBucket aggregation = PARSER.parse(parser, null);
        aggregation.setName(name);
        return aggregation;
    }
}

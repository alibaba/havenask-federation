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

import org.havenask.common.ParseField;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.io.stream.StreamOutput;
import org.havenask.common.xcontent.ObjectParser;
import org.havenask.common.xcontent.ToXContent;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.index.query.QueryBuilder;
import org.havenask.index.query.QueryShardContext;
import org.havenask.search.DocValueFormat;
import org.havenask.search.aggregations.AggregationBuilder;
import org.havenask.search.aggregations.AggregatorFactories.Builder;
import org.havenask.search.aggregations.AggregatorFactory;
import org.havenask.search.aggregations.support.CoreValuesSourceType;
import org.havenask.search.aggregations.support.MultiValuesSourceAggregationBuilder;
import org.havenask.search.aggregations.support.MultiValuesSourceAggregatorFactory;
import org.havenask.search.aggregations.support.MultiValuesSourceFieldConfig;
import org.havenask.search.aggregations.support.MultiValuesSourceParseHelper;
import org.havenask.search.aggregations.support.ValueType;
import org.havenask.search.aggregations.support.ValuesSourceConfig;
import org.havenask.search.aggregations.support.ValuesSourceRegistry;
import org.havenask.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class WeightedAvgAggregationBuilder extends MultiValuesSourceAggregationBuilder.LeafOnly<WeightedAvgAggregationBuilder> {
    public static final String NAME = "weighted_avg";
    public static final ParseField VALUE_FIELD = new ParseField("value");
    public static final ParseField WEIGHT_FIELD = new ParseField("weight");

    public static final ObjectParser<WeightedAvgAggregationBuilder, String> PARSER =
            ObjectParser.fromBuilder(NAME, WeightedAvgAggregationBuilder::new);
    static {
        MultiValuesSourceParseHelper.declareCommon(PARSER, true, ValueType.NUMERIC);
        MultiValuesSourceParseHelper.declareField(VALUE_FIELD.getPreferredName(), PARSER, true, false, false);
        MultiValuesSourceParseHelper.declareField(WEIGHT_FIELD.getPreferredName(), PARSER, true, false, false);
    }

    public static void registerUsage(ValuesSourceRegistry.Builder builder) {
        builder.registerUsage(NAME, CoreValuesSourceType.NUMERIC);
    }

    public WeightedAvgAggregationBuilder(String name) {
        super(name);
    }

    public WeightedAvgAggregationBuilder(WeightedAvgAggregationBuilder clone, Builder factoriesBuilder, Map<String, Object> metadata) {
        super(clone, factoriesBuilder, metadata);
    }

    public WeightedAvgAggregationBuilder value(MultiValuesSourceFieldConfig valueConfig) {
        valueConfig = Objects.requireNonNull(valueConfig, "Configuration for field [" + VALUE_FIELD + "] cannot be null");
        field(VALUE_FIELD.getPreferredName(), valueConfig);
        return this;
    }

    public WeightedAvgAggregationBuilder weight(MultiValuesSourceFieldConfig weightConfig) {
        weightConfig = Objects.requireNonNull(weightConfig, "Configuration for field [" + WEIGHT_FIELD + "] cannot be null");
        field(WEIGHT_FIELD.getPreferredName(), weightConfig);
        return this;
    }

    /**
     * Read from a stream.
     */
    public WeightedAvgAggregationBuilder(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    protected AggregationBuilder shallowCopy(Builder factoriesBuilder, Map<String, Object> metadata) {
        return new WeightedAvgAggregationBuilder(this, factoriesBuilder, metadata);
    }

    @Override
    protected ValuesSourceType defaultValueSourceType() {
        return CoreValuesSourceType.NUMERIC;
    }

    @Override
    protected void innerWriteTo(StreamOutput out) {
        // Do nothing, no extra state to write to stream
    }

    @Override
    public BucketCardinality bucketCardinality() {
        return BucketCardinality.NONE;
    }

    @Override
    protected MultiValuesSourceAggregatorFactory innerBuild(QueryShardContext queryShardContext,
                                                            Map<String, ValuesSourceConfig> configs,
                                                            Map<String, QueryBuilder> filters,
                                                            DocValueFormat format,
                                                            AggregatorFactory parent,
                                                            Builder subFactoriesBuilder) throws IOException {
        return new WeightedAvgAggregatorFactory(name, configs, format, queryShardContext, parent, subFactoriesBuilder, metadata);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, ToXContent.Params params) throws IOException {
        return builder;
    }

    @Override
    public String getType() {
        return NAME;
    }
}

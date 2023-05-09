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

package org.havenask.search.aggregations.bucket.composite;

import org.apache.lucene.index.IndexReader;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.io.stream.StreamOutput;
import org.havenask.common.util.BigArrays;
import org.havenask.common.xcontent.ObjectParser;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.common.xcontent.XContentParser;
import org.havenask.index.mapper.MappedFieldType;
import org.havenask.index.query.QueryShardContext;
import org.havenask.search.aggregations.bucket.histogram.Histogram;
import org.havenask.search.aggregations.support.CoreValuesSourceType;
import org.havenask.search.aggregations.support.ValuesSource;
import org.havenask.search.aggregations.support.ValuesSourceConfig;
import org.havenask.search.aggregations.support.ValuesSourceRegistry;
import org.havenask.search.aggregations.support.ValuesSourceType;
import org.havenask.search.sort.SortOrder;

import java.io.IOException;
import java.util.Objects;
import java.util.function.LongConsumer;

/**
 * A {@link CompositeValuesSourceBuilder} that builds a {@link HistogramValuesSource} from another numeric values source
 * using the provided interval.
 */
public class HistogramValuesSourceBuilder extends CompositeValuesSourceBuilder<HistogramValuesSourceBuilder> {
    @FunctionalInterface
    public interface HistogramCompositeSupplier {
        CompositeValuesSourceConfig apply(
            ValuesSourceConfig config,
            double interval,
            String name,
            boolean hasScript, // probably redundant with the config, but currently we check this two different ways...
            String format,
            boolean missingBucket,
            SortOrder order
        );
    }

    static final String TYPE = "histogram";
    static final ValuesSourceRegistry.RegistryKey<HistogramCompositeSupplier> REGISTRY_KEY = new ValuesSourceRegistry.RegistryKey<>(
        TYPE,
        HistogramCompositeSupplier.class
    );

    private static final ObjectParser<HistogramValuesSourceBuilder, Void> PARSER;
    static {
        PARSER = new ObjectParser<>(HistogramValuesSourceBuilder.TYPE);
        PARSER.declareDouble(HistogramValuesSourceBuilder::interval, Histogram.INTERVAL_FIELD);
        CompositeValuesSourceParserHelper.declareValuesSourceFields(PARSER);
    }
    static HistogramValuesSourceBuilder parse(String name, XContentParser parser) throws IOException {
        return PARSER.parse(parser, new HistogramValuesSourceBuilder(name), null);
    }

    public static void register(ValuesSourceRegistry.Builder builder) {
        builder.register(
            REGISTRY_KEY,
            org.havenask.common.collect.List.of(CoreValuesSourceType.DATE, CoreValuesSourceType.NUMERIC),
            (valuesSourceConfig, interval, name, hasScript, format, missingBucket, order) -> {
                ValuesSource.Numeric numeric = (ValuesSource.Numeric) valuesSourceConfig.getValuesSource();
                final HistogramValuesSource vs = new HistogramValuesSource(numeric, interval);
                final MappedFieldType fieldType = valuesSourceConfig.fieldType();
                return new CompositeValuesSourceConfig(
                    name,
                    fieldType,
                    vs,
                    valuesSourceConfig.format(),
                    order,
                    missingBucket,
                    hasScript,
                    (
                        BigArrays bigArrays,
                        IndexReader reader,
                        int size,
                        LongConsumer addRequestCircuitBreakerBytes,
                        CompositeValuesSourceConfig compositeValuesSourceConfig) -> {
                        final ValuesSource.Numeric numericValuesSource = (ValuesSource.Numeric) compositeValuesSourceConfig.valuesSource();
                        return new DoubleValuesSource(
                            bigArrays,
                            compositeValuesSourceConfig.fieldType(),
                            numericValuesSource::doubleValues,
                            compositeValuesSourceConfig.format(),
                            compositeValuesSourceConfig.missingBucket(),
                            size,
                            compositeValuesSourceConfig.reverseMul()
                        );
                    }
                );
            }, false);
    }

    private double interval = 0;

    public HistogramValuesSourceBuilder(String name) {
        super(name);
    }

    protected HistogramValuesSourceBuilder(StreamInput in) throws IOException {
        super(in);
        this.interval = in.readDouble();
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        out.writeDouble(interval);
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(Histogram.INTERVAL_FIELD.getPreferredName(), interval);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), interval);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        HistogramValuesSourceBuilder other = (HistogramValuesSourceBuilder) obj;
        return Objects.equals(interval, other.interval);
    }

    @Override
    public String type() {
        return TYPE;
    }

    /**
     * Returns the interval that is set on this source
     **/
    public double interval() {
        return interval;
    }

    /**
     * Sets the interval on this source.
     **/
    public HistogramValuesSourceBuilder interval(double interval) {
        if (interval <= 0) {
            throw new IllegalArgumentException("[interval] must be greater than 0 for [histogram] source");
        }
        this.interval = interval;
        return this;
    }

    @Override
    protected ValuesSourceType getDefaultValuesSourceType() {
        return CoreValuesSourceType.NUMERIC;
    }

    @Override
    protected CompositeValuesSourceConfig innerBuild(QueryShardContext queryShardContext, ValuesSourceConfig config) throws IOException {
        return queryShardContext.getValuesSourceRegistry()
            .getAggregator(REGISTRY_KEY, config)
            .apply(config, interval, name, script() != null, format(), missingBucket(), order());
    }
}

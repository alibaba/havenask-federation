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
import org.havenask.LegacyESVersion;
import org.havenask.common.ParseField;
import org.havenask.common.Rounding;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.io.stream.StreamOutput;
import org.havenask.common.unit.TimeValue;
import org.havenask.common.util.BigArrays;
import org.havenask.common.xcontent.ObjectParser;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.common.xcontent.XContentParser;
import org.havenask.index.mapper.MappedFieldType;
import org.havenask.index.query.QueryShardContext;
import org.havenask.script.Script;
import org.havenask.search.DocValueFormat;
import org.havenask.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.havenask.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.havenask.search.aggregations.bucket.histogram.DateIntervalConsumer;
import org.havenask.search.aggregations.bucket.histogram.DateIntervalWrapper;
import org.havenask.search.aggregations.bucket.histogram.Histogram;
import org.havenask.search.aggregations.support.CoreValuesSourceType;
import org.havenask.search.aggregations.support.ValuesSource;
import org.havenask.search.aggregations.support.ValuesSourceConfig;
import org.havenask.search.aggregations.support.ValuesSourceRegistry;
import org.havenask.search.aggregations.support.ValuesSourceType;
import org.havenask.search.sort.SortOrder;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Objects;
import java.util.function.LongConsumer;

/**
 * A {@link CompositeValuesSourceBuilder} that builds a {@link RoundingValuesSource} from a {@link Script} or
 * a field name using the provided interval.
 */
public class DateHistogramValuesSourceBuilder
    extends CompositeValuesSourceBuilder<DateHistogramValuesSourceBuilder> implements DateIntervalConsumer {
    @FunctionalInterface
    public interface DateHistogramCompositeSupplier {
        CompositeValuesSourceConfig apply(
            ValuesSourceConfig config,
            Rounding rounding,
            String name,
            boolean hasScript, // probably redundant with the config, but currently we check this two different ways...
            String format,
            boolean missingBucket,
            SortOrder order
        );
    }

    static final String TYPE = "date_histogram";
    static final ValuesSourceRegistry.RegistryKey<DateHistogramCompositeSupplier> REGISTRY_KEY = new ValuesSourceRegistry.RegistryKey<>(
        TYPE,
        DateHistogramCompositeSupplier.class
    );

    static final ObjectParser<DateHistogramValuesSourceBuilder, String> PARSER =
            ObjectParser.fromBuilder(TYPE, DateHistogramValuesSourceBuilder::new);
    static {
        PARSER.declareString(DateHistogramValuesSourceBuilder::format, new ParseField("format"));
        DateIntervalWrapper.declareIntervalFields(PARSER);
        PARSER.declareField(DateHistogramValuesSourceBuilder::offset, p -> {
            if (p.currentToken() == XContentParser.Token.VALUE_NUMBER) {
                return p.longValue();
            } else {
                return DateHistogramAggregationBuilder.parseStringOffset(p.text());
            }
        }, Histogram.OFFSET_FIELD, ObjectParser.ValueType.LONG);
        PARSER.declareField(DateHistogramValuesSourceBuilder::timeZone, p -> {
            if (p.currentToken() == XContentParser.Token.VALUE_STRING) {
                return ZoneId.of(p.text());
            } else {
                return ZoneOffset.ofHours(p.intValue());
            }
        }, new ParseField("time_zone"), ObjectParser.ValueType.LONG);
        CompositeValuesSourceParserHelper.declareValuesSourceFields(PARSER);
    }

    private ZoneId timeZone = null;
    private DateIntervalWrapper dateHistogramInterval = new DateIntervalWrapper();
    private long offset = 0;

    public DateHistogramValuesSourceBuilder(String name) {
        super(name);
    }

    protected DateHistogramValuesSourceBuilder(StreamInput in) throws IOException {
        super(in);
        dateHistogramInterval = new DateIntervalWrapper(in);
        timeZone = in.readOptionalZoneId();
        if (in.getVersion().onOrAfter(LegacyESVersion.V_7_6_0)) {
            offset = in.readLong();
        }
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        dateHistogramInterval.writeTo(out);
        out.writeOptionalZoneId(timeZone);
        if (out.getVersion().onOrAfter(LegacyESVersion.V_7_6_0)) {
            out.writeLong(offset);
        }
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, Params params) throws IOException {
        dateHistogramInterval.toXContent(builder, params);
        if (timeZone != null) {
            builder.field("time_zone", timeZone.toString());
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), dateHistogramInterval, timeZone);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        DateHistogramValuesSourceBuilder other = (DateHistogramValuesSourceBuilder) obj;
        return Objects.equals(dateHistogramInterval, other.dateHistogramInterval)
            && Objects.equals(timeZone, other.timeZone);
    }

    @Override
    public String type() {
        return TYPE;
    }

    /**
     * Returns the interval in milliseconds that is set on this source
     **/
    @Deprecated
    public long interval() {
        return dateHistogramInterval.interval();
    }

    /**
     * Sets the interval on this source.
     * If both {@link #interval()} and {@link #dateHistogramInterval()} are set,
     * then the {@link #dateHistogramInterval()} wins.
     *
     * @deprecated Use {@link #calendarInterval(DateHistogramInterval)} or {@link #fixedInterval(DateHistogramInterval)} instead
     * @since 7.2.0
     **/
    @Deprecated
    public DateHistogramValuesSourceBuilder interval(long interval) {
        dateHistogramInterval.interval(interval);
        return this;
    }

    /**
     * Returns the date interval that is set on this source
     **/
    @Deprecated
    public DateHistogramInterval dateHistogramInterval() {
        return dateHistogramInterval.dateHistogramInterval();
    }

    /**
     * @deprecated Use {@link #calendarInterval(DateHistogramInterval)} or {@link #fixedInterval(DateHistogramInterval)} instead
     * @since 7.2.0
     */
    @Deprecated
    public DateHistogramValuesSourceBuilder dateHistogramInterval(DateHistogramInterval interval) {
        dateHistogramInterval.dateHistogramInterval(interval);
        return this;
    }

    /**
     * Sets the interval of the DateHistogram using calendar units (`1d`, `1w`, `1M`, etc).  These units
     * are calendar-aware, meaning they respect leap additions, variable days per month, etc.
     *
     * This is mutually exclusive with {@link DateHistogramValuesSourceBuilder#fixedInterval(DateHistogramInterval)}
     *
     * @param interval The calendar interval to use with the aggregation
     */
    public DateHistogramValuesSourceBuilder calendarInterval(DateHistogramInterval interval) {
        dateHistogramInterval.calendarInterval(interval);
        return this;
    }

    /**
     * Sets the interval of the DateHistogram using fixed units (`1ms`, `1s`, `10m`, `4h`, etc).  These are
     * not calendar aware and are simply multiples of fixed, SI units.
     *
     * This is mutually exclusive with {@link DateHistogramValuesSourceBuilder#calendarInterval(DateHistogramInterval)}
     *
     * @param interval The fixed interval to use with the aggregation
     */
    public DateHistogramValuesSourceBuilder fixedInterval(DateHistogramInterval interval) {
        dateHistogramInterval.fixedInterval(interval);
        return this;
    }

    /** Return the interval as a date time unit if applicable, regardless of how it was configured. If this returns
     *  {@code null} then it means that the interval is expressed as a fixed
     *  {@link TimeValue} and may be accessed via {@link #getIntervalAsFixed()} ()}. */
    public DateHistogramInterval getIntervalAsCalendar() {
        return dateHistogramInterval.getAsCalendarInterval();
    }

    /**
     * Get the interval as a {@link TimeValue}, regardless of how it was configured. Returns null if
     * the interval cannot be parsed as a fixed time.
     */
    public DateHistogramInterval getIntervalAsFixed() {
        return dateHistogramInterval.getAsFixedInterval();
    }

    /**
     * Sets the time zone to use for this aggregation
     */
    public DateHistogramValuesSourceBuilder timeZone(ZoneId timeZone) {
        if (timeZone == null) {
            throw new IllegalArgumentException("[timeZone] must not be null: [" + name + "]");
        }
        this.timeZone = timeZone;
        return this;
    }

    /**
     * Gets the time zone to use for this aggregation
     */
    @Override
    public ZoneId timeZone() {
        return timeZone;
    }

    /**
     * Get the offset to use when rounding, which is a number of milliseconds.
     */
    public long offset() {
        return offset;
    }

    /**
     * Set the offset on this builder, which is a number of milliseconds.
     * @return this for chaining
     */
    public DateHistogramValuesSourceBuilder offset(long offset) {
        this.offset = offset;
        return this;
    }

    public static void register(ValuesSourceRegistry.Builder builder) {
        builder.register(
            REGISTRY_KEY,
            org.havenask.common.collect.List.of(CoreValuesSourceType.DATE, CoreValuesSourceType.NUMERIC),
            (valuesSourceConfig, rounding, name, hasScript, format, missingBucket, order) -> {
                ValuesSource.Numeric numeric = (ValuesSource.Numeric) valuesSourceConfig.getValuesSource();
                // TODO once composite is plugged in to the values source registry or at least understands Date values source types use it
                // here
                Rounding.Prepared preparedRounding = rounding.prepareForUnknown();
                RoundingValuesSource vs = new RoundingValuesSource(numeric, preparedRounding);
                // is specified in the builder.
                final DocValueFormat docValueFormat = format == null ? DocValueFormat.RAW : valuesSourceConfig.format();
                final MappedFieldType fieldType = valuesSourceConfig.fieldType();
                return new CompositeValuesSourceConfig(
                    name,
                    fieldType,
                    vs,
                    docValueFormat,
                    order,
                    missingBucket,
                    hasScript,
                    (
                        BigArrays bigArrays,
                        IndexReader reader,
                        int size,
                        LongConsumer addRequestCircuitBreakerBytes,
                        CompositeValuesSourceConfig compositeValuesSourceConfig) -> {
                        final RoundingValuesSource roundingValuesSource = (RoundingValuesSource) compositeValuesSourceConfig.valuesSource();
                        return new LongValuesSource(
                            bigArrays,
                            compositeValuesSourceConfig.fieldType(),
                            roundingValuesSource::longValues,
                            roundingValuesSource::round,
                            compositeValuesSourceConfig.format(),
                            compositeValuesSourceConfig.missingBucket(),
                            size,
                            compositeValuesSourceConfig.reverseMul()
                        );
                    }
                );
            },
            false);
    }

    @Override
    protected ValuesSourceType getDefaultValuesSourceType() {
        return CoreValuesSourceType.DATE;
    }

    @Override
    protected CompositeValuesSourceConfig innerBuild(QueryShardContext queryShardContext, ValuesSourceConfig config) throws IOException {
        Rounding rounding = dateHistogramInterval.createRounding(timeZone(), offset);
        return queryShardContext.getValuesSourceRegistry()
            .getAggregator(REGISTRY_KEY, config)
            .apply(config, rounding, name, config.script() != null, format(), missingBucket(), order());
    }
}

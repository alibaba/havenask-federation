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

import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.io.stream.StreamOutput;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.search.DocValueFormat;
import org.havenask.search.aggregations.AggregationBuilder;
import org.havenask.search.aggregations.pipeline.BucketHelpers.GapPolicy;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public abstract class BucketMetricsPipelineAggregationBuilder<AF extends BucketMetricsPipelineAggregationBuilder<AF>>
        extends AbstractPipelineAggregationBuilder<AF> {

    private String format = null;
    private GapPolicy gapPolicy = GapPolicy.SKIP;

    protected BucketMetricsPipelineAggregationBuilder(String name, String type, String[] bucketsPaths) {
        super(name, type, bucketsPaths);
    }

    /**
     * Read from a stream.
     */
    protected BucketMetricsPipelineAggregationBuilder(StreamInput in, String type) throws IOException {
        super(in, type);
        format = in.readOptionalString();
        gapPolicy = GapPolicy.readFrom(in);
    }

    @Override
    protected final void doWriteTo(StreamOutput out) throws IOException {
        out.writeOptionalString(format);
        gapPolicy.writeTo(out);
        innerWriteTo(out);
    }

    protected abstract void innerWriteTo(StreamOutput out) throws IOException;

    /**
     * Sets the format to use on the output of this aggregation.
     */
    @SuppressWarnings("unchecked")
    public AF format(String format) {
        this.format = format;
        return (AF) this;
    }

    /**
     * Gets the format to use on the output of this aggregation.
     */
    public String format() {
        return format;
    }

    protected DocValueFormat formatter() {
        if (format != null) {
            return new DocValueFormat.Decimal(format);
        } else {
            return DocValueFormat.RAW;
        }
    }

    /**
     * Sets the gap policy to use for this aggregation.
     */
    @SuppressWarnings("unchecked")
    public AF gapPolicy(GapPolicy gapPolicy) {
        this.gapPolicy = gapPolicy;
        return (AF) this;
    }

    /**
     * Gets the gap policy to use for this aggregation.
     */
    public GapPolicy gapPolicy() {
        return gapPolicy;
    }

    @Override
    protected abstract PipelineAggregator createInternal(Map<String, Object> metadata);

    @Override
    protected void validate(ValidationContext context) {
        if (bucketsPaths.length != 1) {
            context.addBucketPathValidationError("must contain a single entry for aggregation [" + name + "]");
            return;
        }
        // Need to find the first agg name in the buckets path to check its a
        // multi bucket agg: aggs are split with '>' and can optionally have a
        // metric name after them by using '.' so need to split on both to get
        // just the agg name
        final String firstAgg = bucketsPaths[0].split("[>\\.]")[0];
        Optional<AggregationBuilder> aggBuilder = context.getSiblingAggregations().stream()
                .filter(builder -> builder.getName().equals(firstAgg))
                .findAny();
        if (false == aggBuilder.isPresent()) {
            context.addBucketPathValidationError("aggregation does not exist for aggregation [" + name + "]: " + bucketsPaths[0]);
            return;
        }
        if (aggBuilder.get().bucketCardinality() != AggregationBuilder.BucketCardinality.MANY) {
            context.addValidationError("The first aggregation in " + PipelineAggregator.Parser.BUCKETS_PATH.getPreferredName()
                    + " must be a multi-bucket aggregation for aggregation [" + name + "] found :"
                    + aggBuilder.get().getClass().getName() + " for buckets path: " + bucketsPaths[0]);
        }
    }

    @Override
    protected final XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        if (format != null) {
            builder.field(BucketMetricsParser.FORMAT.getPreferredName(), format);
        }
        if (gapPolicy != null) {
            builder.field(BucketMetricsParser.GAP_POLICY.getPreferredName(), gapPolicy.getName());
        }
        doXContentBody(builder, params);
        return builder;
    }

    protected abstract XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException;

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), format, gapPolicy);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        @SuppressWarnings("unchecked")
        BucketMetricsPipelineAggregationBuilder<AF> other = (BucketMetricsPipelineAggregationBuilder<AF>) obj;
        return Objects.equals(format, other.format)
            && Objects.equals(gapPolicy, other.gapPolicy);
    }

}

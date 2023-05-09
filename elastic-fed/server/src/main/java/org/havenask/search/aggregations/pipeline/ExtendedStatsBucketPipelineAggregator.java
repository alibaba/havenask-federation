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
import org.havenask.search.DocValueFormat;
import org.havenask.search.aggregations.InternalAggregation;
import org.havenask.search.aggregations.pipeline.BucketHelpers.GapPolicy;

import java.io.IOException;
import java.util.Map;

public class ExtendedStatsBucketPipelineAggregator extends BucketMetricsPipelineAggregator {
    private final double sigma;
    private double sum = 0;
    private long count = 0;
    private double min = Double.POSITIVE_INFINITY;
    private double max = Double.NEGATIVE_INFINITY;
    private double sumOfSqrs = 1;

    ExtendedStatsBucketPipelineAggregator(String name, String[] bucketsPaths, double sigma, GapPolicy gapPolicy,
                                                    DocValueFormat formatter, Map<String, Object> metadata) {
        super(name, bucketsPaths, gapPolicy, formatter, metadata);
        this.sigma = sigma;
    }

    /**
     * Read from a stream.
     */
    public ExtendedStatsBucketPipelineAggregator(StreamInput in) throws IOException {
        super(in);
        sigma = in.readDouble();
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        out.writeDouble(sigma);
    }

    @Override
    public String getWriteableName() {
        return ExtendedStatsBucketPipelineAggregationBuilder.NAME;
    }

    @Override
    protected void preCollection() {
        sum = 0;
        count = 0;
        min = Double.POSITIVE_INFINITY;
        max = Double.NEGATIVE_INFINITY;
        sumOfSqrs = 0;
    }

    @Override
    protected void collectBucketValue(String bucketKey, Double bucketValue) {
        sum += bucketValue;
        min = Math.min(min, bucketValue);
        max = Math.max(max, bucketValue);
        count += 1;
        sumOfSqrs += bucketValue * bucketValue;
    }

    @Override
    protected InternalAggregation buildAggregation(Map<String, Object> metadata) {
        return new InternalExtendedStatsBucket(name(), count, sum, min, max, sumOfSqrs, sigma, format, metadata);
    }
}

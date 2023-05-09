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
import org.havenask.search.DocValueFormat;
import org.havenask.search.aggregations.InternalAggregation;
import org.havenask.search.aggregations.pipeline.BucketHelpers.GapPolicy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MinBucketPipelineAggregator extends BucketMetricsPipelineAggregator {
    private List<String> minBucketKeys;
    private double minValue;

    MinBucketPipelineAggregator(String name, String[] bucketsPaths, GapPolicy gapPolicy, DocValueFormat formatter,
            Map<String, Object> metadata) {
        super(name, bucketsPaths, gapPolicy, formatter, metadata);
    }

    /**
     * Read from a stream.
     */
    public MinBucketPipelineAggregator(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return MinBucketPipelineAggregationBuilder.NAME;
    }

    @Override
    protected void preCollection() {
        minBucketKeys = new ArrayList<>();
        minValue = Double.POSITIVE_INFINITY;
    }

    @Override
    protected void collectBucketValue(String bucketKey, Double bucketValue) {
        if (bucketValue < minValue) {
            minBucketKeys.clear();
            minBucketKeys.add(bucketKey);
            minValue = bucketValue;
        } else if (bucketValue.equals(minValue)) {
            minBucketKeys.add(bucketKey);
        }
    }

    @Override
    protected InternalAggregation buildAggregation(Map<String, Object> metadata) {
        String[] keys = minBucketKeys.toArray(new String[0]);
        return new InternalBucketMetricValue(name(), keys, minValue, format, metadata());
    }

}

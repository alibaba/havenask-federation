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

import org.havenask.LegacyESVersion;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.io.stream.StreamOutput;
import org.havenask.search.DocValueFormat;
import org.havenask.search.aggregations.InternalAggregation;
import org.havenask.search.aggregations.pipeline.BucketHelpers.GapPolicy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class PercentilesBucketPipelineAggregator extends BucketMetricsPipelineAggregator {

    private final double[] percents;
    private boolean keyed = true;
    private List<Double> data;

    PercentilesBucketPipelineAggregator(String name, double[] percents, boolean keyed, String[] bucketsPaths,
                                        GapPolicy gapPolicy, DocValueFormat formatter, Map<String, Object> metadata) {
        super(name, bucketsPaths, gapPolicy, formatter, metadata);
        this.percents = percents;
        this.keyed = keyed;
    }

    /**
     * Read from a stream.
     */
    public PercentilesBucketPipelineAggregator(StreamInput in) throws IOException {
        super(in);
        percents = in.readDoubleArray();

        if (in.getVersion().onOrAfter(LegacyESVersion.V_7_0_0)) {
            keyed = in.readBoolean();
        }
    }

    @Override
    public void innerWriteTo(StreamOutput out) throws IOException {
        out.writeDoubleArray(percents);

        if (out.getVersion().onOrAfter(LegacyESVersion.V_7_0_0)) {
            out.writeBoolean(keyed);
        }
    }

    @Override
    public String getWriteableName() {
        return PercentilesBucketPipelineAggregationBuilder.NAME;
    }

    @Override
    protected void preCollection() {
       data = new ArrayList<>(1024);
    }

    @Override
    protected void collectBucketValue(String bucketKey, Double bucketValue) {
        data.add(bucketValue);
    }

    @Override
    protected InternalAggregation buildAggregation(Map<String, Object> metadata) {
        // Perform the sorting and percentile collection now that all the data
        // has been collected.
        Collections.sort(data);

        double[] percentiles = new double[percents.length];
        if (data.size() == 0) {
            for (int i = 0; i < percents.length; i++) {
                percentiles[i] = Double.NaN;
            }
        } else {
            for (int i = 0; i < percents.length; i++) {
                int index = (int) Math.round((percents[i] / 100.0) * (data.size() - 1));
                percentiles[i] = data.get(index);
            }
        }

        // todo need postCollection() to clean up temp sorted data?

        return new InternalPercentilesBucket(name(), percents, percentiles, keyed, format, metadata);
    }
}

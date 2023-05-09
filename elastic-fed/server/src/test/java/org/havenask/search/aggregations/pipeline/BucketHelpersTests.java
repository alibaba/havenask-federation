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

import org.havenask.common.io.stream.StreamOutput;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.search.aggregations.AggregationExecutionException;
import org.havenask.search.aggregations.Aggregations;
import org.havenask.search.aggregations.InternalMultiBucketAggregation;
import org.havenask.search.aggregations.bucket.MultiBucketsAggregation;
import org.havenask.search.aggregations.metrics.InternalTDigestPercentiles;
import org.havenask.test.HavenaskTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

public class BucketHelpersTests extends HavenaskTestCase {

    public void testReturnsObjectArray() {

        MultiBucketsAggregation agg = new MultiBucketsAggregation() {
            @Override
            public List<? extends Bucket> getBuckets() {
                return null;
            }

            @Override
            public String getName() {
                return "foo";
            }

            @Override
            public String getType() {
                return null;
            }

            @Override
            public Map<String, Object> getMetadata() {
                return null;
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                return null;
            }
        };

        InternalMultiBucketAggregation.InternalBucket bucket = new InternalMultiBucketAggregation.InternalBucket() {
            @Override
            public void writeTo(StreamOutput out) throws IOException {

            }

            @Override
            public Object getKey() {
                return null;
            }

            @Override
            public String getKeyAsString() {
                return null;
            }

            @Override
            public long getDocCount() {
                return 0;
            }

            @Override
            public Aggregations getAggregations() {
                return null;
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                return null;
            }

            @Override
            public Object getProperty(String containingAggName, List<String> path) {
                return new Object[0];
            }
        };

        AggregationExecutionException e = expectThrows(AggregationExecutionException.class,
            () -> BucketHelpers.resolveBucketValue(agg, bucket, "foo>bar", BucketHelpers.GapPolicy.SKIP));

        assertThat(e.getMessage(), equalTo("buckets_path must reference either a number value or a single value numeric " +
            "metric aggregation, got: [Object[]] at aggregation [foo]"));
    }

    public void testReturnMultiValueObject() {

        MultiBucketsAggregation agg = new MultiBucketsAggregation() {
            @Override
            public List<? extends Bucket> getBuckets() {
                return null;
            }

            @Override
            public String getName() {
                return "foo";
            }

            @Override
            public String getType() {
                return null;
            }

            @Override
            public Map<String, Object> getMetadata() {
                return null;
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                return null;
            }
        };

        InternalMultiBucketAggregation.InternalBucket bucket = new InternalMultiBucketAggregation.InternalBucket() {
            @Override
            public void writeTo(StreamOutput out) throws IOException {

            }

            @Override
            public Object getKey() {
                return null;
            }

            @Override
            public String getKeyAsString() {
                return null;
            }

            @Override
            public long getDocCount() {
                return 0;
            }

            @Override
            public Aggregations getAggregations() {
                return null;
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                return null;
            }

            @Override
            public Object getProperty(String containingAggName, List<String> path) {
                return mock(InternalTDigestPercentiles.class);
            }
        };

        AggregationExecutionException e = expectThrows(AggregationExecutionException.class,
            () -> BucketHelpers.resolveBucketValue(agg, bucket, "foo>bar", BucketHelpers.GapPolicy.SKIP));

        assertThat(e.getMessage(), equalTo("buckets_path must reference either a number value or a single value numeric " +
            "metric aggregation, but [foo] contains multiple values. Please specify which to use."));
    }
}

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

package org.havenask.search.aggregations.bucket.filter;

import org.havenask.search.aggregations.InternalAggregation;
import org.havenask.search.aggregations.InternalAggregation.ReduceContext;
import org.havenask.search.aggregations.InternalAggregations;
import org.havenask.search.aggregations.InternalSingleBucketAggregationTestCase;
import org.havenask.search.aggregations.bucket.ParsedSingleBucketAggregation;
import org.havenask.search.aggregations.pipeline.PipelineAggregator;
import org.havenask.search.aggregations.pipeline.PipelineAggregator.PipelineTree;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.sameInstance;

public class InternalFilterTests extends InternalSingleBucketAggregationTestCase<InternalFilter> {
    @Override
    protected InternalFilter createTestInstance(String name, long docCount, InternalAggregations aggregations,
            Map<String, Object> metadata) {
        return new InternalFilter(name, docCount, aggregations, metadata);
    }

    @Override
    protected void extraAssertReduced(InternalFilter reduced, List<InternalFilter> inputs) {
        // Nothing extra to assert
    }

    @Override
    protected Class<? extends ParsedSingleBucketAggregation> implementationClass() {
        return ParsedFilter.class;
    }

    public void testReducePipelinesReturnsSameInstanceWithoutPipelines() {
        InternalFilter test = createTestInstance();
        assertThat(test.reducePipelines(test, emptyReduceContextBuilder().forFinalReduction(), PipelineTree.EMPTY), sameInstance(test));
    }

    public void testReducePipelinesReducesBucketPipelines() {
        /*
         * Tests that a pipeline buckets by creating a mock pipeline that
         * replaces "inner" with "dummy".
         */
        InternalFilter dummy = createTestInstance();
        InternalFilter inner = createTestInstance();

        InternalAggregations sub = InternalAggregations.from(Collections.singletonList(inner));
        InternalFilter test = createTestInstance("test", randomNonNegativeLong(), sub, emptyMap());
        PipelineAggregator mockPipeline = new PipelineAggregator(null, null, null) {
            @Override
            public InternalAggregation reduce(InternalAggregation aggregation, ReduceContext reduceContext) {
                return dummy;
            }
        };
        PipelineTree tree = new PipelineTree(
            org.havenask.common.collect.Map.of(inner.getName(), new PipelineTree(emptyMap(), singletonList(mockPipeline))),
            emptyList());
        InternalFilter reduced = (InternalFilter) test.reducePipelines(test, emptyReduceContextBuilder().forFinalReduction(), tree);
        assertThat(reduced.getAggregations().get(dummy.getName()), sameInstance(dummy));
    }
}

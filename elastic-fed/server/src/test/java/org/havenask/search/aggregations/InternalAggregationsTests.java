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

package org.havenask.search.aggregations;

import org.apache.lucene.util.BytesRef;
import org.havenask.Version;
import org.havenask.common.io.stream.BytesStreamOutput;
import org.havenask.common.io.stream.NamedWriteableAwareStreamInput;
import org.havenask.common.io.stream.NamedWriteableRegistry;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.settings.Settings;
import org.havenask.search.DocValueFormat;
import org.havenask.search.SearchModule;
import org.havenask.search.aggregations.bucket.histogram.InternalDateHistogramTests;
import org.havenask.search.aggregations.bucket.terms.StringTerms;
import org.havenask.search.aggregations.bucket.terms.StringTermsTests;
import org.havenask.search.aggregations.pipeline.AvgBucketPipelineAggregationBuilder;
import org.havenask.search.aggregations.pipeline.InternalSimpleValueTests;
import org.havenask.search.aggregations.pipeline.MaxBucketPipelineAggregationBuilder;
import org.havenask.search.aggregations.pipeline.PipelineAggregator;
import org.havenask.search.aggregations.pipeline.SiblingPipelineAggregator;
import org.havenask.search.aggregations.pipeline.SumBucketPipelineAggregationBuilder;
import org.havenask.test.HavenaskTestCase;
import org.havenask.test.InternalAggregationTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.equalTo;

public class InternalAggregationsTests extends HavenaskTestCase {

    private final NamedWriteableRegistry registry = new NamedWriteableRegistry(
        new SearchModule(Settings.EMPTY, false, Collections.emptyList()).getNamedWriteables());

    public void testReduceEmptyAggs() {
        List<InternalAggregations> aggs = Collections.emptyList();
        InternalAggregation.ReduceContextBuilder builder = InternalAggregationTestCase.emptyReduceContextBuilder();
        InternalAggregation.ReduceContext reduceContext = randomBoolean() ? builder.forFinalReduction() : builder.forPartialReduction();
        assertNull(InternalAggregations.reduce(aggs, reduceContext));
    }

    public void testNonFinalReduceTopLevelPipelineAggs()  {
        InternalAggregation terms = new StringTerms("name", BucketOrder.key(true), BucketOrder.key(true),
            10, 1, Collections.emptyMap(), DocValueFormat.RAW, 25, false, 10, Collections.emptyList(), 0);
        List<InternalAggregations> aggs = singletonList(InternalAggregations.from(Collections.singletonList(terms)));
        InternalAggregations reducedAggs = InternalAggregations.topLevelReduce(aggs, maxBucketReduceContext().forPartialReduction());
        assertEquals(1, reducedAggs.getTopLevelPipelineAggregators().size());
        assertEquals(1, reducedAggs.aggregations.size());
    }

    public void testFinalReduceTopLevelPipelineAggs()  {
        InternalAggregation terms = new StringTerms("name", BucketOrder.key(true), BucketOrder.key(true),
            10, 1, Collections.emptyMap(), DocValueFormat.RAW, 25, false, 10, Collections.emptyList(), 0);

        InternalAggregations aggs = InternalAggregations.from(Collections.singletonList(terms));
        InternalAggregations reducedAggs = InternalAggregations.topLevelReduce(Collections.singletonList(aggs),
                maxBucketReduceContext().forFinalReduction());
        assertEquals(0, reducedAggs.getTopLevelPipelineAggregators().size());
        assertEquals(2, reducedAggs.aggregations.size());
    }

    private InternalAggregation.ReduceContextBuilder maxBucketReduceContext() {
        MaxBucketPipelineAggregationBuilder maxBucketPipelineAggregationBuilder = new MaxBucketPipelineAggregationBuilder("test", "test");
        PipelineAggregator.PipelineTree tree =
                new PipelineAggregator.PipelineTree(emptyMap(), singletonList(maxBucketPipelineAggregationBuilder.create()));
        return InternalAggregationTestCase.emptyReduceContextBuilder(tree);
    }

    public static InternalAggregations createTestInstance() throws Exception {
        return createTestInstance(randomPipelineTree());
    }

    public static InternalAggregations createTestInstance(PipelineAggregator.PipelineTree pipelineTree) throws Exception {
        List<InternalAggregation> aggsList = new ArrayList<>();
        if (randomBoolean()) {
            StringTermsTests stringTermsTests = new StringTermsTests();
            stringTermsTests.init();
            stringTermsTests.setUp();
            aggsList.add(stringTermsTests.createTestInstance());
        }
        if (randomBoolean()) {
            InternalDateHistogramTests dateHistogramTests = new InternalDateHistogramTests();
            dateHistogramTests.setUp();
            aggsList.add(dateHistogramTests.createTestInstance());
        }
        if (randomBoolean()) {
            InternalSimpleValueTests simpleValueTests = new InternalSimpleValueTests();
            aggsList.add(simpleValueTests.createTestInstance());
        }
        return new InternalAggregations(aggsList, () -> pipelineTree);
    }

    private static PipelineAggregator.PipelineTree randomPipelineTree() {
        List<PipelineAggregator> topLevelPipelineAggs = new ArrayList<>();
        if (randomBoolean()) {
            if (randomBoolean()) {
                topLevelPipelineAggs.add((SiblingPipelineAggregator)new MaxBucketPipelineAggregationBuilder("name1", "bucket1").create());
            }
            if (randomBoolean()) {
                topLevelPipelineAggs.add((SiblingPipelineAggregator)new AvgBucketPipelineAggregationBuilder("name2", "bucket2").create());
            }
            if (randomBoolean()) {
                topLevelPipelineAggs.add((SiblingPipelineAggregator)new SumBucketPipelineAggregationBuilder("name3", "bucket3").create());
            }
        }
        return new PipelineAggregator.PipelineTree(emptyMap(), topLevelPipelineAggs);
    }

    public void testSerialization() throws Exception {
        InternalAggregations aggregations = createTestInstance();
        writeToAndReadFrom(aggregations, Version.CURRENT, 0);
    }

    public void testSerializedSize() throws Exception {
        InternalAggregations aggregations = createTestInstance();
        assertThat(aggregations.getSerializedSize(),
            equalTo((long) serialize(aggregations, Version.CURRENT).length));
    }

    private void writeToAndReadFrom(InternalAggregations aggregations, Version version, int iteration) throws IOException {
        BytesRef serializedAggs = serialize(aggregations, version);
        try (StreamInput in = new NamedWriteableAwareStreamInput(StreamInput.wrap(serializedAggs.bytes), registry)) {
            in.setVersion(version);
            InternalAggregations deserialized = InternalAggregations.readFrom(in);
            assertEquals(aggregations.aggregations, deserialized.aggregations);
            if (iteration < 2) {
                writeToAndReadFrom(deserialized, version, iteration + 1);
            }
        }
    }

    private BytesRef serialize(InternalAggregations aggs, Version version) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setVersion(version);
            aggs.writeTo(out);
            return out.bytes().toBytesRef();
        }
    }
}

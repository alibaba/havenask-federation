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

import static java.util.Collections.emptyList;
import static org.havenask.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;
import static org.hamcrest.Matchers.hasSize;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.havenask.cluster.metadata.Metadata;
import org.havenask.common.io.stream.BytesStreamOutput;
import org.havenask.common.io.stream.NamedWriteableAwareStreamInput;
import org.havenask.common.io.stream.NamedWriteableRegistry;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.settings.Settings;
import org.havenask.common.xcontent.NamedXContentRegistry;
import org.havenask.common.xcontent.ToXContent;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.common.xcontent.XContentFactory;
import org.havenask.common.xcontent.XContentParser;
import org.havenask.common.xcontent.XContentType;
import org.havenask.env.Environment;
import org.havenask.indices.IndicesModule;
import org.havenask.plugins.SearchPlugin;
import org.havenask.search.SearchModule;
import org.havenask.search.aggregations.PipelineAggregationBuilder.ValidationContext;
import org.havenask.search.aggregations.pipeline.AbstractPipelineAggregationBuilder;
import org.havenask.test.AbstractQueryTestCase;
import org.havenask.test.HavenaskTestCase;

public abstract class BasePipelineAggregationTestCase<AF extends AbstractPipelineAggregationBuilder<AF>> extends HavenaskTestCase {

    protected static final String STRING_FIELD_NAME = "mapped_string";
    protected static final String INT_FIELD_NAME = "mapped_int";
    protected static final String DOUBLE_FIELD_NAME = "mapped_double";
    protected static final String BOOLEAN_FIELD_NAME = "mapped_boolean";
    protected static final String DATE_FIELD_NAME = "mapped_date";

    private String[] currentTypes;

    protected String[] getCurrentTypes() {
        return currentTypes;
    }

    private NamedWriteableRegistry namedWriteableRegistry;
    private NamedXContentRegistry xContentRegistry;

    protected abstract AF createTestAggregatorFactory();

    /**
     * Setup for the whole base test class.
     */
    @Override
    public void setUp() throws Exception {
        super.setUp();
        Settings settings = Settings.builder()
            .put("node.name", AbstractQueryTestCase.class.toString())
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
            .build();
        IndicesModule indicesModule = new IndicesModule(Collections.emptyList());
        SearchModule searchModule = new SearchModule(settings, false, plugins());
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        entries.addAll(indicesModule.getNamedWriteables());
        entries.addAll(searchModule.getNamedWriteables());
        entries.addAll(additionalNamedWriteables());
        namedWriteableRegistry = new NamedWriteableRegistry(entries);
        List<NamedXContentRegistry.Entry> xContentEntries = searchModule.getNamedXContents();
        xContentEntries.addAll(additionalNamedContents());
        xContentRegistry = new NamedXContentRegistry(xContentEntries);
        //create some random type with some default field, those types will stick around for all of the subclasses
        currentTypes = new String[randomIntBetween(0, 5)];
        for (int i = 0; i < currentTypes.length; i++) {
            String type = randomAlphaOfLengthBetween(1, 10);
            currentTypes[i] = type;
        }
    }

    /**
     * Plugins to add to the test.
     */
    protected List<SearchPlugin> plugins() {
        return emptyList();
    }

    /**
     * Any extra named writeables required not registered by {@link SearchModule}
     */
    protected List<NamedWriteableRegistry.Entry> additionalNamedWriteables() {
        return emptyList();
    }

    /**
     * Any extra named xcontents required not registered by {@link SearchModule}
     */
    protected List<NamedXContentRegistry.Entry> additionalNamedContents() {
        return emptyList();
    }

    /**
     * Generic test that creates new AggregatorFactory from the test
     * AggregatorFactory and checks both for equality and asserts equality on
     * the two queries.
     */
    public void testFromXContent() throws IOException {
        AF testAgg = createTestAggregatorFactory();
        AggregatorFactories.Builder factoriesBuilder = AggregatorFactories.builder().addPipelineAggregator(testAgg);
        logger.info("Content string: {}", factoriesBuilder);
        XContentBuilder builder = XContentFactory.contentBuilder(randomFrom(XContentType.values()));
        if (randomBoolean()) {
            builder.prettyPrint();
        }
        factoriesBuilder.toXContent(builder, ToXContent.EMPTY_PARAMS);
        XContentBuilder shuffled = shuffleXContent(builder);
        try (XContentParser parser = createParser(shuffled)) {
            String contentString = factoriesBuilder.toString();
            logger.info("Content string: {}", contentString);
            PipelineAggregationBuilder newAgg = parse(parser);
            assertNotSame(newAgg, testAgg);
            assertEquals(testAgg, newAgg);
            assertEquals(testAgg.hashCode(), newAgg.hashCode());
        }
    }

    protected PipelineAggregationBuilder parse(XContentParser parser) throws IOException {
        assertSame(XContentParser.Token.START_OBJECT, parser.nextToken());
        AggregatorFactories.Builder parsed = AggregatorFactories.parseAggregators(parser);
        assertThat(parsed.getAggregatorFactories(), hasSize(0));
        assertThat(parsed.getPipelineAggregatorFactories(), hasSize(1));
        PipelineAggregationBuilder newAgg = parsed.getPipelineAggregatorFactories().iterator().next();
        assertNull(parser.nextToken());
        assertNotNull(newAgg);
        return newAgg;
    }

    /**
     * Test serialization and deserialization of the test AggregatorFactory.
     */
    public void testSerialization() throws IOException {
        AF testAgg = createTestAggregatorFactory();
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.writeNamedWriteable(testAgg);
            try (StreamInput in = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), namedWriteableRegistry)) {
                PipelineAggregationBuilder deserializedQuery = in.readNamedWriteable(PipelineAggregationBuilder.class);
                assertEquals(deserializedQuery, testAgg);
                assertEquals(deserializedQuery.hashCode(), testAgg.hashCode());
                assertNotSame(deserializedQuery, testAgg);
            }
        }
    }


    public void testEqualsAndHashcode() throws IOException {
        // TODO we only change name and boost, we should extend by any sub-test supplying a "mutate" method that randomly changes one
        // aspect of the object under test
        checkEqualsAndHashCode(createTestAggregatorFactory(), this::copyAggregation);
    }

    // we use the streaming infra to create a copy of the query provided as
    // argument
    private AF copyAggregation(AF agg) throws IOException {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.writeNamedWriteable(agg);
            try (StreamInput in = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), namedWriteableRegistry)) {
                @SuppressWarnings("unchecked")
                AF secondAgg = (AF) in.readNamedWriteable(PipelineAggregationBuilder.class);
                return secondAgg;
            }
        }
    }

    protected String[] getRandomTypes() {
        String[] types;
        if (currentTypes.length > 0 && randomBoolean()) {
            int numberOfQueryTypes = randomIntBetween(1, currentTypes.length);
            types = new String[numberOfQueryTypes];
            for (int i = 0; i < numberOfQueryTypes; i++) {
                types[i] = randomFrom(currentTypes);
            }
        } else {
            if (randomBoolean()) {
                types = new String[]{Metadata.ALL};
            } else {
                types = new String[0];
            }
        }
        return types;
    }

    public String randomNumericField() {
        int randomInt = randomInt(3);
        switch (randomInt) {
            case 0:
                return DATE_FIELD_NAME;
            case 1:
                return DOUBLE_FIELD_NAME;
            case 2:
            default:
                return INT_FIELD_NAME;
        }
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return xContentRegistry;
    }

    /**
     * Helper for testing validation.
     */
    protected String validate(AggregationBuilder parent, AF builder) {
        return validate(ValidationContext.forInsideTree(parent, null), builder);
    }

    /**
     * Helper for testing validation.
     */
    protected String validate(Collection<AggregationBuilder> siblingAggregations, AF builder) {
        return validate(siblingAggregations, emptyList(), builder);
    }

    /**
     * Helper for testing validation.
     */
    protected String validate(Collection<AggregationBuilder> siblingAggregations,
            Collection<PipelineAggregationBuilder> siblingPipelineAggregations, AF builder) {
        return validate(ValidationContext.forTreeRoot(siblingAggregations, siblingPipelineAggregations, null), builder);
    }

    /**
     * Helper for testing validation.
     */
    protected String validate(ValidationContext context, AF builder) {
        builder.validate(context);
        return context.getValidationException() == null ? null : context.getValidationException().getMessage();
    }
}

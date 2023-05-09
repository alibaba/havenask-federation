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

package org.havenask.action.admin.indices.rollover;

import org.havenask.action.ActionRequestValidationException;
import org.havenask.action.admin.indices.create.CreateIndexRequest;
import org.havenask.action.admin.indices.create.CreateIndexRequestTests;
import org.havenask.common.bytes.BytesArray;
import org.havenask.common.bytes.BytesReference;
import org.havenask.common.io.stream.BytesStreamOutput;
import org.havenask.common.io.stream.NamedWriteableAwareStreamInput;
import org.havenask.common.io.stream.NamedWriteableRegistry;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.unit.ByteSizeUnit;
import org.havenask.common.unit.ByteSizeValue;
import org.havenask.common.unit.TimeValue;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.common.xcontent.XContentFactory;
import org.havenask.common.xcontent.XContentHelper;
import org.havenask.common.xcontent.XContentParseException;
import org.havenask.common.xcontent.XContentType;
import org.havenask.index.RandomCreateIndexGenerator;
import org.havenask.index.mapper.MapperService;
import org.havenask.indices.IndicesModule;
import org.havenask.test.HavenaskTestCase;
import org.havenask.test.XContentTestUtils;
import org.havenask.test.hamcrest.HavenaskAssertions;
import org.havenask.action.admin.indices.rollover.Condition;
import org.havenask.action.admin.indices.rollover.MaxAgeCondition;
import org.havenask.action.admin.indices.rollover.MaxDocsCondition;
import org.havenask.action.admin.indices.rollover.MaxSizeCondition;
import org.havenask.action.admin.indices.rollover.RolloverRequest;

import java.io.IOException;
import org.junit.Before;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.havenask.common.xcontent.ToXContent.EMPTY_PARAMS;
import static org.hamcrest.Matchers.equalTo;

public class RolloverRequestTests extends HavenaskTestCase {
    private NamedWriteableRegistry writeableRegistry;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        writeableRegistry = new NamedWriteableRegistry(new IndicesModule(Collections.emptyList()).getNamedWriteables());
    }

    public void testConditionsParsing() throws Exception {
        final RolloverRequest request = new RolloverRequest(randomAlphaOfLength(10), randomAlphaOfLength(10));
        final XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
                .startObject("conditions")
                    .field("max_age", "10d")
                    .field("max_docs", 100)
                    .field("max_size", "45gb")
                .endObject()
            .endObject();
        request.fromXContent(false, createParser(builder));
        Map<String, Condition<?>> conditions = request.getConditions();
        assertThat(conditions.size(), equalTo(3));
        MaxAgeCondition maxAgeCondition = (MaxAgeCondition)conditions.get(MaxAgeCondition.NAME);
        assertThat(maxAgeCondition.value.getMillis(), equalTo(TimeValue.timeValueHours(24 * 10).getMillis()));
        MaxDocsCondition maxDocsCondition = (MaxDocsCondition)conditions.get(MaxDocsCondition.NAME);
        assertThat(maxDocsCondition.value, equalTo(100L));
        MaxSizeCondition maxSizeCondition = (MaxSizeCondition)conditions.get(MaxSizeCondition.NAME);
        assertThat(maxSizeCondition.value.getBytes(), equalTo(ByteSizeUnit.GB.toBytes(45)));
    }

    public void testParsingWithIndexSettings() throws Exception {
        final RolloverRequest request = new RolloverRequest(randomAlphaOfLength(10), randomAlphaOfLength(10));
        final XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
                .startObject("conditions")
                    .field("max_age", "10d")
                    .field("max_docs", 100)
                .endObject()
                .startObject("mappings")
                    .startObject("type1")
                        .startObject("properties")
                            .startObject("field1")
                                .field("type", "string")
                                .field("index", "not_analyzed")
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject()
                .startObject("settings")
                    .field("number_of_shards", 10)
                .endObject()
                .startObject("aliases")
                    .startObject("alias1").endObject()
                .endObject()
            .endObject();
        request.fromXContent(true, createParser(builder));
        Map<String, Condition<?>> conditions = request.getConditions();
        assertThat(conditions.size(), equalTo(2));
        assertThat(request.getCreateIndexRequest().mappings().size(), equalTo(1));
        assertThat(request.getCreateIndexRequest().aliases().size(), equalTo(1));
        assertThat(request.getCreateIndexRequest().settings().getAsInt("number_of_shards", 0), equalTo(10));
    }

    public void testTypelessMappingParsing() throws Exception {
        final RolloverRequest request = new RolloverRequest(randomAlphaOfLength(10), randomAlphaOfLength(10));
        final XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
                .startObject("mappings")
                    .startObject("properties")
                        .startObject("field1")
                            .field("type", "keyword")
                        .endObject()
                    .endObject()
                .endObject()
            .endObject();

        boolean includeTypeName = false;
        request.fromXContent(includeTypeName, createParser(builder));

        CreateIndexRequest createIndexRequest = request.getCreateIndexRequest();
        String mapping = createIndexRequest.mappings().get(MapperService.SINGLE_MAPPING_NAME);
        assertNotNull(mapping);

        Map<String, Object> parsedMapping = XContentHelper.convertToMap(
            new BytesArray(mapping), false, XContentType.JSON).v2();

        @SuppressWarnings("unchecked")
        Map<String, Object> properties = (Map<String, Object>) parsedMapping.get(MapperService.SINGLE_MAPPING_NAME);
        assertNotNull(properties);
        assertFalse(properties.isEmpty());
    }

    public void testSerialize() throws Exception {
        RolloverRequest originalRequest = new RolloverRequest("alias-index", "new-index-name");
        originalRequest.addMaxIndexDocsCondition(randomNonNegativeLong());
        originalRequest.addMaxIndexAgeCondition(TimeValue.timeValueNanos(randomNonNegativeLong()));
        originalRequest.addMaxIndexSizeCondition(new ByteSizeValue(randomNonNegativeLong()));
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            originalRequest.writeTo(out);
            BytesReference bytes = out.bytes();
            try (StreamInput in = new NamedWriteableAwareStreamInput(bytes.streamInput(), writeableRegistry)) {
                RolloverRequest cloneRequest = new RolloverRequest(in);
                assertThat(cloneRequest.getNewIndexName(), equalTo(originalRequest.getNewIndexName()));
                assertThat(cloneRequest.getRolloverTarget(), equalTo(originalRequest.getRolloverTarget()));
                for (Map.Entry<String, Condition<?>> entry : cloneRequest.getConditions().entrySet()) {
                    Condition<?> condition = originalRequest.getConditions().get(entry.getKey());
                    //here we compare the string representation as there is some information loss when serializing
                    //and de-serializing MaxAgeCondition
                    assertEquals(condition.toString(), entry.getValue().toString());
                }
            }
        }
    }

    public void testToAndFromXContent() throws IOException {
        RolloverRequest rolloverRequest = createTestItem();

        final XContentType xContentType = randomFrom(XContentType.values());
        boolean humanReadable = randomBoolean();
        BytesReference originalBytes = toShuffledXContent(rolloverRequest, xContentType, EMPTY_PARAMS, humanReadable);

        RolloverRequest parsedRolloverRequest = new RolloverRequest();
        parsedRolloverRequest.fromXContent(true, createParser(xContentType.xContent(), originalBytes));

        CreateIndexRequest createIndexRequest = rolloverRequest.getCreateIndexRequest();
        CreateIndexRequest parsedCreateIndexRequest = parsedRolloverRequest.getCreateIndexRequest();
        CreateIndexRequestTests.assertMappingsEqual(createIndexRequest.mappings(), parsedCreateIndexRequest.mappings());
        CreateIndexRequestTests.assertAliasesEqual(createIndexRequest.aliases(), parsedCreateIndexRequest.aliases());
        assertEquals(createIndexRequest.settings(), parsedCreateIndexRequest.settings());
        assertEquals(rolloverRequest.getConditions(), parsedRolloverRequest.getConditions());

        BytesReference finalBytes = toShuffledXContent(parsedRolloverRequest, xContentType, EMPTY_PARAMS, humanReadable);
        HavenaskAssertions.assertToXContentEquivalent(originalBytes, finalBytes, xContentType);
    }

    public void testUnknownFields() throws IOException {
        final RolloverRequest request = new RolloverRequest();
        XContentType xContentType = randomFrom(XContentType.values());
        final XContentBuilder builder = XContentFactory.contentBuilder(xContentType);
        builder.startObject();
        {
            builder.startObject("conditions");
            builder.field("max_age", "10d");
            builder.endObject();
        }
        builder.endObject();
        BytesReference mutated = XContentTestUtils.insertRandomFields(xContentType, BytesReference.bytes(builder), null, random());
        expectThrows(XContentParseException.class, () -> request.fromXContent(false, createParser(xContentType.xContent(), mutated)));
    }

    public void testSameConditionCanOnlyBeAddedOnce() {
        RolloverRequest rolloverRequest = new RolloverRequest();
        Consumer<RolloverRequest> rolloverRequestConsumer = randomFrom(conditionsGenerator);
        rolloverRequestConsumer.accept(rolloverRequest);
        expectThrows(IllegalArgumentException.class, () -> rolloverRequestConsumer.accept(rolloverRequest));
    }

    public void testValidation() {
        RolloverRequest rolloverRequest = new RolloverRequest();
        assertNotNull(rolloverRequest.getCreateIndexRequest());
        ActionRequestValidationException validationException = rolloverRequest.validate();
        assertNotNull(validationException);
        assertEquals(1, validationException.validationErrors().size());
        assertEquals("rollover target is missing", validationException.validationErrors().get(0));
    }

    private static List<Consumer<RolloverRequest>> conditionsGenerator = new ArrayList<>();
    static {
        conditionsGenerator.add((request) -> request.addMaxIndexDocsCondition(randomNonNegativeLong()));
        conditionsGenerator.add((request) -> request.addMaxIndexSizeCondition(new ByteSizeValue(randomNonNegativeLong())));
        conditionsGenerator.add((request) -> request.addMaxIndexAgeCondition(new TimeValue(randomNonNegativeLong())));
    }

    private static RolloverRequest createTestItem() throws IOException {
        RolloverRequest rolloverRequest = new RolloverRequest();
        if (randomBoolean()) {
            String type = randomAlphaOfLengthBetween(3, 10);
            rolloverRequest.getCreateIndexRequest().mapping(type, RandomCreateIndexGenerator.randomMapping(type));
        }
        if (randomBoolean()) {
            RandomCreateIndexGenerator.randomAliases(rolloverRequest.getCreateIndexRequest());
        }
        if (randomBoolean()) {
            rolloverRequest.getCreateIndexRequest().settings(RandomCreateIndexGenerator.randomIndexSettings());
        }
        int numConditions = randomIntBetween(0, 3);
        List<Consumer<RolloverRequest>> conditions = randomSubsetOf(numConditions, conditionsGenerator);
        for (Consumer<RolloverRequest> consumer : conditions) {
            consumer.accept(rolloverRequest);
        }
        return rolloverRequest;
    }
}

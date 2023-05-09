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

package org.havenask.search.profile;

import org.havenask.common.Strings;
import org.havenask.common.bytes.BytesReference;
import org.havenask.common.xcontent.ToXContent;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.common.xcontent.XContentFactory;
import org.havenask.common.xcontent.XContentParser;
import org.havenask.common.xcontent.XContentType;
import org.havenask.test.HavenaskTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static org.havenask.common.xcontent.XContentHelper.toXContent;
import static org.havenask.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.havenask.test.XContentTestUtils.insertRandomFields;
import static org.havenask.test.hamcrest.HavenaskAssertions.assertToXContentEquivalent;

public class ProfileResultTests extends HavenaskTestCase {

    public static ProfileResult createTestItem(int depth) {
        String type = randomAlphaOfLengthBetween(5, 10);
        String description = randomAlphaOfLengthBetween(5, 10);
        int breakdownsSize = randomIntBetween(0, 5);
        Map<String, Long> breakdown = new HashMap<>(breakdownsSize);
        while (breakdown.size() < breakdownsSize) {
            long value = randomNonNegativeLong();
            if (randomBoolean()) {
                // also often use "small" values in tests
                value = value % 10000;
            }
            breakdown.put(randomAlphaOfLengthBetween(5, 10), value);
        }
        int debugSize = randomIntBetween(0, 5);
        Map<String, Object> debug = new HashMap<>(debugSize);
        while (debug.size() < debugSize) {
            debug.put(randomAlphaOfLength(5), randomAlphaOfLength(4));
        }
        int childrenSize = depth > 0 ? randomIntBetween(0, 1) : 0;
        List<ProfileResult> children = new ArrayList<>(childrenSize);
        for (int i = 0; i < childrenSize; i++) {
            children.add(createTestItem(depth - 1));
        }
        return new ProfileResult(type, description, breakdown, debug, randomNonNegativeLong(), children);
    }

    public void testFromXContent() throws IOException {
        doFromXContentTestWithRandomFields(false);
    }

    /**
     * This test adds random fields and objects to the xContent rendered out to ensure we can parse it
     * back to be forward compatible with additions to the xContent
     */
    public void testFromXContentWithRandomFields() throws IOException {
        doFromXContentTestWithRandomFields(true);
    }

    private void doFromXContentTestWithRandomFields(boolean addRandomFields) throws IOException {
        ProfileResult profileResult = createTestItem(2);
        XContentType xContentType = randomFrom(XContentType.values());
        boolean humanReadable = randomBoolean();
        BytesReference originalBytes = toShuffledXContent(profileResult, xContentType, ToXContent.EMPTY_PARAMS, humanReadable);
        BytesReference mutated;
        if (addRandomFields) {
            // "breakdown" and "debug" just consists of key/value pairs, we shouldn't add anything random there
            Predicate<String> excludeFilter = (s) ->
                s.endsWith(ProfileResult.BREAKDOWN.getPreferredName()) || s.endsWith(ProfileResult.DEBUG.getPreferredName());
            mutated = insertRandomFields(xContentType, originalBytes, excludeFilter, random());
        } else {
            mutated = originalBytes;
        }
        ProfileResult parsed;
        try (XContentParser parser = createParser(xContentType.xContent(), mutated)) {
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
            parsed = ProfileResult.fromXContent(parser);
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
            assertNull(parser.nextToken());
        }
        assertEquals(profileResult.getTime(), parsed.getTime());
        assertToXContentEquivalent(originalBytes, toXContent(parsed, xContentType, humanReadable), xContentType);
    }

    public void testToXContent() throws IOException {
        List<ProfileResult> children = new ArrayList<>();
        children.add(new ProfileResult(
            "child1",
            "desc1",
            org.havenask.common.collect.Map.of("key1", 100L),
            org.havenask.common.collect.Map.of(),
            100L,
            org.havenask.common.collect.List.of())
        );
        children.add(new ProfileResult(
            "child2",
            "desc2",
            org.havenask.common.collect.Map.of("key1", 123356L),
            org.havenask.common.collect.Map.of(),
            123356L,
            org.havenask.common.collect.List.of()));
        Map<String, Long> breakdown = new LinkedHashMap<>();
        breakdown.put("key1", 123456L);
        breakdown.put("stuff", 10000L);
        Map<String, Object> debug = new LinkedHashMap<>();
        debug.put("a", "foo");
        debug.put("b", "bar");
        ProfileResult result = new ProfileResult("someType", "some description", breakdown, debug, 223456L, children);
        XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
        result.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertEquals("{\n" +
                "  \"type\" : \"someType\",\n" +
                "  \"description\" : \"some description\",\n" +
                "  \"time_in_nanos\" : 223456,\n" +
                "  \"breakdown\" : {\n" +
                "    \"key1\" : 123456,\n" +
                "    \"stuff\" : 10000\n" +
                "  },\n" +
                "  \"debug\" : {\n" +
                "    \"a\" : \"foo\",\n" +
                "    \"b\" : \"bar\"\n" +
                "  },\n" +
                "  \"children\" : [\n" +
                "    {\n" +
                "      \"type\" : \"child1\",\n" +
                "      \"description\" : \"desc1\",\n" +
                "      \"time_in_nanos\" : 100,\n" +
                "      \"breakdown\" : {\n" +
                "        \"key1\" : 100\n" +
                "      }\n" +
                "    },\n" +
                "    {\n" +
                "      \"type\" : \"child2\",\n" +
                "      \"description\" : \"desc2\",\n" +
                "      \"time_in_nanos\" : 123356,\n" +
                "      \"breakdown\" : {\n" +
                "        \"key1\" : 123356\n" +
                "      }\n" +
                "    }\n" +
                "  ]\n" +
          "}", Strings.toString(builder));

        builder = XContentFactory.jsonBuilder().prettyPrint().humanReadable(true);
        result.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertEquals("{\n" +
                "  \"type\" : \"someType\",\n" +
                "  \"description\" : \"some description\",\n" +
                "  \"time\" : \"223.4micros\",\n" +
                "  \"time_in_nanos\" : 223456,\n" +
                "  \"breakdown\" : {\n" +
                "    \"key1\" : 123456,\n" +
                "    \"stuff\" : 10000\n" +
                "  },\n" +
                "  \"debug\" : {\n" +
                "    \"a\" : \"foo\",\n" +
                "    \"b\" : \"bar\"\n" +
                "  },\n" +
                "  \"children\" : [\n" +
                "    {\n" +
                "      \"type\" : \"child1\",\n" +
                "      \"description\" : \"desc1\",\n" +
                "      \"time\" : \"100nanos\",\n" +
                "      \"time_in_nanos\" : 100,\n" +
                "      \"breakdown\" : {\n" +
                "        \"key1\" : 100\n" +
                "      }\n" +
                "    },\n" +
                "    {\n" +
                "      \"type\" : \"child2\",\n" +
                "      \"description\" : \"desc2\",\n" +
                "      \"time\" : \"123.3micros\",\n" +
                "      \"time_in_nanos\" : 123356,\n" +
                "      \"breakdown\" : {\n" +
                "        \"key1\" : 123356\n" +
                "      }\n" +
                "    }\n" +
                "  ]\n" +
          "}", Strings.toString(builder));

        result = new ProfileResult(
            "profileName",
            "some description",
            org.havenask.common.collect.Map.of("key1", 12345678L),
            org.havenask.common.collect.Map.of(),
            12345678L,
            org.havenask.common.collect.List.of()
        );
        builder = XContentFactory.jsonBuilder().prettyPrint().humanReadable(true);
        result.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertEquals("{\n" +
                "  \"type\" : \"profileName\",\n" +
                "  \"description\" : \"some description\",\n" +
                "  \"time\" : \"12.3ms\",\n" +
                "  \"time_in_nanos\" : 12345678,\n" +
                "  \"breakdown\" : {\n" +
                "    \"key1\" : 12345678\n" +
                "  }\n" +
              "}", Strings.toString(builder));

        result = new ProfileResult(
            "profileName",
            "some description",
            org.havenask.common.collect.Map.of("key1", 1234567890L),
            org.havenask.common.collect.Map.of(),
            1234567890L,
            org.havenask.common.collect.List.of()
        );
        builder = XContentFactory.jsonBuilder().prettyPrint().humanReadable(true);
        result.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertEquals("{\n" +
                "  \"type\" : \"profileName\",\n" +
                "  \"description\" : \"some description\",\n" +
                "  \"time\" : \"1.2s\",\n" +
                "  \"time_in_nanos\" : 1234567890,\n" +
                "  \"breakdown\" : {\n" +
                "    \"key1\" : 1234567890\n" +
                "  }\n" +
              "}", Strings.toString(builder));
    }
}

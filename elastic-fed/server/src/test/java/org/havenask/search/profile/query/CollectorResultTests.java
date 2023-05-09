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

package org.havenask.search.profile.query;

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
import java.util.Collections;
import java.util.List;

import static org.havenask.common.xcontent.XContentHelper.toXContent;
import static org.havenask.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.havenask.test.XContentTestUtils.insertRandomFields;
import static org.havenask.test.hamcrest.HavenaskAssertions.assertToXContentEquivalent;

public class CollectorResultTests extends HavenaskTestCase {

    public static CollectorResult createTestItem(int depth) {
        String name = randomAlphaOfLengthBetween(5, 10);
        String reason = randomAlphaOfLengthBetween(5, 10);
        long time = randomNonNegativeLong();
        if (randomBoolean()) {
            // also often use relatively "small" values, otherwise we will mostly test huge longs
            time = time % 100000;
        }
        int size = randomIntBetween(0, 5);
        List<CollectorResult> children = new ArrayList<>(size);
        if (depth > 0) {
            for (int i = 0; i < size; i++) {
                children.add(createTestItem(depth - 1));
            }
        }
        return new CollectorResult(name, reason, time, children);
    }

    public void testFromXContent() throws IOException {
        doFromXContentTestWithRandomFields(false);
    }

    public void testFromXContentWithRandomFields() throws IOException {
        doFromXContentTestWithRandomFields(true);
    }

    private void doFromXContentTestWithRandomFields(boolean addRandomFields) throws IOException {
        CollectorResult collectorResult = createTestItem(1);
        XContentType xContentType = randomFrom(XContentType.values());
        boolean humanReadable = randomBoolean();
        BytesReference originalBytes = toShuffledXContent(collectorResult, xContentType, ToXContent.EMPTY_PARAMS, humanReadable);
        BytesReference mutated;
        if (addRandomFields) {
            mutated = insertRandomFields(xContentType, originalBytes, null, random());
        } else {
            mutated = originalBytes;
        }
        try (XContentParser parser = createParser(xContentType.xContent(), mutated)) {
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
            CollectorResult parsed = CollectorResult.fromXContent(parser);
            assertNull(parser.nextToken());
            assertToXContentEquivalent(originalBytes, toXContent(parsed, xContentType, humanReadable), xContentType);
        }
    }

    public void testToXContent() throws IOException {
        List<CollectorResult> children = new ArrayList<>();
        children.add(new CollectorResult("child1", "reason1", 100L, Collections.emptyList()));
        children.add(new CollectorResult("child2", "reason1", 123356L, Collections.emptyList()));
        CollectorResult result = new CollectorResult("collectorName", "some reason", 123456L, children);
        XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
        result.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertEquals("{\n" +
            "  \"name\" : \"collectorName\",\n" +
            "  \"reason\" : \"some reason\",\n" +
            "  \"time_in_nanos\" : 123456,\n" +
            "  \"children\" : [\n" +
            "    {\n" +
            "      \"name\" : \"child1\",\n" +
            "      \"reason\" : \"reason1\",\n" +
            "      \"time_in_nanos\" : 100\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\" : \"child2\",\n" +
            "      \"reason\" : \"reason1\",\n" +
            "      \"time_in_nanos\" : 123356\n" +
            "    }\n" +
            "  ]\n" +
          "}", Strings.toString(builder));

        builder = XContentFactory.jsonBuilder().prettyPrint().humanReadable(true);
        result.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertEquals("{\n" +
            "  \"name\" : \"collectorName\",\n" +
            "  \"reason\" : \"some reason\",\n" +
            "  \"time\" : \"123.4micros\",\n" +
            "  \"time_in_nanos\" : 123456,\n" +
            "  \"children\" : [\n" +
            "    {\n" +
            "      \"name\" : \"child1\",\n" +
            "      \"reason\" : \"reason1\",\n" +
            "      \"time\" : \"100nanos\",\n" +
            "      \"time_in_nanos\" : 100\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\" : \"child2\",\n" +
            "      \"reason\" : \"reason1\",\n" +
            "      \"time\" : \"123.3micros\",\n" +
            "      \"time_in_nanos\" : 123356\n" +
            "    }\n" +
            "  ]\n" +
          "}", Strings.toString(builder));

        result = new CollectorResult("collectorName", "some reason", 12345678L, Collections.emptyList());
        builder = XContentFactory.jsonBuilder().prettyPrint().humanReadable(true);
        result.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertEquals("{\n" +
                "  \"name\" : \"collectorName\",\n" +
                "  \"reason\" : \"some reason\",\n" +
                "  \"time\" : \"12.3ms\",\n" +
                "  \"time_in_nanos\" : 12345678\n" +
              "}", Strings.toString(builder));

        result = new CollectorResult("collectorName", "some reason", 1234567890L, Collections.emptyList());
        builder = XContentFactory.jsonBuilder().prettyPrint().humanReadable(true);
        result.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertEquals("{\n" +
                "  \"name\" : \"collectorName\",\n" +
                "  \"reason\" : \"some reason\",\n" +
                "  \"time\" : \"1.2s\",\n" +
                "  \"time_in_nanos\" : 1234567890\n" +
              "}", Strings.toString(builder));
    }
}

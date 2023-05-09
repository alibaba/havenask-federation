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

package org.havenask.action.search;

import org.havenask.common.Strings;
import org.havenask.common.bytes.BytesReference;
import org.havenask.common.xcontent.ToXContent;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.common.xcontent.XContentFactory;
import org.havenask.common.xcontent.XContentHelper;
import org.havenask.common.xcontent.XContentParser;
import org.havenask.common.xcontent.XContentType;
import org.havenask.common.xcontent.json.JsonXContent;
import org.havenask.test.HavenaskTestCase;

import java.io.IOException;

import static org.havenask.test.hamcrest.HavenaskAssertions.assertToXContentEquivalent;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.startsWith;

public class ClearScrollRequestTests extends HavenaskTestCase {

    public void testFromXContent() throws Exception {
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        if (randomBoolean()) {
            //test that existing values get overridden
            clearScrollRequest = createClearScrollRequest();
        }
        try (XContentParser parser = createParser(XContentFactory.jsonBuilder()
                .startObject()
                .array("scroll_id", "value_1", "value_2")
                .endObject())) {
            clearScrollRequest.fromXContent(parser);
        }
        assertThat(clearScrollRequest.scrollIds(), contains("value_1", "value_2"));
    }

    public void testFromXContentWithoutArray() throws Exception {
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        if (randomBoolean()) {
            //test that existing values get overridden
            clearScrollRequest = createClearScrollRequest();
        }
        try (XContentParser parser = createParser(XContentFactory.jsonBuilder()
                .startObject()
                .field("scroll_id", "value_1")
                .endObject())) {
            clearScrollRequest.fromXContent(parser);
        }
        assertThat(clearScrollRequest.scrollIds(), contains("value_1"));
    }

    public void testFromXContentWithUnknownParamThrowsException() throws Exception {
        XContentParser invalidContent = createParser(XContentFactory.jsonBuilder()
                .startObject()
                .array("scroll_id", "value_1", "value_2")
                .field("unknown", "keyword")
                .endObject());
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();

        Exception e = expectThrows(IllegalArgumentException.class, () -> clearScrollRequest.fromXContent(invalidContent));
        assertThat(e.getMessage(), startsWith("Unknown parameter [unknown]"));
    }

    public void testToXContent() throws IOException {
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId("SCROLL_ID");
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            clearScrollRequest.toXContent(builder, ToXContent.EMPTY_PARAMS);
            assertEquals("{\"scroll_id\":[\"SCROLL_ID\"]}", Strings.toString(builder));
        }
    }

    public void testFromAndToXContent() throws IOException {
        XContentType xContentType = randomFrom(XContentType.values());
        ClearScrollRequest originalRequest = createClearScrollRequest();
        BytesReference originalBytes = toShuffledXContent(originalRequest, xContentType, ToXContent.EMPTY_PARAMS, randomBoolean());
        ClearScrollRequest parsedRequest = new ClearScrollRequest();
        try (XContentParser parser = createParser(xContentType.xContent(), originalBytes)) {
            parsedRequest.fromXContent(parser);
        }
        assertEquals(originalRequest.scrollIds(), parsedRequest.scrollIds());
        BytesReference parsedBytes = XContentHelper.toXContent(parsedRequest, xContentType, randomBoolean());
        assertToXContentEquivalent(originalBytes, parsedBytes, xContentType);
    }

    public static ClearScrollRequest createClearScrollRequest() {
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        int numScrolls = randomIntBetween(1, 10);
        for (int i = 0; i < numScrolls; i++) {
            clearScrollRequest.addScrollId(randomAlphaOfLengthBetween(3, 10));
        }
        return clearScrollRequest;
    }
}

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

package org.havenask.search.suggest;

import org.havenask.common.bytes.BytesReference;
import org.havenask.common.text.Text;
import org.havenask.common.xcontent.ToXContent;
import org.havenask.common.xcontent.XContentParser;
import org.havenask.common.xcontent.XContentType;
import org.havenask.search.suggest.Suggest.Suggestion.Entry.Option;
import org.havenask.search.suggest.phrase.PhraseSuggestion;
import org.havenask.test.HavenaskTestCase;

import java.io.IOException;

import static org.havenask.common.xcontent.XContentHelper.toXContent;
import static org.havenask.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.havenask.test.XContentTestUtils.insertRandomFields;
import static org.havenask.test.hamcrest.HavenaskAssertions.assertToXContentEquivalent;

public class SuggestionOptionTests extends HavenaskTestCase {

    public static Option createTestItem() {
        Text text = new Text(randomAlphaOfLengthBetween(5, 15));
        float score = randomFloat();
        Text highlighted = randomFrom((Text) null, new Text(randomAlphaOfLengthBetween(5, 15)));
        Boolean collateMatch = randomFrom((Boolean) null, randomBoolean());
        return new PhraseSuggestion.Entry.Option(text, highlighted, score, collateMatch);
    }

    public void testFromXContent() throws IOException {
        doTestFromXContent(false);
    }

    public void testFromXContentWithRandomFields() throws IOException {
        doTestFromXContent(true);
    }

    private void doTestFromXContent(boolean addRandomFields) throws IOException {
        Option option = createTestItem();
        XContentType xContentType = randomFrom(XContentType.values());
        boolean humanReadable = randomBoolean();
        BytesReference originalBytes = toShuffledXContent(option, xContentType, ToXContent.EMPTY_PARAMS, humanReadable);
        BytesReference mutated;
        if (addRandomFields) {
            mutated = insertRandomFields(xContentType, originalBytes, null, random());
        } else {
            mutated = originalBytes;
        }
        Option parsed;
        try (XContentParser parser = createParser(xContentType.xContent(), mutated)) {
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
            parsed = PhraseSuggestion.Entry.Option.fromXContent(parser);
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
            assertNull(parser.nextToken());
        }
        assertEquals(option.getText(), parsed.getText());
        assertEquals(option.getHighlighted(), parsed.getHighlighted());
        assertEquals(option.getScore(), parsed.getScore(), Float.MIN_VALUE);
        assertEquals(option.collateMatch(), parsed.collateMatch());
        assertToXContentEquivalent(originalBytes, toXContent(parsed, xContentType, humanReadable), xContentType);
    }

    public void testToXContent() throws IOException {
        Option option = new PhraseSuggestion.Entry.Option(new Text("someText"), new Text("somethingHighlighted"), 1.3f, true);
        BytesReference xContent = toXContent(option, XContentType.JSON, randomBoolean());
        assertEquals(
            ("{"
                + "  \"text\": \"someText\","
                + "  \"highlighted\": \"somethingHighlighted\","
                + "  \"score\": 1.3,"
                + "  \"collate_match\": true"
                + "}").replaceAll("\\s+", ""),
            xContent.utf8ToString()
        );
    }
}

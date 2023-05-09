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

package org.havenask.action.explain;

import org.apache.lucene.search.Explanation;
import org.havenask.common.bytes.BytesArray;
import org.havenask.common.bytes.BytesReference;
import org.havenask.common.document.DocumentField;
import org.havenask.common.io.stream.Writeable;
import org.havenask.common.xcontent.ToXContent;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.common.xcontent.XContentFactory;
import org.havenask.common.xcontent.XContentParser;
import org.havenask.common.xcontent.XContentType;
import org.havenask.index.get.GetResult;
import org.havenask.test.AbstractSerializingTestCase;
import org.havenask.test.RandomObjects;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.equalTo;

public class ExplainResponseTests extends AbstractSerializingTestCase<ExplainResponse> {

    @Override
    protected ExplainResponse doParseInstance(XContentParser parser) throws IOException {
        return ExplainResponse.fromXContent(parser, randomBoolean());
    }

    @Override
    protected Writeable.Reader<ExplainResponse> instanceReader() {
        return ExplainResponse::new;
    }

    @Override
    protected ExplainResponse createTestInstance() {
        String index = randomAlphaOfLength(5);
        String type = randomAlphaOfLength(5);
        String id = String.valueOf(randomIntBetween(1,100));
        boolean exist = randomBoolean();
        Explanation explanation = randomExplanation(randomExplanation(randomExplanation()), randomExplanation());
        String fieldName = randomAlphaOfLength(10);
        List<Object> values = Arrays.asList(randomAlphaOfLengthBetween(3, 10), randomInt(), randomLong(), randomDouble(), randomBoolean());
        GetResult getResult = new GetResult(randomAlphaOfLengthBetween(3, 10),
            randomAlphaOfLengthBetween(3, 10),
            randomAlphaOfLengthBetween(3, 10),
            0, 1, randomNonNegativeLong(),
            true,
            RandomObjects.randomSource(random()),
            singletonMap(fieldName, new DocumentField(fieldName, values)), null);
        return new ExplainResponse(index, type, id, exist, explanation, getResult);
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return field -> field.equals("get") || field.startsWith("get.fields") || field.startsWith("get._source");
    }

    public void testToXContent() throws IOException {
        String index = "index";
        String type = "type";
        String id = "1";
        boolean exist = true;
        Explanation explanation = Explanation.match(1.0f, "description", Collections.emptySet());
        GetResult getResult = new GetResult(null, null, null, 0, 1, -1, true, new BytesArray("{ \"field1\" : " +
            "\"value1\", \"field2\":\"value2\"}"), singletonMap("field1", new DocumentField("field1",
            singletonList("value1"))), null);
        ExplainResponse response = new ExplainResponse(index, type, id, exist, explanation, getResult);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);

        String generatedResponse = BytesReference.bytes(builder).utf8ToString().replaceAll("\\s+", "");

        String expectedResponse =
            ("{\n" +
            "    \"_index\":\"index\",\n" +
            "    \"_type\":\"type\",\n" +
            "    \"_id\":\"1\",\n" +
            "    \"matched\":true,\n" +
            "    \"explanation\":{\n" +
            "        \"value\":1.0,\n" +
            "        \"description\":\"description\",\n" +
            "        \"details\":[]\n" +
            "    },\n" +
            "    \"get\":{\n" +
            "        \"_seq_no\":0," +
            "        \"_primary_term\":1," +
            "        \"found\":true,\n" +
            "        \"_source\":{\n" +
            "            \"field1\":\"value1\",\n" +
            "            \"field2\":\"value2\"\n" +
            "        },\n" +
            "        \"fields\":{\n" +
            "            \"field1\":[\n" +
            "                \"value1\"\n" +
            "            ]\n" +
            "        }\n" +
            "    }\n" +
            "}").replaceAll("\\s+", "");
        assertThat(expectedResponse, equalTo(generatedResponse));
    }

    private static Explanation randomExplanation(Explanation... explanations) {
        return Explanation.match(randomFloat(), randomAlphaOfLengthBetween(1, 10),
            explanations.length > 0 ? explanations : new Explanation[0]);
    }
}

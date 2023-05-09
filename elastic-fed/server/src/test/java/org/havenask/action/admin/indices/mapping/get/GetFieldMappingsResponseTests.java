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

package org.havenask.action.admin.indices.mapping.get;

import org.havenask.action.admin.indices.mapping.get.GetFieldMappingsResponse;
import org.havenask.action.admin.indices.mapping.get.GetFieldMappingsResponse.FieldMappingMetadata;
import org.havenask.common.bytes.BytesArray;
import org.havenask.common.io.stream.BytesStreamOutput;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.io.stream.Writeable;
import org.havenask.common.xcontent.LoggingDeprecationHandler;
import org.havenask.common.xcontent.ToXContent;
import org.havenask.common.xcontent.XContentParser;
import org.havenask.common.xcontent.XContentType;
import org.havenask.test.AbstractSerializingTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

import static org.havenask.rest.BaseRestHandler.INCLUDE_TYPE_NAME_PARAMETER;
import static org.hamcrest.CoreMatchers.equalTo;

public class GetFieldMappingsResponseTests extends AbstractSerializingTestCase<GetFieldMappingsResponse> {

    public void testManualSerialization() throws IOException {
        Map<String, Map<String, Map<String, FieldMappingMetadata>>> mappings = new HashMap<>();
        FieldMappingMetadata fieldMappingMetadata = new FieldMappingMetadata("my field", new BytesArray("{}"));
        mappings.put("index", Collections.singletonMap("type", Collections.singletonMap("field", fieldMappingMetadata)));
        GetFieldMappingsResponse response = new GetFieldMappingsResponse(mappings);

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            response.writeTo(out);
            try (StreamInput in = StreamInput.wrap(out.bytes().toBytesRef().bytes)) {
                GetFieldMappingsResponse serialized = new GetFieldMappingsResponse(in);
                FieldMappingMetadata metadata = serialized.fieldMappings("index", "type", "field");
                assertNotNull(metadata);
                assertEquals(new BytesArray("{}"), metadata.getSource());
            }
        }
    }

    public void testManualJunkedJson() throws Exception {
        // in fact random fields could be evaluated as proper mapping, while proper junk in this case is arrays and values
        final String json =
            "{\"index1\":{\"mappings\":"
                + "{\"doctype0\":{\"field1\":{\"full_name\":\"my field\",\"mapping\":{\"type\":\"keyword\"}},"
                    + "\"field0\":{\"full_name\":\"my field\",\"mapping\":{\"type\":\"keyword\"}}},"
                // junk here
                + "\"junk1\": [\"field1\", {\"field2\":{}}],"
                + "\"junk2\": [{\"field3\":{}}],"
                + "\"junk3\": 42,"
                + "\"junk4\": \"Q\","
                + "\"doctype1\":{\"field1\":{\"full_name\":\"my field\",\"mapping\":{\"type\":\"keyword\"}},"
                    + "\"field0\":{\"full_name\":\"my field\",\"mapping\":{\"type\":\"keyword\"}}}}},"
            + "\"index0\":{\"mappings\":"
                + "{\"doctype0\":{\"field1\":{\"full_name\":\"my field\",\"mapping\":{\"type\":\"keyword\"}},"
                + "\"field0\":{\"full_name\":\"my field\",\"mapping\":{\"type\":\"keyword\"}}},"
                + "\"doctype1\":{\"field1\":{\"full_name\":\"my field\",\"mapping\":{\"type\":\"keyword\"}},"
                + "\"field0\":{\"full_name\":\"my field\",\"mapping\":{\"type\":\"keyword\"}}}}}}";

        final XContentParser parser = XContentType.JSON.xContent().createParser(xContentRegistry(),
            LoggingDeprecationHandler.INSTANCE, json.getBytes("UTF-8"));

        final GetFieldMappingsResponse response = GetFieldMappingsResponse.fromXContent(parser);

        FieldMappingMetadata fieldMappingMetadata =
            new FieldMappingMetadata("my field", new BytesArray("{\"type\":\"keyword\"}"));
        Map<String, FieldMappingMetadata> fieldMapping = new HashMap<>();
        fieldMapping.put("field0", fieldMappingMetadata);
        fieldMapping.put("field1", fieldMappingMetadata);

        Map<String, Map<String, FieldMappingMetadata>> typeMapping = new HashMap<>();
        typeMapping.put("doctype0", fieldMapping);
        typeMapping.put("doctype1", fieldMapping);

        Map<String, Map<String, Map<String, FieldMappingMetadata>>> mappings = new HashMap<>();
        mappings.put("index0", typeMapping);
        mappings.put("index1", typeMapping);

        final Map<String, Map<String, Map<String, FieldMappingMetadata>>> responseMappings = response.mappings();
        assertThat(responseMappings, equalTo(mappings));
    }

    @Override
    protected GetFieldMappingsResponse doParseInstance(XContentParser parser) throws IOException {
        return GetFieldMappingsResponse.fromXContent(parser);
    }

    @Override
    protected GetFieldMappingsResponse createTestInstance() {
        return new GetFieldMappingsResponse(randomMapping());
    }

    @Override
    protected Writeable.Reader<GetFieldMappingsResponse> instanceReader() {
        return GetFieldMappingsResponse::new;
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        // allow random fields at the level of `index` and `index.mappings.doctype.field`
        // otherwise random field could be evaluated as index name or type name
        return s -> false == (s.matches("(?<index>[^.]+)")
            || s.matches("(?<index>[^.]+)\\.mappings\\.(?<doctype>[^.]+)\\.(?<field>[^.]+)"));
    }

    /**
     * For xContent roundtrip testing we force the xContent output to still contain types because the parser
     * still expects them. The new typeless parsing is implemented in the client side GetFieldMappingsResponse.
     */
    @Override
    protected ToXContent.Params getToXContentParams() {
        return new ToXContent.MapParams(Collections.singletonMap(INCLUDE_TYPE_NAME_PARAMETER, "true"));
    }

    private Map<String, Map<String, Map<String, FieldMappingMetadata>>> randomMapping() {
        Map<String, Map<String, Map<String, FieldMappingMetadata>>> mappings = new HashMap<>();

        int indices = randomInt(10);
        for(int i = 0; i < indices; i++) {
            final Map<String, Map<String, FieldMappingMetadata>> doctypesMappings = new HashMap<>();
            int doctypes = randomInt(10);
            for(int j = 0; j < doctypes; j++) {
                Map<String, FieldMappingMetadata> fieldMappings = new HashMap<>();
                int fields = randomInt(10);
                for(int k = 0; k < fields; k++) {
                    final String mapping = randomBoolean() ? "{\"type\":\"string\"}" : "{\"type\":\"keyword\"}";
                    FieldMappingMetadata metadata =
                        new FieldMappingMetadata("my field", new BytesArray(mapping));
                    fieldMappings.put("field" + k, metadata);
                }
                doctypesMappings.put("doctype" + j, fieldMappings);
            }
            mappings.put("index" + i, doctypesMappings);
        }
        return mappings;
    }
}

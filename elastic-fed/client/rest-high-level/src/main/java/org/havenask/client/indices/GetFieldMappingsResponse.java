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

package org.havenask.client.indices;

import org.havenask.common.ParseField;
import org.havenask.common.bytes.BytesReference;
import org.havenask.common.xcontent.ConstructingObjectParser;
import org.havenask.common.xcontent.ObjectParser;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.common.xcontent.XContentHelper;
import org.havenask.common.xcontent.XContentParser;
import org.havenask.common.xcontent.XContentType;
import org.havenask.index.mapper.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.havenask.common.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.havenask.common.xcontent.XContentFactory.jsonBuilder;
import static org.havenask.common.xcontent.XContentParserUtils.ensureExpectedToken;

/** Response object for {@link GetFieldMappingsRequest} API */
public class GetFieldMappingsResponse {

    private static final ParseField MAPPINGS = new ParseField("mappings");

    private static final ObjectParser<Map<String, FieldMappingMetadata>, String> PARSER =
        new ObjectParser<>(MAPPINGS.getPreferredName(), true, HashMap::new);

    static {
        PARSER.declareField((p, fieldMappings, index) -> {
            p.nextToken();
            while (p.currentToken() == XContentParser.Token.FIELD_NAME) {
                final String fieldName = p.currentName();
                final FieldMappingMetadata fieldMappingMetadata = FieldMappingMetadata.fromXContent(p);
                fieldMappings.put(fieldName, fieldMappingMetadata);
                p.nextToken();
            }
        }, MAPPINGS, ObjectParser.ValueType.OBJECT);
    }

    private Map<String, Map<String, FieldMappingMetadata>> mappings;

    GetFieldMappingsResponse(Map<String, Map<String, FieldMappingMetadata>> mappings) {
        this.mappings = mappings;
    }


     /**
     * Returns the fields mapping. The return map keys are indexes and fields (as specified in the request).
     */
    public Map<String, Map<String, FieldMappingMetadata>> mappings() {
        return mappings;
    }

    /**
     * Returns the mappings of a specific index and field.
     *
     * @param field field name as specified in the {@link GetFieldMappingsRequest}
     * @return FieldMappingMetadata for the requested field or null if not found.
     */
    public FieldMappingMetadata fieldMappings(String index, String field) {
        Map<String, FieldMappingMetadata> indexMapping = mappings.get(index);
        if (indexMapping == null) {
            return null;
        }
        return indexMapping.get(field);
    }


    public static GetFieldMappingsResponse fromXContent(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        final Map<String, Map<String, FieldMappingMetadata>> mappings = new HashMap<>();
        if (parser.nextToken() == XContentParser.Token.FIELD_NAME) {
            while (parser.currentToken() == XContentParser.Token.FIELD_NAME) {
                final String index = parser.currentName();
                final Map<String, FieldMappingMetadata> fieldMappings = PARSER.parse(parser, index);
                mappings.put(index, fieldMappings);
                parser.nextToken();
            }
        }
        return new GetFieldMappingsResponse(mappings);
    }

    public static class FieldMappingMetadata {
        private static final ParseField FULL_NAME = new ParseField("full_name");
        private static final ParseField MAPPING = new ParseField("mapping");

        private static final ConstructingObjectParser<FieldMappingMetadata, String> PARSER =
            new ConstructingObjectParser<>("field_mapping_meta_data", true,
                a -> new FieldMappingMetadata((String)a[0], (BytesReference)a[1])
            );

        static {
            PARSER.declareField(optionalConstructorArg(),
                (p, c) -> p.text(), FULL_NAME, ObjectParser.ValueType.STRING);
            PARSER.declareField(optionalConstructorArg(),
                (p, c) -> {
                    final XContentBuilder jsonBuilder = jsonBuilder().copyCurrentStructure(p);
                    final BytesReference bytes = BytesReference.bytes(jsonBuilder);
                    return bytes;
                }, MAPPING, ObjectParser.ValueType.OBJECT);
        }

        private String fullName;
        private BytesReference source;

        public FieldMappingMetadata(String fullName, BytesReference source) {
            this.fullName = fullName;
            this.source = source;
        }

        public String fullName() {
            return fullName;
        }

        /**
         * Returns the mappings as a map. Note that the returned map has a single key which is always the field's {@link Mapper#name}.
         */
        public Map<String, Object> sourceAsMap() {
            return XContentHelper.convertToMap(source, true, XContentType.JSON).v2();
        }

        //pkg-private for testing
        BytesReference getSource() {
            return source;
        }

        public static FieldMappingMetadata fromXContent(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }

       @Override
        public String toString() {
            return "FieldMappingMetadata{fullName='" + fullName + '\'' + ", source=" + source + '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof FieldMappingMetadata)) return false;
            FieldMappingMetadata that = (FieldMappingMetadata) o;
            return Objects.equals(fullName, that.fullName) && Objects.equals(source, that.source);
        }

        @Override
        public int hashCode() {
            return Objects.hash(fullName, source);
        }
    }


    @Override
    public String toString() {
        return "GetFieldMappingsResponse{" +  "mappings=" + mappings + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof GetFieldMappingsResponse)) return false;
        GetFieldMappingsResponse that = (GetFieldMappingsResponse) o;
        return Objects.equals(mappings, that.mappings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(mappings);
    }

}

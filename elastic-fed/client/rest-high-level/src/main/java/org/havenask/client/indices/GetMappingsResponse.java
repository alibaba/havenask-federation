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

import org.havenask.cluster.metadata.MappingMetadata;
import org.havenask.common.ParseField;
import org.havenask.common.xcontent.XContentParser;
import org.havenask.common.xcontent.XContentParserUtils;
import org.havenask.index.mapper.MapperService;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class GetMappingsResponse {

    static final ParseField MAPPINGS = new ParseField("mappings");

    private Map<String, MappingMetadata> mappings;

    public GetMappingsResponse(Map<String, MappingMetadata> mappings) {
        this.mappings = mappings;
    }

    public Map<String, MappingMetadata> mappings() {
        return mappings;
    }

    public static GetMappingsResponse fromXContent(XContentParser parser) throws IOException {
        if (parser.currentToken() == null) {
            parser.nextToken();
        }

        XContentParserUtils.ensureExpectedToken(parser.currentToken(),
            XContentParser.Token.START_OBJECT,
            parser);

        Map<String, Object> parts = parser.map();

        Map<String, MappingMetadata> mappings = new HashMap<>();
        for (Map.Entry<String, Object> entry : parts.entrySet()) {
            String indexName = entry.getKey();
            assert entry.getValue() instanceof Map : "expected a map as type mapping, but got: " + entry.getValue().getClass();

            @SuppressWarnings("unchecked")
            final Map<String, Object> fieldMappings = (Map<String, Object>) ((Map<String, ?>) entry.getValue())
                    .get(MAPPINGS.getPreferredName());

            mappings.put(indexName, new MappingMetadata(MapperService.SINGLE_MAPPING_NAME, fieldMappings));
        }

        return new GetMappingsResponse(mappings);
    }
}

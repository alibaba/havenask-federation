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
import org.havenask.common.collect.ImmutableOpenMap;
import org.havenask.common.xcontent.ToXContent;
import org.havenask.common.xcontent.ToXContent.Params;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.index.mapper.MapperService;
import org.havenask.rest.BaseRestHandler;
import org.havenask.test.HavenaskTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;

import static org.havenask.client.indices.GetMappingsResponse.MAPPINGS;
import static org.havenask.test.AbstractXContentTestCase.xContentTester;

public class GetMappingsResponseTests extends HavenaskTestCase {

    // Because the client-side class does not have a toXContent method, we test xContent serialization by creating
    // a random client object, converting it to a server object then serializing it to xContent, and finally
    // parsing it back as a client object. We check equality between the original client object, and the parsed one.
    public void testFromXContent() throws IOException {
        xContentTester(
            this::createParser,
            GetMappingsResponseTests::createTestInstance,
            GetMappingsResponseTests::toXContent,
            GetMappingsResponse::fromXContent)
            .supportsUnknownFields(true)
            .assertEqualsConsumer(GetMappingsResponseTests::assertEqualInstances)
            .randomFieldsExcludeFilter(randomFieldsExcludeFilter())
            .test();
    }

    private static GetMappingsResponse createTestInstance() {
        Map<String, MappingMetadata> mappings = Collections.singletonMap(
            "index-" + randomAlphaOfLength(5), randomMappingMetadata());
        return new GetMappingsResponse(mappings);
    }

    private static void assertEqualInstances(GetMappingsResponse expected, GetMappingsResponse actual) {
        assertEquals(expected.mappings(), actual.mappings());
    }

    private Predicate<String> randomFieldsExcludeFilter() {
        return field -> !field.equals(MAPPINGS.getPreferredName());
    }

    public static MappingMetadata randomMappingMetadata() {
        Map<String, Object> mappings = new HashMap<>();

        if (frequently()) { // rarely have no fields
            mappings.put("field1", randomFieldMapping());
            if (randomBoolean()) {
                mappings.put("field2", randomFieldMapping());
            }
        }

        try {
            return new MappingMetadata(MapperService.SINGLE_MAPPING_NAME, mappings);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static Map<String, Object> randomFieldMapping() {
        Map<String, Object> mappings = new HashMap<>();
        if (randomBoolean()) {
            mappings.put("type", randomFrom("text", "keyword"));
            mappings.put("index", "analyzed");
            mappings.put("analyzer", "english");
        } else {
            mappings.put("type", randomFrom("integer", "float", "long", "double"));
            mappings.put("index", Objects.toString(randomBoolean()));
        }
        return mappings;
    }

    private static void toXContent(GetMappingsResponse response, XContentBuilder builder) throws IOException {
        Params params = new ToXContent.MapParams(
            Collections.singletonMap(BaseRestHandler.INCLUDE_TYPE_NAME_PARAMETER, "false"));
        ImmutableOpenMap.Builder<String, ImmutableOpenMap<String, MappingMetadata>> allMappings = ImmutableOpenMap.builder();

        for (Map.Entry<String, MappingMetadata> indexEntry : response.mappings().entrySet()) {
            ImmutableOpenMap.Builder<String, MappingMetadata> mappings = ImmutableOpenMap.builder();
            mappings.put(MapperService.SINGLE_MAPPING_NAME, indexEntry.getValue());
            allMappings.put(indexEntry.getKey(), mappings.build());
        }

        org.havenask.action.admin.indices.mapping.get.GetMappingsResponse serverResponse =
            new org.havenask.action.admin.indices.mapping.get.GetMappingsResponse(allMappings.build());

        builder.startObject();
        serverResponse.toXContent(builder, params);
        builder.endObject();
    }
}

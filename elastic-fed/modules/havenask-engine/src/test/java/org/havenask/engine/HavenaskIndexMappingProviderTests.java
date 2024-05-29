/*
 * Copyright (c) 2021, Alibaba Group;
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.havenask.engine;

import org.havenask.common.collect.Map;
import org.havenask.common.settings.Settings;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.common.xcontent.XContentFactory;
import org.havenask.common.xcontent.XContentType;
import org.havenask.index.mapper.MapperService;
import org.havenask.index.mapper.MapperServiceTestCase;

import java.io.IOException;

import static org.havenask.index.engine.EngineTestCase.createMapperService;

public class HavenaskIndexMappingProviderTests extends MapperServiceTestCase {
    public void testValidateIndexMapping() throws IOException {
        String tableName = "index";
        Settings settings = Settings.builder().put("index.engine", "havenask").build();
        HavenaskIndexMappingProvider havenaskIndexMappingProvider = new HavenaskIndexMappingProvider();

        MapperService mapperService = createTestMapperService(Map.of("properties", Map.of("user", Map.of("type", "text"))));

        havenaskIndexMappingProvider.validateIndexMapping(tableName, settings, mapperService);

        MapperService illegalMapperService = createTestMapperService(Map.of("properties", Map.of("user-illegal", Map.of("type", "text"))));

        try {
            havenaskIndexMappingProvider.validateIndexMapping(tableName, settings, illegalMapperService);
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Unsupported field name [user-illegal], havenask field name cannot contain hyphen '-'"));
        }

        MapperService illegalMapperServiceContainsSummary = createTestMapperService(
            Map.of("properties", Map.of("summary", Map.of("type", "text")))
        );
        try {
            havenaskIndexMappingProvider.validateIndexMapping(tableName, settings, illegalMapperServiceContainsSummary);
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Unsupported field name [summary], havenask field name cannot be 'summary'"));
        }
    }

    private MapperService createTestMapperService(java.util.Map<String, ?> mapping) throws IOException {
        XContentBuilder Builder = XContentFactory.contentBuilder(XContentType.JSON);
        Builder.map(mapping);
        return createMapperService(Builder);
    }
}

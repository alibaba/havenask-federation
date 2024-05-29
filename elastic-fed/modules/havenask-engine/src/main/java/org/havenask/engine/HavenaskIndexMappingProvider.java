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

import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.havenask.common.settings.Settings;
import org.havenask.engine.index.config.generator.SchemaGenerator;
import org.havenask.engine.index.engine.EngineSettings;
import org.havenask.index.mapper.MapperService;
import org.havenask.index.shard.IndexMappingProvider;

public class HavenaskIndexMappingProvider implements IndexMappingProvider {
    public static final String ILLEGAL_HAVENASK_FIELD_NAME = "summary";

    @Override
    public Map<String, Object> getAdditionalIndexMapping(Settings settings) {
        if (EngineSettings.isHavenaskEngine(settings)) {
            Map<String, Object> mappings = new HashMap<>();
            mappings.put("dynamic", false);
            return mappings;
        } else {
            return Collections.emptyMap();
        }
    }

    @Override
    public void validateIndexMapping(String table, Settings indexSettings, MapperService mapperService)
        throws UnsupportedOperationException {
        if (EngineSettings.isHavenaskEngine(indexSettings)) {
            // check field name
            mapperService.documentMapper().mappers().forEach(mapper -> {
                String fieldName = mapper.name();
                if (fieldName.contains("-")) {
                    throw new IllegalArgumentException(
                        String.format(Locale.ROOT, "Unsupported field name [%s], havenask field name cannot contain hyphen '-'", fieldName)
                    );
                }
                if (fieldName.equals(ILLEGAL_HAVENASK_FIELD_NAME)) {
                    throw new IllegalArgumentException(
                        String.format(
                            Locale.ROOT,
                            "Unsupported field name [%s], havenask field name cannot be '%s'",
                            fieldName,
                            ILLEGAL_HAVENASK_FIELD_NAME
                        )
                    );
                }
            });

            SchemaGenerator generate = new SchemaGenerator();
            // validate the index mapping
            generate.getSchema(table, indexSettings, mapperService);
        }
    }
}

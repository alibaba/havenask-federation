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

import org.havenask.client.RequestOptions;
import org.havenask.client.indices.CreateIndexRequest;
import org.havenask.common.collect.Map;
import org.havenask.common.settings.Settings;
import org.havenask.engine.index.engine.EngineSettings;

import java.util.ArrayList;
import java.util.Arrays;

public class MappingIT extends AbstractHavenaskRestTestCase {
    // test unsupported data type
    public void testUnsupportedDataType() throws Exception {
        String index = "index_unsupported_data_type";

        ArrayList<String> unsupportedDataType = new ArrayList<String>(
            Arrays.asList(
                "binary",
                "constant_keyword",
                "wildcard",
                "half_float",
                "scaled_float",
                "date_nanos",
                "alias",  // Common types
                "flattened",
                "nested",
                "join",    // Object And relational types
                "integer_range",
                "float_range",
                "long_range",
                "double_range",
                "date_range",
                "ip_range",
                "ip",
                "version",
                "murmur3",     // Structured data types
                "histogram",    // Aggregate data types
                "annotated-text",
                "completion",
                "search_as_you_type",
                "token_count",    // Text search types
                "dense_vector",
                "sparse_vector",
                "rank_feature",
                "rank_features",   // Document ranking types
                "point",
                "shape",   // Spatial data types
                "percolator"    // Other types
            )
        );

        for (String curDataType : unsupportedDataType) {
            org.havenask.HavenaskStatusException ex = expectThrows(
                org.havenask.HavenaskStatusException.class,
                () -> highLevelClient().indices()
                    .create(
                        new CreateIndexRequest(index).settings(
                            Settings.builder().put(EngineSettings.ENGINE_TYPE_SETTING.getKey(), EngineSettings.ENGINE_HAVENASK).build()
                        ).mapping(Map.of("properties", Map.of("curDataType", Map.of("type", curDataType)))),
                        RequestOptions.DEFAULT
                    )
            );
            String exMessage = ex.getMessage();
            assertTrue(exMessage.contains("unsupported_operation_exception") || exMessage.contains("mapper_parsing_exception"));
        }
    }
}

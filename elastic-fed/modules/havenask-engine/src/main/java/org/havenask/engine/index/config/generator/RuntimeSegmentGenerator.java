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

package org.havenask.engine.index.config.generator;

import org.havenask.common.Nullable;
import org.havenask.common.settings.Settings;
import org.havenask.engine.index.config.Schema;
import org.havenask.engine.util.RangeUtil;
import org.havenask.index.mapper.MapperService;
import org.havenask.index.shard.ShardId;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Locale;

public class RuntimeSegmentGenerator {
    public static final String VERSION_FILE_NAME = "version.0";
    public static final String VERSION_FILE_CONTENT = "{\n"
        + "\"description\":\n"
        + "  {\n"
        + "  },\n"
        + "\"format_version\":\n"
        + "  2,\n"
        + "\"last_segmentid\":\n"
        + "  -1,\n"
        + "\"level_info\":\n"
        + "  {\n"
        + "  \"level_metas\":\n"
        + "    [\n"
        + "      {\n"
        + "      \"cursor\":\n"
        + "        0,\n"
        + "      \"level_idx\":\n"
        + "        0,\n"
        + "      \"segments\":\n"
        + "        [\n"
        + "        ],\n"
        + "      \"topology\":\n"
        + "        \"sequence\"\n"
        + "      }\n"
        + "    ]\n"
        + "  },\n"
        + "\"locator\":\n"
        + "  \"\",\n"
        + "\"schema_version\":\n"
        + "  0,\n"
        + "\"segments\":\n"
        + "  [\n"
        + "  ],\n"
        + "\"timestamp\":\n"
        + "  -1,\n"
        + "\"versionid\":\n"
        + "  0\n"
        + "}";

    public static final String INDEX_FORMAT_VERSION_FILE_NAME = "index_format_version";
    public static final String INDEX_FORMAT_VERSION_FILE_CONTENT = "{\n"
        + "\"index_format_version\":\n"
        + "  \"2.1.2\",\n"
        + "\"inverted_index_binary_format_version\":\n"
        + "  1\n"
        + "}";
    public static final String INDEX_PARTITION_META_FILE_NAME = "index_partition_meta";
    public static final String INDEX_PARTITION_META_FILE_CONTENT = "{\n" + "\"PartitionMeta\":\n" + "  [\n" + "  ]\n" + "}";
    public static final String DEPLOY_META_FILE_NAME = "deploy_meta.0";
    public static final String DEPLOY_META_FILE_CONTENT_TEMPLATE = "{\n"
        + "\"deploy_file_metas\":\n"
        + "  [\n"
        + "    {\n"
        + "    \"file_length\":\n"
        + "      82,\n"
        + "    \"modify_time\":\n"
        + "      18446744073709551615,\n"
        + "    \"path\":\n"
        + "      \"index_format_version\"\n"
        + "    },\n"
        + "    {\n"
        + "    \"file_length\":\n"
        + "      %d,\n"
        + "    \"modify_time\":\n"
        + "      18446744073709551615,\n"
        + "    \"path\":\n"
        + "      \"schema.json\"\n"
        + "    },\n"
        + "    {\n"
        + "    \"file_length\":\n"
        + "      28,\n"
        + "    \"modify_time\":\n"
        + "      18446744073709551615,\n"
        + "    \"path\":\n"
        + "      \"index_partition_meta\"\n"
        + "    },\n"
        + "    {\n"
        + "    \"file_length\":\n"
        + "      -1,\n"
        + "    \"modify_time\":\n"
        + "      18446744073709551615,\n"
        + "    \"path\":\n"
        + "      \"deploy_meta.0\"\n"
        + "    }\n"
        + "  ],\n"
        + "\"final_deploy_file_metas\":\n"
        + "  [\n"
        + "    {\n"
        + "    \"file_length\":\n"
        + "      372,\n"
        + "    \"modify_time\":\n"
        + "      18446744073709551615,\n"
        + "    \"path\":\n"
        + "      \"version.0\"\n"
        + "    }\n"
        + "  ],\n"
        + "\"lifecycle\":\n"
        + "  \"\"\n"
        + "}";

    public static final String ENTRY_TABLE_FILE_NAME = "entry_table.0";
    public static final String ENTRY_TABLE_FILE_CONTENT = "{\n"
        + "\"files\":\n"
        + "  {\n"
        + "  \"\":\n"
        + "    {\n"
        + "    \"deploy_meta.0\":\n"
        + "      {\n"
        + "      \"length\":\n"
        + "        %d\n"
        + "      },\n"
        + "    \"index_format_version\":\n"
        + "      {\n"
        + "      \"length\":\n"
        + "        82\n"
        + "      },\n"
        + "    \"index_partition_meta\":\n"
        + "      {\n"
        + "      \"length\":\n"
        + "        28\n"
        + "      },\n"
        + "    \"schema.json\":\n"
        + "      {\n"
        + "      \"length\":\n"
        + "        %d\n"
        + "      },\n"
        + "    \"version.0\":\n"
        + "      {\n"
        + "      \"length\":\n"
        + "        372\n"
        + "      }\n"
        + "    }\n"
        + "  },\n"
        + "\"package_files\":\n"
        + "  {\n"
        + "  }\n"
        + "}";

    public static final String SCHEMA_FILE_NAME = "schema.json";

    private final Path runtimedataPath;
    private final ShardId shardId;
    private final int numberOfShards;
    private final Settings indexSettings;
    private final MapperService mapperService;

    public RuntimeSegmentGenerator(
        ShardId shardId,
        int numberOfShards,
        Settings indexSettings,
        @Nullable MapperService mapperService,
        Path runtimedataPath
    ) {
        this.shardId = shardId;
        this.numberOfShards = numberOfShards;
        this.indexSettings = indexSettings;
        this.mapperService = mapperService;
        this.runtimedataPath = runtimedataPath;
    }

    public void generate() throws IOException {
        String indexName = shardId.getIndexName();
        String partitionName = RangeUtil.getRangePartition(numberOfShards, shardId.id());

        Path dataPath = runtimedataPath.resolve(indexName).resolve("generation_0").resolve(partitionName);
        if (false == Files.exists(dataPath)) {
            Files.createDirectories(dataPath);
        }

        if (Files.exists(dataPath.resolve(VERSION_FILE_NAME))) {
            return;
        }

        Files.write(
            dataPath.resolve(VERSION_FILE_NAME),
            VERSION_FILE_CONTENT.getBytes(StandardCharsets.UTF_8),
            StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING
        );

        Files.write(
            dataPath.resolve(INDEX_FORMAT_VERSION_FILE_NAME),
            INDEX_FORMAT_VERSION_FILE_CONTENT.getBytes(StandardCharsets.UTF_8),
            StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING
        );

        Files.write(
            dataPath.resolve(INDEX_PARTITION_META_FILE_NAME),
            INDEX_PARTITION_META_FILE_CONTENT.getBytes(StandardCharsets.UTF_8),
            StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING
        );

        SchemaGenerator schemaGenerator = new SchemaGenerator();
        Schema schema = schemaGenerator.getSchema(indexName, indexSettings, mapperService);
        String strSchema = schema.toString();
        Files.write(
            dataPath.resolve(SCHEMA_FILE_NAME),
            strSchema.getBytes(StandardCharsets.UTF_8),
            StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING
        );

        String strDeployMeta = String.format(Locale.ROOT, DEPLOY_META_FILE_CONTENT_TEMPLATE, strSchema.length());
        Files.write(
            dataPath.resolve(DEPLOY_META_FILE_NAME),
            strDeployMeta.getBytes(StandardCharsets.UTF_8),
            StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING
        );

        Files.write(
            dataPath.resolve(ENTRY_TABLE_FILE_NAME),
            String.format(Locale.ROOT, ENTRY_TABLE_FILE_CONTENT, strDeployMeta.length(), strSchema.length())
                .getBytes(StandardCharsets.UTF_8),
            StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING
        );
    }

    public static void generateRuntimeSegment(
        ShardId shardId,
        int numberOfShards,
        Settings indexSettings,
        MapperService mapperService,
        Path runtimedataPath
    ) throws IOException {
        RuntimeSegmentGenerator runtimeSegmentGenerator = new RuntimeSegmentGenerator(
            shardId,
            numberOfShards,
            indexSettings,
            mapperService,
            runtimedataPath
        );
        runtimeSegmentGenerator.generate();
    }
}

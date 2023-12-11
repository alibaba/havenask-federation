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

import static java.util.Collections.singletonList;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.Locale;

import org.havenask.common.settings.Settings;
import org.havenask.engine.HavenaskEnginePlugin;
import org.havenask.index.mapper.MapperService;
import org.havenask.index.mapper.MapperServiceTestCase;
import org.havenask.index.shard.ShardId;
import org.havenask.plugins.Plugin;

public class RuntimeSegmentGeneratorTests extends MapperServiceTestCase {
    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return singletonList(new HavenaskEnginePlugin(Settings.EMPTY));
    }

    public void testBasic() throws IOException {
        String indexName = randomAlphaOfLength(5);
        MapperService mapperService = createMapperService(fieldMapping(b -> b.field("type", "keyword")));
        Path runtimePath = createTempDir();
        ShardId shardId = new ShardId(indexName, "_na_", 0);
        RuntimeSegmentGenerator runtimeSegmentGenerator = new RuntimeSegmentGenerator(
            shardId,
            1,
            Settings.EMPTY,
            mapperService,
            runtimePath
        );
        runtimeSegmentGenerator.generate();

        Path dataPath = runtimePath.resolve(indexName).resolve("generation_0").resolve("partition_0_65535");
        assertTrue(Files.exists(dataPath.resolve(RuntimeSegmentGenerator.VERSION_FILE_NAME)));
        String versionContent = Files.readString(dataPath.resolve(RuntimeSegmentGenerator.VERSION_FILE_NAME));
        assertEquals(RuntimeSegmentGenerator.VERSION_FILE_CONTENT, versionContent);

        assertTrue(Files.exists(dataPath.resolve(RuntimeSegmentGenerator.INDEX_FORMAT_VERSION_FILE_NAME)));
        String indexFormatVersionContent = Files.readString(dataPath.resolve(RuntimeSegmentGenerator.INDEX_FORMAT_VERSION_FILE_NAME));
        assertEquals(RuntimeSegmentGenerator.INDEX_FORMAT_VERSION_FILE_CONTENT, indexFormatVersionContent);

        assertTrue(Files.exists(dataPath.resolve(RuntimeSegmentGenerator.INDEX_PARTITION_META_FILE_NAME)));
        String indexPartitionMetaContent = Files.readString(dataPath.resolve(RuntimeSegmentGenerator.INDEX_PARTITION_META_FILE_NAME));
        assertEquals(RuntimeSegmentGenerator.INDEX_PARTITION_META_FILE_CONTENT, indexPartitionMetaContent);

        assertTrue(Files.exists(dataPath.resolve(RuntimeSegmentGenerator.SCHEMA_FILE_NAME)));
        String schmeaContent = Files.readString(dataPath.resolve(RuntimeSegmentGenerator.SCHEMA_FILE_NAME));
        String expectSchemaContent = String.format(
            Locale.ROOT,
            "{\n"
                + "\t\"attributes\":[\"_seq_no\",\"field\",\"_id\",\"_version\",\"_primary_term\"],\n"
                + "\t\"fields\":[{\n"
                + "\t\t\"binary_field\":false,\n"
                + "\t\t\"field_name\":\"_routing\",\n"
                + "\t\t\"field_type\":\"STRING\"\n"
                + "\t},{\n"
                + "\t\t\"binary_field\":false,\n"
                + "\t\t\"field_name\":\"_seq_no\",\n"
                + "\t\t\"field_type\":\"INT64\"\n"
                + "\t},{\n"
                + "\t\t\"binary_field\":false,\n"
                + "\t\t\"field_name\":\"field\",\n"
                + "\t\t\"field_type\":\"STRING\"\n"
                + "\t},{\n"
                + "\t\t\"binary_field\":false,\n"
                + "\t\t\"field_name\":\"_source\",\n"
                + "\t\t\"field_type\":\"STRING\"\n"
                + "\t},{\n"
                + "\t\t\"binary_field\":false,\n"
                + "\t\t\"field_name\":\"_id\",\n"
                + "\t\t\"field_type\":\"STRING\"\n"
                + "\t},{\n"
                + "\t\t\"binary_field\":false,\n"
                + "\t\t\"field_name\":\"_version\",\n"
                + "\t\t\"field_type\":\"INT64\"\n"
                + "\t},{\n"
                + "\t\t\"binary_field\":false,\n"
                + "\t\t\"field_name\":\"_primary_term\",\n"
                + "\t\t\"field_type\":\"INT64\"\n"
                + "\t}],\n"
                + "\t\"indexs\":[{\n"
                + "\t\t\"has_primary_key_attribute\":true,\n"
                + "\t\t\"index_fields\":\"_id\",\n"
                + "\t\t\"index_name\":\"_id\",\n"
                + "\t\t\"index_type\":\"PRIMARYKEY64\",\n"
                + "\t\t\"is_primary_key_sorted\":false\n"
                + "\t},{\n"
                + "\t\t\"index_fields\":\"_routing\",\n"
                + "\t\t\"index_name\":\"_routing\",\n"
                + "\t\t\"index_type\":\"STRING\"\n"
                + "\t},{\n"
                + "\t\t\"index_fields\":\"_seq_no\",\n"
                + "\t\t\"index_name\":\"_seq_no\",\n"
                + "\t\t\"index_type\":\"NUMBER\"\n"
                + "\t},{\n"
                + "\t\t\"index_fields\":\"field\",\n"
                + "\t\t\"index_name\":\"field\",\n"
                + "\t\t\"index_type\":\"STRING\"\n"
                + "\t}],\n"
                + "\t\"summarys\":{\n"
                + "\t\t\"summary_fields\":[\"_routing\",\"_source\",\"_id\"]\n"
                + "\t},\n"
                + "\t\"table_name\":\"%s\",\n"
                + "\t\"table_type\":\"normal\"\n"
                + "}",
            indexName
        );
        assertEquals(expectSchemaContent, schmeaContent);

        assertTrue(Files.exists(dataPath.resolve(RuntimeSegmentGenerator.DEPLOY_META_FILE_NAME)));
        String deployMetaContent = Files.readString(dataPath.resolve(RuntimeSegmentGenerator.DEPLOY_META_FILE_NAME));
        assertEquals(
            String.format(Locale.ROOT, RuntimeSegmentGenerator.DEPLOY_META_FILE_CONTENT_TEMPLATE, schmeaContent.length()),
            deployMetaContent
        );

        assertTrue(Files.exists(dataPath.resolve(RuntimeSegmentGenerator.ENTRY_TABLE_FILE_NAME)));
        String entryTableContent = Files.readString(dataPath.resolve(RuntimeSegmentGenerator.ENTRY_TABLE_FILE_NAME));
        assertEquals(
            String.format(
                Locale.ROOT,
                RuntimeSegmentGenerator.ENTRY_TABLE_FILE_CONTENT,
                deployMetaContent.length(),
                schmeaContent.length()
            ),
            entryTableContent
        );
    }

    public void testExists() throws IOException {
        String indexName = randomAlphaOfLength(5);
        MapperService mapperService = createMapperService(fieldMapping(b -> b.field("type", "keyword")));
        Path runtimePath = createTempDir();
        ShardId shardId = new ShardId(indexName, "_na_", 0);
        RuntimeSegmentGenerator runtimeSegmentGenerator = new RuntimeSegmentGenerator(
            shardId,
            1,
            Settings.EMPTY,
            mapperService,
            runtimePath
        );

        Path dataPath = runtimePath.resolve(indexName).resolve("generation_0").resolve("partition_0_65535");
        Files.createDirectories(dataPath);

        Files.write(
            dataPath.resolve("version.1"),
            RuntimeSegmentGenerator.VERSION_FILE_CONTENT.getBytes(),
            StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING
        );

        runtimeSegmentGenerator.generate();

        assertFalse(Files.exists(dataPath.resolve(RuntimeSegmentGenerator.VERSION_FILE_NAME)));
        assertFalse(Files.exists(dataPath.resolve(RuntimeSegmentGenerator.INDEX_FORMAT_VERSION_FILE_NAME)));
        assertFalse(Files.exists(dataPath.resolve(RuntimeSegmentGenerator.INDEX_PARTITION_META_FILE_NAME)));
        assertFalse(Files.exists(dataPath.resolve(RuntimeSegmentGenerator.SCHEMA_FILE_NAME)));
        assertFalse(Files.exists(dataPath.resolve(RuntimeSegmentGenerator.DEPLOY_META_FILE_NAME)));
        assertFalse(Files.exists(dataPath.resolve(RuntimeSegmentGenerator.ENTRY_TABLE_FILE_NAME)));
    }

}

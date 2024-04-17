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

package org.havenask.engine.index.store;

import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.havenask.Version;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.common.UUIDs;
import org.havenask.common.lucene.Lucene;
import org.havenask.common.settings.Settings;
import org.havenask.engine.HavenaskEngineEnvironment;
import org.havenask.engine.util.Utils;
import org.havenask.index.IndexSettings;
import org.havenask.index.shard.ShardId;
import org.havenask.index.store.Store;
import org.havenask.index.store.Store.OnClose;
import org.havenask.index.store.StoreFileMetadata;
import org.havenask.test.DummyShardLock;
import org.havenask.test.HavenaskTestCase;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.containsString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HavenaskStoreTests extends HavenaskTestCase {
    private HavenaskStore havenaskStore;
    private Path workDir = createTempDir();
    private ShardId shardId;
    private Path dataPath;

    @Before
    public void setup() throws IOException {
        String index = "test";
        IndexMetadata build = IndexMetadata.builder(index)
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        IndexSettings indexSettings = new IndexSettings(build, Settings.EMPTY);
        shardId = new ShardId(index, index, 1);

        String tableName = Utils.getHavenaskTableName(shardId);
        dataPath = workDir.resolve(HavenaskEngineEnvironment.DEFAULT_DATA_PATH)
            .resolve(HavenaskEngineEnvironment.HAVENASK_RUNTIMEDATA_PATH)
            .resolve(tableName)
            .resolve("generation_0")
            .resolve("partition_0_65535");
        Files.createDirectories(dataPath);

        Directory directory = mock(Directory.class);
        when(directory.listAll()).thenReturn(new String[] { "existDir/existFile" });

        havenaskStore = new HavenaskStore(shardId, indexSettings, directory, new DummyShardLock(shardId), OnClose.EMPTY, dataPath);
    }

    public void testGetHavenaskMetadata() throws IOException {
        IndexCommit indexCommit = Lucene.getIndexCommit(new SegmentInfos(7), null);
        {
            // assert no version file
            NoSuchFileException e = expectThrows(NoSuchFileException.class, () -> havenaskStore.getHavenaskMetadata(indexCommit));
            assertThat(e.getMessage(), containsString("version.0"));
        }

        String versionContent = "{\n"
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
        Files.write(dataPath.resolve("version.0"), versionContent.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE);

        {
            // assert no entry table file
            NoSuchFileException e = expectThrows(NoSuchFileException.class, () -> havenaskStore.getHavenaskMetadata(indexCommit));
            assertThat(e.getMessage(), containsString("entry_table.0"));
        }

        String entryTableContent = "{\n"
            + "\"files\":\n"
            + "  {\n"
            + "  \"\":\n"
            + "    {\n"
            + "    \"deploy_meta.0\":\n"
            + "      {\n"
            + "      \"length\":\n"
            + "        711\n"
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
            + "        2335\n"
            + "      },\n"
            + "    \"version.0\":\n"
            + "      {\n"
            + "      \"length\":\n"
            + "        372\n"
            + "      }\n"
            + "    },\n"
            + "\"\\/usr\\/share\\/havenask\\/data\\/havenask\\/local_search_12000\\/general_p0_r0\\/runtimedata\\"
            + "/image_index1\\/generation_0\\/partition_0_13107\":\n"
            + "    {\n"
            + "\"segment_1_level_0\\/attribute\\/_id\":\n"
            + "      {\n"
            + "      \"length\":\n"
            + "        -2\n"
            + "      },\n"
            + "    \"segment_1_level_0\\/attribute\\/_id\\/data\":\n"
            + "      {\n"
            + "      \"length\":\n"
            + "        210\n"
            + "      },\n"
            + "    \"segment_1_level_0\\/attribute\\/_id\\/data_info\":\n"
            + "      {\n"
            + "      \"length\":\n"
            + "        72\n"
            + "      },\n"
            + "    \"segment_1_level_0\\/attribute\\/_id\\/offset\":\n"
            + "      {\n"
            + "      \"length\":\n"
            + "        44\n"
            + "      }"
            + "   },"
            + "\"\\/usr\\/share\\/havenask\\/data\\/havenask\\/local_search_12000\\/general_p0_r0\\/runtimedata\\"
            + "/image_index1\\/generation_0\\/partition_0_13107\\/__FENCE__2DQFAdcBaayEkuws5yzXoAOkVB\":\n"
            + "    {\n"
            + "    \"segment_536870931_level_0\":\n"
            + "      {\n"
            + "      \"length\":\n"
            + "        31\n"
            + "      },\n"
            + "    \"segment_536870931_level_0\\/attribute\":\n"
            + "      {\n"
            + "      \"length\":\n"
            + "        57\n"
            + "      },\n"
            + "    \"segment_536870931_level_0\\/attribute\\/_id\":\n"
            + "      {\n"
            + "      \"length\":\n"
            + "        128\n"
            + "      }"
            + "   }"
            + "  },\n"
            + "\"package_files\":\n"
            + "  {\n"
            + "  }\n"
            + "}";
        Files.write(dataPath.resolve("entry_table.0"), entryTableContent.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE);

        {
            Map<String, StoreFileMetadata> snapshot = havenaskStore.getHavenaskMetadata(indexCommit);
            assertEquals(snapshot.size(), 13);
            assertEquals(snapshot.get("deploy_meta.0").length(), 711);
            assertEquals(snapshot.get("index_format_version").length(), 82);
            assertEquals(snapshot.get("index_partition_meta").length(), 28);
            assertEquals(snapshot.get("schema.json").length(), 2335);
            assertEquals(snapshot.get("version.0").length(), 372);
            assertEquals(snapshot.get("entry_table.0").length(), 1350);
            assertEquals(snapshot.get("segment_1_level_0/attribute/_id").length(), 0);
            assertEquals(snapshot.get("segment_1_level_0/attribute/_id/data").length(), 210);
            assertEquals(snapshot.get("segment_1_level_0/attribute/_id/data_info").length(), 72);
            assertEquals(snapshot.get("segment_1_level_0/attribute/_id/offset").length(), 44);
            assertEquals(snapshot.get("__FENCE__2DQFAdcBaayEkuws5yzXoAOkVB/segment_536870931_level_0").length(), 31);
            assertEquals(snapshot.get("__FENCE__2DQFAdcBaayEkuws5yzXoAOkVB/segment_536870931_level_0/attribute").length(), 57);
            assertEquals(snapshot.get("__FENCE__2DQFAdcBaayEkuws5yzXoAOkVB/segment_536870931_level_0/attribute/_id").length(), 128);
        }

        // commit is null
        {
            Map<String, StoreFileMetadata> snapshot = havenaskStore.getHavenaskMetadata(null);
            assertEquals(snapshot.size(), 13);
        }
    }

    public void testCreateVerifyingOutput() throws IOException {
        List<String> fileLists = List.of("test", "dir1/test", "dir1/dir2/test");
        for (String fileName : fileLists) {
            String content = "test content";
            StoreFileMetadata metadata = new StoreFileMetadata(fileName, content.length(), "", HavenaskStore.HAVENASK_VERSION);
            String tempFileName = "recovery." + UUIDs.randomBase64UUID() + "." + fileName;
            IndexOutput indexOutput = havenaskStore.createVerifyingOutput(tempFileName, metadata, IOContext.DEFAULT);
            indexOutput.writeBytes(content.getBytes(StandardCharsets.UTF_8), content.length());
            indexOutput.close();

            String fileContent = Files.readString(dataPath.resolve(tempFileName));
            assertEquals(fileContent, content);
        }

        // test create directory
        String dirName = "dir1";
        StoreFileMetadata metadata = new StoreFileMetadata(dirName, 0, "DIR", HavenaskStore.HAVENASK_VERSION);
        String tempDirName = "recovery." + UUIDs.randomBase64UUID() + "." + dirName;
        IndexOutput indexOutput = havenaskStore.createVerifyingOutput(tempDirName, metadata, IOContext.DEFAULT);
        indexOutput.close();

        assertTrue(Files.exists(dataPath.resolve(tempDirName)));

        // test create exist file
        String existFileName = "existFile";
        String oldContent = "old content";
        StoreFileMetadata existMetadata = new StoreFileMetadata(existFileName, oldContent.length(), "", HavenaskStore.HAVENASK_VERSION);
        String tempExistFileName = "recovery." + UUIDs.randomBase64UUID() + "." + existFileName;
        Files.write(dataPath.resolve(tempExistFileName), oldContent.getBytes(StandardCharsets.UTF_8));
        IndexOutput existIndexOutput = havenaskStore.createVerifyingOutput(tempExistFileName, existMetadata, IOContext.DEFAULT);
        existIndexOutput.writeBytes("new content".getBytes(StandardCharsets.UTF_8), "new content".length());
        existIndexOutput.close();

        assertTrue(Files.exists(dataPath.resolve(tempExistFileName)));
        String existFileContent = Files.readString(dataPath.resolve(tempExistFileName));
        assertEquals(existFileContent, oldContent);
    }

    public void testRenameTempFilesSafe() throws IOException {
        String prefix = "recovery." + UUIDs.randomBase64UUID() + ".";
        Map<String, String> tempFileMap = new HashMap<>();
        tempFileMap.put(prefix + "test", "test");
        tempFileMap.put(prefix + "dir1/test", "dir1/test");
        tempFileMap.put(prefix + "dir1/dir2/test", "dir1/dir2/test");

        Files.createDirectories(dataPath.resolve(prefix + "dir1/dir2"));
        Files.write(dataPath.resolve(prefix + "test"), "test content".getBytes(StandardCharsets.UTF_8));
        Files.write(dataPath.resolve(prefix + "dir1/test"), "test dir1 content".getBytes(StandardCharsets.UTF_8));
        Files.write(dataPath.resolve(prefix + "dir1/dir2/test"), "test dir1 dir2 content".getBytes(StandardCharsets.UTF_8));
        havenaskStore.renameHavenaskTempFilesSafe(tempFileMap);

        assertTrue(tempFileMap.isEmpty());
        assertEquals(Files.readString(dataPath.resolve("test")), "test content");
        assertEquals(Files.readString(dataPath.resolve("dir1/test")), "test dir1 content");
        assertEquals(Files.readString(dataPath.resolve("dir1/dir2/test")), "test dir1 dir2 content");
    }

    public void testRenameExistTempFilesSafe() throws IOException {
        String prefix = "recovery." + UUIDs.randomBase64UUID() + ".";
        Map<String, String> tempFileMap = new HashMap<>();
        tempFileMap.put(prefix + "test", "test");
        tempFileMap.put(prefix + "dir1/test", "dir1/test");
        tempFileMap.put(prefix + "dir1/dir2/test", "dir1/dir2/test");

        Files.createDirectories(dataPath.resolve(prefix + "dir1/dir2"));
        Files.createDirectories(dataPath.resolve("dir1/dir2"));
        Files.write(dataPath.resolve(prefix + "test"), "test content new".getBytes(StandardCharsets.UTF_8));
        Files.write(dataPath.resolve(prefix + "dir1/test"), "test dir1 content new".getBytes(StandardCharsets.UTF_8));
        Files.write(dataPath.resolve(prefix + "dir1/dir2/test"), "test dir1 dir2 content new".getBytes(StandardCharsets.UTF_8));
        Files.write(dataPath.resolve("test"), "test content".getBytes(StandardCharsets.UTF_8));
        Files.write(dataPath.resolve("dir1/test"), "test dir1 content".getBytes(StandardCharsets.UTF_8));
        Files.write(dataPath.resolve("dir1/dir2/test"), "test dir1 dir2 content".getBytes(StandardCharsets.UTF_8));
        havenaskStore.renameHavenaskTempFilesSafe(tempFileMap);

        assertTrue(tempFileMap.isEmpty());
        assertEquals(Files.readString(dataPath.resolve("test")), "test content new");
        assertEquals(Files.readString(dataPath.resolve("dir1/test")), "test dir1 content new");
        assertEquals(Files.readString(dataPath.resolve("dir1/dir2/test")), "test dir1 dir2 content new");
    }

    public void testCleanFilesAndDirectories() throws IOException {
        String prefix = "recovery." + UUIDs.randomBase64UUID() + ".";

        String dir1Name = prefix + "dir1";
        String dir2Name = prefix + "dir1/dir2";
        String test1Name = prefix + "dir1/test1";
        String test2Name = prefix + "dir1/dir2/test2";
        String existDirName = "existDir";
        String existFileName = "existDir/existFile";

        Path dir1 = dataPath.resolve(dir1Name);
        Path dir2 = dataPath.resolve(dir2Name);
        Path test1 = dataPath.resolve(test1Name);
        Path test2 = dataPath.resolve(test2Name);
        Path existDir = dataPath.resolve(existDirName);
        Path existFile = dataPath.resolve(existFileName);

        Map<String, StoreFileMetadata> metadata = new HashMap<>();
        metadata.put(existDirName, null);
        metadata.put(existFileName, null);
        Store.MetadataSnapshot sourceMetadata = new Store.MetadataSnapshot(metadata, null, 0L);

        Files.createDirectories(dir2);
        Files.createDirectories(existDir);
        Files.write(test1, "test dir1 test1".getBytes(StandardCharsets.UTF_8));
        Files.write(test2, "test dir1 dir2 test2".getBytes(StandardCharsets.UTF_8));
        Files.write(existFile, "test existFile".getBytes(StandardCharsets.UTF_8));
        assertTrue(Files.exists(dir1));
        assertTrue(Files.exists(dir2));
        assertTrue(Files.exists(test1));
        assertTrue(Files.exists(test2));
        assertTrue(Files.exists(existDir));
        assertTrue(Files.exists(existFile));

        havenaskStore.cleanFilesAndDirectories(sourceMetadata);
        assertFalse(Files.exists(dir1));
        assertFalse(Files.exists(dir2));
        assertFalse(Files.exists(test1));
        assertFalse(Files.exists(test2));
        assertTrue(Files.exists(existDir));
        assertTrue(Files.exists(existFile));
    }

    public void testNoRewriteEntryFile() throws IOException {
        String indexName = "test";
        Path shardPath = dataPath;
        long commitVersion = 0;
        String versionFile = "version." + commitVersion;
        String content = "{\n"
            + "  \"description\":\n"
            + "  {\n"
            + "  },\n"
            + "  \"format_version\":\n"
            + "  2,\n"
            + "  \"last_segmentid\":\n"
            + "  -1,\n"
            + "  \"level_info\":\n"
            + "  {\n"
            + "    \"level_metas\":\n"
            + "    [\n"
            + "      {\n"
            + "        \"cursor\":\n"
            + "        0,\n"
            + "        \"level_idx\":\n"
            + "        0,\n"
            + "        \"segments\":\n"
            + "        [\n"
            + "        ],\n"
            + "        \"topology\":\n"
            + "        \"sequence\"\n"
            + "      }\n"
            + "    ]\n"
            + "  },\n"
            + "  \"locator\":\n"
            + "  \"\",\n"
            + "  \"schema_version\":\n"
            + "  0,\n"
            + "  \"segments\":\n"
            + "  [\n"
            + "  ],\n"
            + "  \"timestamp\":\n"
            + "  -1,\n"
            + "  \"versionid\":\n"
            + "  0\n"
            + "}";
        try {
            Files.write(shardPath.resolve(versionFile), content.getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // write schema.json
        String schemaFile = "schema.json";
        String schemaContent = "{\n"
            + "  \"description\":\n"
            + "  {\n"
            + "  },\n"
            + "  \"fields\":\n"
            + "  [\n"
            + "    {\n"
            + "      \"name\": \"_id\",\n"
            + "      \"type\": \"keyword\"\n"
            + "    }\n"
            + "  ]\n"
            + "}";
        try {
            Files.write(shardPath.resolve(schemaFile), schemaContent.getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        String entryTableFile = "entry_table." + commitVersion;
        String entryTableContent = "{\n"
            + "\t\"files\":{\n"
            + "\t\t\"\":{\n"
            + "\t\t\t\"index_format_version\":{\n"
            + "\t\t\t\t\"length\":82\n"
            + "\t\t\t},\n"
            + "\t\t\t\"index_partition_meta\":{\n"
            + "\t\t\t\t\"length\":28\n"
            + "\t\t\t},\n"
            + "\t\t\t\"schema.json\":{\n"
            + "\t\t\t\t\"length\":"
            + schemaContent.length()
            + "\n"
            + "\t\t\t},\n"
            + "\t\t\t\"segment_0_level_0\":{\n"
            + "\t\t\t\t\"length\":-2\n"
            + "\t\t\t},\n"
            + "\t\t\t\"segment_0_level_0\\/attribute\":{\n"
            + "\t\t\t\t\"length\":-2\n"
            + "\t\t\t},\n"
            + "\t\t\t\"segment_0_level_0\\/attribute\\/_id\":{\n"
            + "\t\t\t\t\"length\":-2\n"
            + "\t\t\t},\n"
            + "\t\t\t\"segment_0_level_0\\/attribute\\/_id\\/data\":{\n"
            + "\t\t\t\t\"length\":1050\n"
            + "\t\t\t},\n"
            + "\t\t\t\"segment_0_level_0\\/counter\":{\n"
            + "\t\t\t\t\"length\":0\n"
            + "\t\t\t},\n"
            + "\t\t\t\"segment_0_level_0\\/deletionmap\":{\n"
            + "\t\t\t\t\"length\":-2\n"
            + "\t\t\t},\n"
            + "\t\t\t\"segment_0_level_0\\/segment_info\":{\n"
            + "\t\t\t\t\"length\":347\n"
            + "\t\t\t},\n"
            + "\t\t\t\"segment_0_level_0\\/segment_metrics\":{\n"
            + "\t\t\t\t\"length\":52\n"
            + "\t\t\t},\n"
            + "\t\t\t\"segment_0_level_0\\/summary\":{\n"
            + "\t\t\t\t\"length\":-2\n"
            + "\t\t\t},\n"
            + "\t\t\t\"segment_0_level_0\\/summary\\/data\":{\n"
            + "\t\t\t\t\"length\":1400\n"
            + "\t\t\t},\n"
            + "\t\t\t\"segment_0_level_0\\/summary\\/offset\":{\n"
            + "\t\t\t\t\"length\":400\n"
            + "\t\t\t},\n"
            + "\t\t\t\"truncate_meta\":{\n"
            + "\t\t\t\t\"length\":-2\n"
            + "\t\t\t},\n"
            + "\t\t\t\"truncate_meta\\/index.mapper\":{\n"
            + "\t\t\t\t\"length\":41\n"
            + "\t\t\t},\n"
            + "\t\t\t\"version.1\":{\n"
            + "\t\t\t\t\"length\":1371\n"
            + "\t\t\t}\n"
            + "\t\t}\n"
            + "\t},\n"
            + "\t\"package_files\":{}\n"
            + "}";
        try {
            Files.write(shardPath.resolve(entryTableFile), entryTableContent.getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        HavenaskStore.rewriteEntryFile(indexName, shardPath);

        try {
            String newEntryTable = Files.readString(shardPath.resolve(entryTableFile));
            assertEquals(newEntryTable, entryTableContent);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void testRewriteEntryFile() throws IOException {
        String indexName = "test";
        Path shardPath = dataPath;
        long commitVersion = 0;
        String versionFile = "version." + commitVersion;
        String content = "{\n"
            + "  \"description\":\n"
            + "  {\n"
            + "  },\n"
            + "  \"fence_name\": \"__FENCE__2EyV9JborPyRrMe3G2kmwtd3IA\",\n"
            + "  \"format_version\":\n"
            + "  2,\n"
            + "  \"last_segmentid\":\n"
            + "  -1,\n"
            + "  \"level_info\":\n"
            + "  {\n"
            + "    \"level_metas\":\n"
            + "    [\n"
            + "      {\n"
            + "        \"cursor\":\n"
            + "        0,\n"
            + "        \"level_idx\":\n"
            + "        0,\n"
            + "        \"segments\":\n"
            + "        [\n"
            + "        ],\n"
            + "        \"topology\":\n"
            + "        \"sequence\"\n"
            + "      }\n"
            + "    ]\n"
            + "  },\n"
            + "  \"locator\":\n"
            + "  \"\",\n"
            + "  \"schema_version\":\n"
            + "  0,\n"
            + "  \"segments\":\n"
            + "  [\n"
            + "  ],\n"
            + "  \"timestamp\":\n"
            + "  -1,\n"
            + "  \"versionid\":\n"
            + "  0\n"
            + "}";
        try {
            Files.write(shardPath.resolve(versionFile), content.getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // write schema.json
        String schemaFile = "schema.json";
        String schemaContent = "{\n"
            + "  \"description\":\n"
            + "  {\n"
            + "  },\n"
            + "  \"fields\":\n"
            + "  [\n"
            + "    {\n"
            + "      \"name\": \"_id\",\n"
            + "      \"type\": \"keyword\"\n"
            + "    }\n"
            + "  ]\n"
            + "}";
        try {
            Files.write(shardPath.resolve(schemaFile), schemaContent.getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        String fenceName = "__FENCE__2EyV9JborPyRrMe3G2kmwtd3IA";
        String entryTableFile = "entry_table." + commitVersion;
        String entryTableContent = "{\n"
            + "  \"files\":\n"
            + "  {\n"
            + "    \"\":\n"
            + "    {\n"
            + "      \"deploy_meta.0\":\n"
            + "      {\n"
            + "        \"length\":\n"
            + "        711\n"
            + "      },\n"
            + "      \"index_format_version\":\n"
            + "      {\n"
            + "        \"length\":\n"
            + "        82\n"
            + "      },\n"
            + "      \"index_partition_meta\":\n"
            + "      {\n"
            + "        \"length\":\n"
            + "        28\n"
            + "      },\n"
            + "      \"schema.json\":\n"
            + "      {\n"
            + "        \"length\":\n"
            + "        2335\n"
            + "      },\n"
            + "      \"version.0\":\n"
            + "      {\n"
            + "        \"length\":\n"
            + "        372\n"
            + "      }\n"
            + "    },\n"
            + "\"\\/usr\\/share\\/havenask\\/data\\/havenask\\/local_search_12000\\/general_p0_r0\\/runtimedata\\"
            + "/image_index1\\/generation_0\\/partition_0_13107\":\n"
            + "    {\n"
            + "      \"segment_1_level_0/attribute/_id\":\n"
            + "      {\n"
            + "        \"length\":\n"
            + "        -2\n"
            + "      },\n"
            + "      \"segment_1_level_0/attribute/_id/data\":\n"
            + "      {\n"
            + "        \"length\":\n"
            + "        210\n"
            + "      },\n"
            + "      \"segment_1_level_0/attribute/_id/data_info\":\n"
            + "      {\n"
            + "        \"length\":\n"
            + "        72\n"
            + "      },\n"
            + "      \"segment_1_level_0/attribute/_id/offset\":\n"
            + "      {\n"
            + "        \"length\":\n"
            + "        44\n"
            + "      }\n"
            + "    }\n"
            + "  },\n"
            + "  \"package_files\":\n"
            + "  {\n"
            + "  }\n"
            + "}";
        try {
            Files.createDirectories(shardPath.resolve(fenceName));
            Files.write(shardPath.resolve(fenceName).resolve(entryTableFile), entryTableContent.getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        HavenaskStore.rewriteEntryFile(indexName, shardPath);

        String newEntryTableContent = "{\n"
            + "\t\"files\":{\n"
            + "\t\t\"\":{\n"
            + "\t\t\t\"deploy_meta.0\":{\n"
            + "\t\t\t\t\"length\":711\n"
            + "\t\t\t},\n"
            + "\t\t\t\"index_format_version\":{\n"
            + "\t\t\t\t\"length\":82\n"
            + "\t\t\t},\n"
            + "\t\t\t\"index_partition_meta\":{\n"
            + "\t\t\t\t\"length\":28\n"
            + "\t\t\t},\n"
            + "\t\t\t\"schema.json\":{\n"
            + "\t\t\t\t\"length\":"
            + schemaContent.length()
            + "\n"
            + "\t\t\t},\n"
            + "\t\t\t\"version.0\":{\n"
            + "\t\t\t\t\"length\":372\n"
            + "\t\t\t}\n"
            + "\t\t},\n"
            + "\t\t\"\\/usr\\/share\\/havenask\\/data\\/havenask\\/local_search_12000\\/general_p0_r0\\"
            + "/runtimedata\\/test\\/generation_0\\/partition_0_13107\":{\n"
            + "\t\t\t\"segment_1_level_0\\/attribute\\/_id\":{\n"
            + "\t\t\t\t\"length\":-2\n"
            + "\t\t\t},\n"
            + "\t\t\t\"segment_1_level_0\\/attribute\\/_id\\/data\":{\n"
            + "\t\t\t\t\"length\":210\n"
            + "\t\t\t},\n"
            + "\t\t\t\"segment_1_level_0\\/attribute\\/_id\\/data_info\":{\n"
            + "\t\t\t\t\"length\":72\n"
            + "\t\t\t},\n"
            + "\t\t\t\"segment_1_level_0\\/attribute\\/_id\\/offset\":{\n"
            + "\t\t\t\t\"length\":44\n"
            + "\t\t\t}\n"
            + "\t\t}\n"
            + "\t},\n"
            + "\t\"package_files\":{}\n"
            + "}";
        try {
            String newEntryTable = Files.readString(shardPath.resolve(fenceName).resolve(entryTableFile));
            assertEquals(newEntryTable, newEntryTableContent);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

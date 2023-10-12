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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Map;

import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.SegmentInfos;
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
import org.havenask.index.store.Store.OnClose;
import org.havenask.index.store.StoreFileMetadata;
import org.havenask.test.DummyShardLock;
import org.havenask.test.HavenaskTestCase;
import org.junit.Before;

import static org.hamcrest.CoreMatchers.containsString;

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

        havenaskStore = new HavenaskStore(shardId, indexSettings, null, new DummyShardLock(shardId), OnClose.EMPTY, dataPath);
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
            + "    }\n"
            + "  },\n"
            + "\"package_files\":\n"
            + "  {\n"
            + "  }\n"
            + "}";
        Files.write(dataPath.resolve("entry_table.0"), entryTableContent.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE);

        {
            Map<String, StoreFileMetadata> snapshot = havenaskStore.getHavenaskMetadata(indexCommit);
            assertEquals(snapshot.size(), 6);
            assertEquals(snapshot.get("deploy_meta.0").length(), 711);
            assertEquals(snapshot.get("index_format_version").length(), 82);
            assertEquals(snapshot.get("index_partition_meta").length(), 28);
            assertEquals(snapshot.get("schema.json").length(), 2335);
            assertEquals(snapshot.get("version.0").length(), 372);
            assertEquals(snapshot.get("entry_table.0").length(), 400);
        }

        // commit is null
        {
            Map<String, StoreFileMetadata> snapshot = havenaskStore.getHavenaskMetadata(null);
            assertEquals(snapshot.size(), 6);
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
    }

    public void testRenameTempFilesSafe() throws IOException {
        String prefix = "recovery." + UUIDs.randomBase64UUID() + ".";
        Map<String, String> tempFileMap = Map.of(
            prefix + "test",
            "test",
            prefix + "dir1/test",
            "dir1/test",
            prefix + "dir1/dir2/test",
            "dir1/dir2/test"
        );

        Files.createDirectories(dataPath.resolve(prefix + "dir1/dir2"));
        Files.write(dataPath.resolve(prefix + "test"), "test content".getBytes(StandardCharsets.UTF_8));
        Files.write(dataPath.resolve(prefix + "dir1/test"), "test dir1 content".getBytes(StandardCharsets.UTF_8));
        Files.write(dataPath.resolve(prefix + "dir1/dir2/test"), "test dir1 dir2 content".getBytes(StandardCharsets.UTF_8));
        havenaskStore.renameHavenaskTempFilesSafe(tempFileMap);

        assertEquals(Files.readString(dataPath.resolve("test")), "test content");
        assertEquals(Files.readString(dataPath.resolve("dir1/test")), "test dir1 content");
        assertEquals(Files.readString(dataPath.resolve("dir1/dir2/test")), "test dir1 dir2 content");
    }
}

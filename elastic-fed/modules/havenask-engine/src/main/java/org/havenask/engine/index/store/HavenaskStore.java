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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.Version;
import org.havenask.common.Strings;
import org.havenask.engine.HavenaskEngineEnvironment;
import org.havenask.engine.index.config.EntryTable;
import org.havenask.engine.index.engine.EngineSettings;
import org.havenask.engine.index.engine.HavenaskEngine.HavenaskCommitInfo;
import org.havenask.env.ShardLock;
import org.havenask.index.IndexSettings;
import org.havenask.index.shard.ShardId;
import org.havenask.index.store.Store;
import org.havenask.index.store.StoreFileMetadata;

import static org.havenask.engine.util.Utils.INDEX_SUB_PATH;

public class HavenaskStore extends Store {

    public static final Version havenaskVersion = Version.fromBits(1, 0, 0);
    private static final String HAVENASK_VERSION_FILE_PREFIX = "version.";
    private static final String HAVENASK_ENTRY_TABLE_FILE_PREFIX = "entry_table.";

    private final HavenaskEngineEnvironment env;
    private final Path shardPath;

    public HavenaskStore(
        ShardId shardId,
        IndexSettings indexSettings,
        Directory directory,
        ShardLock shardLock,
        OnClose onClose,
        HavenaskEngineEnvironment env
    ) {
        super(shardId, indexSettings, directory, shardLock, onClose);
        this.env = env;
        this.shardPath = env.getShardPath(shardId).resolve(INDEX_SUB_PATH);
    }

    @Override
    public MetadataSnapshot getMetadata(IndexCommit commit) throws IOException {
        MetadataSnapshot luceneSnapshot = super.getMetadata(commit, false);
        Map<String, StoreFileMetadata> metadata = new HashMap<>(luceneSnapshot.asMap());
        if (EngineSettings.isHavenaskEngine(indexSettings.getSettings())) {
            metadata.putAll(getHavenaskMetadata(commit));
        }
        return new MetadataSnapshot(metadata, luceneSnapshot.getCommitUserData(), luceneSnapshot.getNumDocs());
    }

    Map<String, StoreFileMetadata> getHavenaskMetadata(IndexCommit commit) throws IOException {
        if (commit == null) {
            return new LinkedHashMap<>();
        }

        long commitVersion = commit.getUserData().containsKey(HavenaskCommitInfo.COMMIT_VERSION_KEY)
            ? Long.valueOf(commit.getUserData().get(HavenaskCommitInfo.COMMIT_VERSION_KEY))
            : 0;
        String versionFile = HAVENASK_VERSION_FILE_PREFIX + commitVersion;
        String content = Files.readString(shardPath.resolve(versionFile));
        JSONObject jsonObject = JSON.parseObject(content);
        String fenceName = jsonObject.getString("fence_name");

        String entryTableFile = HAVENASK_ENTRY_TABLE_FILE_PREFIX + commitVersion;
        Path entryTablePath = shardPath.resolve(entryTableFile);
        if (false == Strings.isEmpty(fenceName)) {
            entryTablePath = shardPath.resolve(fenceName).resolve(entryTableFile);
        }
        String entryTableContent = Files.readString(entryTablePath);
        EntryTable entryTable = EntryTable.parse(entryTableContent);

        // TODO 如何处理目录
        Map<String, StoreFileMetadata> metadata = new LinkedHashMap<>();
        entryTable.files.forEach((name, file) -> {
            if (file.type == EntryTable.Type.FILE) {
                StoreFileMetadata storeFileMetadata = new StoreFileMetadata(file.name, file.length, "", havenaskVersion);
                metadata.put(file.name, storeFileMetadata);
            }
        });

        return metadata;
    }

    public IndexOutput createVerifyingOutput(String fileName, final StoreFileMetadata metadata,
        final IOContext context) throws IOException {
        if (isHavenaskFile(metadata.writtenBy())) {
            // TODO: add havenask file
            return null;
        } else {
            return super.createVerifyingOutput(fileName, metadata, context);
        }
    }

    public static boolean isHavenaskFile(Version version) {
        return version.major == havenaskVersion.major;
    }
}

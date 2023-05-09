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

package org.havenask.index.snapshots.blobstore;

import org.havenask.common.Nullable;
import org.havenask.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Contains a list of files participating in a snapshot
 */
public class SnapshotFiles {

    private final String snapshot;

    private final List<FileInfo> indexFiles;

    @Nullable
    private final String shardStateIdentifier;

    private Map<String, FileInfo> physicalFiles = null;

    /**
     * Returns snapshot name
     *
     * @return snapshot name
     */
    public String snapshot() {
        return snapshot;
    }

    /**
     * @param snapshot             snapshot name
     * @param indexFiles           index files
     * @param shardStateIdentifier unique identifier for the state of the shard that this snapshot was taken from
     */
    public SnapshotFiles(String snapshot, List<FileInfo> indexFiles, @Nullable String shardStateIdentifier) {
        this.snapshot = snapshot;
        this.indexFiles = indexFiles;
        this.shardStateIdentifier = shardStateIdentifier;
    }

    /**
     * Creates a new instance with the given snapshot name but otherwise identical to the current instance.
     */
    public SnapshotFiles withSnapshotName(String snapshotName) {
        return new SnapshotFiles(snapshotName, indexFiles, shardStateIdentifier);
    }

    /**
     * Checks if the given other instance contains the same files as well as the same {@link #shardStateIdentifier}.
     */
    public boolean isSame(SnapshotFiles other) {
        if (Objects.equals(shardStateIdentifier, other.shardStateIdentifier) == false) {
            return false;
        }
        final int fileCount = indexFiles.size();
        if (other.indexFiles.size() != fileCount) {
            return false;
        }
        for (int i = 0; i < fileCount; i++) {
            if (indexFiles.get(i).isSame(other.indexFiles.get(i)) == false) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns an identifier for the shard state that can be used to check whether a shard has changed between
     * snapshots or not.
     */
    @Nullable
    public String shardStateIdentifier() {
        return shardStateIdentifier;
    }

    /**
     * Returns a list of file in the snapshot
     */
    public List<FileInfo> indexFiles() {
        return indexFiles;
    }

    /**
     * Returns true if this snapshot contains a file with a given original name
     *
     * @param physicalName original file name
     * @return true if the file was found, false otherwise
     */
    public boolean containPhysicalIndexFile(String physicalName) {
        return findPhysicalIndexFile(physicalName) != null;
    }

    /**
     * Returns information about a physical file with the given name
     * @param physicalName the original file name
     * @return information about this file
     */
    public FileInfo findPhysicalIndexFile(String physicalName) {
        if (physicalFiles == null) {
            Map<String, FileInfo> files = new HashMap<>();
            for(FileInfo fileInfo : indexFiles) {
                files.put(fileInfo.physicalName(), fileInfo);
            }
            this.physicalFiles = files;
        }
        return physicalFiles.get(physicalName);
    }

}

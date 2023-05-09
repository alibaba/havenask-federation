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

package org.havenask.indices.recovery;

import org.havenask.LegacyESVersion;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.io.stream.StreamOutput;
import org.havenask.index.seqno.SequenceNumbers;
import org.havenask.index.shard.ShardId;
import org.havenask.index.store.Store;

import java.io.IOException;

public class RecoveryCleanFilesRequest extends RecoveryTransportRequest {

    private final long recoveryId;
    private final ShardId shardId;
    private final Store.MetadataSnapshot snapshotFiles;
    private final int totalTranslogOps;
    private final long globalCheckpoint;

    public RecoveryCleanFilesRequest(long recoveryId, long requestSeqNo, ShardId shardId, Store.MetadataSnapshot snapshotFiles,
                              int totalTranslogOps, long globalCheckpoint) {
        super(requestSeqNo);
        this.recoveryId = recoveryId;
        this.shardId = shardId;
        this.snapshotFiles = snapshotFiles;
        this.totalTranslogOps = totalTranslogOps;
        this.globalCheckpoint = globalCheckpoint;
    }

    RecoveryCleanFilesRequest(StreamInput in) throws IOException {
        super(in);
        recoveryId = in.readLong();
        shardId = new ShardId(in);
        snapshotFiles = new Store.MetadataSnapshot(in);
        totalTranslogOps = in.readVInt();
        if (in.getVersion().onOrAfter(LegacyESVersion.V_7_2_0)) {
            globalCheckpoint = in.readZLong();
        } else {
            globalCheckpoint = SequenceNumbers.UNASSIGNED_SEQ_NO;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeLong(recoveryId);
        shardId.writeTo(out);
        snapshotFiles.writeTo(out);
        out.writeVInt(totalTranslogOps);
        if (out.getVersion().onOrAfter(LegacyESVersion.V_7_2_0)) {
            out.writeZLong(globalCheckpoint);
        }
    }

    public Store.MetadataSnapshot sourceMetaSnapshot() {
        return snapshotFiles;
    }

    public long recoveryId() {
        return this.recoveryId;
    }

    public ShardId shardId() {
        return shardId;
    }

    public int totalTranslogOps() {
        return totalTranslogOps;
    }

    public long getGlobalCheckpoint() {
        return globalCheckpoint;
    }
}

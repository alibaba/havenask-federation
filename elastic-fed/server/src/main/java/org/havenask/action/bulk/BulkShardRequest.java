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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.havenask.action.bulk;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.havenask.LegacyESVersion;
import org.havenask.Version;
import org.havenask.action.support.replication.ReplicatedWriteRequest;
import org.havenask.action.support.replication.ReplicationRequest;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.io.stream.StreamOutput;
import org.havenask.index.shard.ShardId;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

public class BulkShardRequest extends ReplicatedWriteRequest<BulkShardRequest> implements Accountable {

    public static final Version COMPACT_SHARD_ID_VERSION = LegacyESVersion.V_7_9_0;
    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(BulkShardRequest.class);

    private final BulkItemRequest[] items;

    public BulkShardRequest(StreamInput in) throws IOException {
        super(in);
        final ShardId itemShardId = in.getVersion().onOrAfter(COMPACT_SHARD_ID_VERSION) ? shardId : null;
        items = in.readArray(i -> i.readOptionalWriteable(inpt -> new BulkItemRequest(itemShardId, inpt)), BulkItemRequest[]::new);
    }

    public BulkShardRequest(ShardId shardId, RefreshPolicy refreshPolicy, BulkItemRequest[] items) {
        super(shardId);
        this.items = items;
        setRefreshPolicy(refreshPolicy);
    }

    public BulkItemRequest[] items() {
        return items;
    }

    @Override
    public String[] indices() {
        // A bulk shard request encapsulates items targeted at a specific shard of an index.
        // However, items could be targeting aliases of the index, so the bulk request although
        // targeting a single concrete index shard might do so using several alias names.
        // These alias names have to be exposed by this method because authorization works with
        // aliases too, specifically, the item's target alias can be authorized but the concrete
        // index might not be.
        Set<String> indices = new HashSet<>(1);
        for (BulkItemRequest item : items) {
            if (item != null) {
                indices.add(item.index());
            }
        }
        return indices.toArray(new String[0]);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeArray(out.getVersion().onOrAfter(COMPACT_SHARD_ID_VERSION) ? (o, item) -> {
            if (item != null) {
                o.writeBoolean(true);
                item.writeThin(o);
            } else {
                o.writeBoolean(false);
            }
        } : StreamOutput::writeOptionalWriteable, items);
    }

    @Override
    public String toString() {
        // This is included in error messages so we'll try to make it somewhat user friendly.
        StringBuilder b = new StringBuilder("BulkShardRequest [");
        b.append(shardId).append("] containing [");
        if (items.length > 1) {
          b.append(items.length).append("] requests");
        } else {
            b.append(items[0].request()).append("]");
        }

        switch (getRefreshPolicy()) {
        case IMMEDIATE:
            b.append(" and a refresh");
            break;
        case WAIT_UNTIL:
            b.append(" blocking until refresh");
            break;
        case NONE:
            break;
        }
        return b.toString();
    }

    @Override
    public String getDescription() {
        final StringBuilder stringBuilder = new StringBuilder().append("requests[").append(items.length).append("], index").append(shardId);
        final RefreshPolicy refreshPolicy = getRefreshPolicy();
        if (refreshPolicy == RefreshPolicy.IMMEDIATE || refreshPolicy == RefreshPolicy.WAIT_UNTIL) {
            stringBuilder.append(", refresh[").append(refreshPolicy).append(']');
        }
        return stringBuilder.toString();
    }

    @Override
    protected BulkShardRequest routedBasedOnClusterVersion(long routedBasedOnClusterVersion) {
        return super.routedBasedOnClusterVersion(routedBasedOnClusterVersion);
    }

    @Override
    public void onRetry() {
        for (BulkItemRequest item : items) {
            if (item.request() instanceof ReplicationRequest) {
                // all replication requests need to be notified here as well to ie. make sure that internal optimizations are
                // disabled see IndexRequest#canHaveDuplicates()
                ((ReplicationRequest<?>) item.request()).onRetry();
            }
        }
    }

    @Override
    public long ramBytesUsed() {
        return SHALLOW_SIZE + Stream.of(items).mapToLong(Accountable::ramBytesUsed).sum();
    }
}

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

package org.havenask.action.search;

import org.havenask.Version;
import org.havenask.common.Strings;
import org.havenask.common.bytes.BytesReference;
import org.havenask.common.io.stream.ByteBufferStreamInput;
import org.havenask.common.io.stream.BytesStreamOutput;
import org.havenask.common.io.stream.NamedWriteableAwareStreamInput;
import org.havenask.common.io.stream.NamedWriteableRegistry;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.io.stream.StreamOutput;
import org.havenask.index.shard.ShardId;
import org.havenask.search.SearchPhaseResult;
import org.havenask.search.SearchShardTarget;
import org.havenask.search.internal.AliasFilter;
import org.havenask.transport.RemoteClusterAware;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SearchContextId {
    private final Map<ShardId, SearchContextIdForNode> shards;
    private final Map<String, AliasFilter> aliasFilter;

    private SearchContextId(Map<ShardId, SearchContextIdForNode> shards, Map<String, AliasFilter> aliasFilter) {
        this.shards = shards;
        this.aliasFilter = aliasFilter;
    }

    public Map<ShardId, SearchContextIdForNode> shards() {
        return shards;
    }

    public Map<String, AliasFilter> aliasFilter() {
        return aliasFilter;
    }

    public static String encode(List<SearchPhaseResult> searchPhaseResults, Map<String, AliasFilter> aliasFilter, Version version) {
        final Map<ShardId, SearchContextIdForNode> shards = new HashMap<>();
        for (SearchPhaseResult searchPhaseResult : searchPhaseResults) {
            final SearchShardTarget target = searchPhaseResult.getSearchShardTarget();
            shards.put(target.getShardId(),
                new SearchContextIdForNode(target.getClusterAlias(), target.getNodeId(), searchPhaseResult.getContextId()));
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setVersion(version);
            Version.writeVersion(version, out);
            out.writeMap(shards, (o, k) -> k.writeTo(o), (o, v) -> v.writeTo(o));
            out.writeMap(aliasFilter, StreamOutput::writeString, (o, v) -> v.writeTo(o));
            return Base64.getUrlEncoder().encodeToString(BytesReference.toBytes(out.bytes()));
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public static SearchContextId decode(NamedWriteableRegistry namedWriteableRegistry, String id) {
        final ByteBuffer byteBuffer;
        try {
            byteBuffer = ByteBuffer.wrap(Base64.getUrlDecoder().decode(id));
        } catch (Exception e) {
            throw new IllegalArgumentException("invalid id: [" + id + "]", e);
        }
        try (StreamInput in = new NamedWriteableAwareStreamInput(new ByteBufferStreamInput(byteBuffer), namedWriteableRegistry)) {
            final Version version = Version.readVersion(in);
            in.setVersion(version);
            final Map<ShardId, SearchContextIdForNode> shards = in.readMap(ShardId::new, SearchContextIdForNode::new);
            final Map<String, AliasFilter> aliasFilters = in.readMap(StreamInput::readString, AliasFilter::new);
            if (in.available() > 0) {
                throw new IllegalArgumentException("Not all bytes were read");
            }
            return new SearchContextId(Collections.unmodifiableMap(shards), Collections.unmodifiableMap(aliasFilters));
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public String[] getActualIndices() {
        final Set<String> indices = new HashSet<>();
        for (Map.Entry<ShardId, SearchContextIdForNode> entry : shards().entrySet()) {
            final String indexName = entry.getKey().getIndexName();
            final String clusterAlias = entry.getValue().getClusterAlias();
            if (Strings.isEmpty(clusterAlias)) {
                indices.add(indexName);
            } else {
                indices.add(clusterAlias + RemoteClusterAware.REMOTE_CLUSTER_INDEX_SEPARATOR + indexName);
            }
        }
        return indices.toArray(new String[0]);
    }
}

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

import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.RAMOutputStream;
import org.havenask.LegacyESVersion;
import org.havenask.Version;
import org.havenask.common.util.concurrent.AtomicArray;
import org.havenask.search.SearchPhaseResult;
import org.havenask.search.SearchShardTarget;
import org.havenask.search.internal.InternalScrollSearchRequest;
import org.havenask.search.internal.ShardSearchContextId;
import org.havenask.transport.RemoteClusterAware;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Base64;

final class TransportSearchHelper {

    private static final String INCLUDE_CONTEXT_UUID = "include_context_uuid";

    static InternalScrollSearchRequest internalScrollSearchRequest(ShardSearchContextId id, SearchScrollRequest request) {
        return new InternalScrollSearchRequest(request, id);
    }

    static String buildScrollId(AtomicArray<? extends SearchPhaseResult> searchPhaseResults, Version version) {
        boolean includeContextUUID = version.onOrAfter(LegacyESVersion.V_7_7_0);
        try (RAMOutputStream out = new RAMOutputStream()) {
            if (includeContextUUID) {
                out.writeString(INCLUDE_CONTEXT_UUID);
            }
            out.writeString(searchPhaseResults.length() == 1 ? ParsedScrollId.QUERY_AND_FETCH_TYPE : ParsedScrollId.QUERY_THEN_FETCH_TYPE);
            out.writeVInt(searchPhaseResults.asList().size());
            for (SearchPhaseResult searchPhaseResult : searchPhaseResults.asList()) {
                if (includeContextUUID) {
                    out.writeString(searchPhaseResult.getContextId().getSessionId());
                }
                out.writeLong(searchPhaseResult.getContextId().getId());
                SearchShardTarget searchShardTarget = searchPhaseResult.getSearchShardTarget();
                if (searchShardTarget.getClusterAlias() != null) {
                    out.writeString(
                        RemoteClusterAware.buildRemoteIndexName(searchShardTarget.getClusterAlias(), searchShardTarget.getNodeId()));
                } else {
                    out.writeString(searchShardTarget.getNodeId());
                }
            }
            byte[] bytes = new byte[(int) out.getFilePointer()];
            out.writeTo(bytes, 0);
            return Base64.getUrlEncoder().encodeToString(bytes);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static ParsedScrollId parseScrollId(String scrollId) {
        try {
            byte[] bytes = Base64.getUrlDecoder().decode(scrollId);
            ByteArrayDataInput in = new ByteArrayDataInput(bytes);
            final boolean includeContextUUID;
            final String type;
            final String firstChunk = in.readString();
            if (INCLUDE_CONTEXT_UUID.equals(firstChunk)) {
                includeContextUUID = true;
                type = in.readString();
            } else {
                includeContextUUID = false;
                type = firstChunk;
            }
            SearchContextIdForNode[] context = new SearchContextIdForNode[in.readVInt()];
            for (int i = 0; i < context.length; ++i) {
                final String contextUUID = includeContextUUID ? in.readString() : "";
                long id = in.readLong();
                String target = in.readString();
                String clusterAlias;
                final int index = target.indexOf(RemoteClusterAware.REMOTE_CLUSTER_INDEX_SEPARATOR);
                if (index == -1) {
                    clusterAlias = null;
                } else {
                    clusterAlias = target.substring(0, index);
                    target = target.substring(index+1);
                }
                context[i] = new SearchContextIdForNode(clusterAlias, target, new ShardSearchContextId(contextUUID, id));
            }
            if (in.getPosition() != bytes.length) {
                throw new IllegalArgumentException("Not all bytes were read");
            }
            return new ParsedScrollId(scrollId, type, context);
        } catch (Exception e) {
            throw new IllegalArgumentException("Cannot parse scroll id", e);
        }
    }

    private TransportSearchHelper() {

    }
}

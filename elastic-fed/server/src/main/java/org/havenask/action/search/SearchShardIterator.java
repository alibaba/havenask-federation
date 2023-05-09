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

import org.havenask.action.OriginalIndices;
import org.havenask.cluster.routing.PlainShardIterator;
import org.havenask.cluster.routing.ShardRouting;
import org.havenask.common.Nullable;
import org.havenask.common.unit.TimeValue;
import org.havenask.common.util.Countable;
import org.havenask.common.util.PlainIterator;
import org.havenask.index.shard.ShardId;
import org.havenask.search.SearchShardTarget;
import org.havenask.search.internal.ShardSearchContextId;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Extension of {@link PlainShardIterator} used in the search api, which also holds the {@link OriginalIndices}
 * of the search request (useful especially with cross-cluster search, as each cluster has its own set of original indices) as well as
 * the cluster alias.
 * @see OriginalIndices
 */
public final class SearchShardIterator implements Comparable<SearchShardIterator>, Countable {

    private final OriginalIndices originalIndices;
    private final String clusterAlias;
    private final ShardId shardId;
    private boolean skip = false;

    private final ShardSearchContextId searchContextId;
    private final TimeValue searchContextKeepAlive;
    private final PlainIterator<String> targetNodesIterator;

    /**
     * Creates a {@link PlainShardIterator} instance that iterates over a subset of the given shards
     * this the a given <code>shardId</code>.
     *
     * @param clusterAlias    the alias of the cluster where the shard is located
     * @param shardId         shard id of the group
     * @param shards          shards to iterate
     * @param originalIndices the indices that the search request originally related to (before any rewriting happened)
     */
    public SearchShardIterator(@Nullable String clusterAlias, ShardId shardId, List<ShardRouting> shards, OriginalIndices originalIndices) {
        this(clusterAlias, shardId, shards.stream().map(ShardRouting::currentNodeId).collect(Collectors.toList()),
            originalIndices, null, null);
    }

    public SearchShardIterator(@Nullable String clusterAlias, ShardId shardId,
                               List<String> targetNodeIds, OriginalIndices originalIndices,
                               ShardSearchContextId searchContextId, TimeValue searchContextKeepAlive) {
        this.shardId = shardId;
        this.targetNodesIterator = new PlainIterator<>(targetNodeIds);
        this.originalIndices = originalIndices;
        this.clusterAlias = clusterAlias;
        this.searchContextId = searchContextId;
        this.searchContextKeepAlive = searchContextKeepAlive;
        assert searchContextKeepAlive == null || searchContextId != null;
    }

    /**
     * Returns the original indices associated with this shard iterator, specifically with the cluster that this shard belongs to.
     */
    public OriginalIndices getOriginalIndices() {
        return originalIndices;
    }

    /**
     * Returns the alias of the cluster where the shard is located.
     */
    @Nullable
    public String getClusterAlias() {
        return clusterAlias;
    }

    SearchShardTarget nextOrNull() {
        final String nodeId = targetNodesIterator.nextOrNull();
        if (nodeId != null) {
            return new SearchShardTarget(nodeId, shardId, clusterAlias, originalIndices);
        }
        return null;
    }

    int remaining() {
        return targetNodesIterator.remaining();
    }

    /**
     * Returns a non-null value if this request should use a specific search context instead of the latest one.
     */
    ShardSearchContextId getSearchContextId() {
        return searchContextId;
    }

    TimeValue getSearchContextKeepAlive() {
        return searchContextKeepAlive;
    }

    List<String> getTargetNodeIds() {
        return targetNodesIterator.asList();
    }

    /**
     * Reset the iterator and mark it as skippable
     * @see #skip()
     */
    void resetAndSkip() {
        reset();
        skip = true;
    }

    void reset() {
        targetNodesIterator.reset();
    }

    /**
     * Returns <code>true</code> if the search execution should skip this shard since it can not match any documents given the query.
     */
    boolean skip() {
        return skip;
    }


    @Override
    public int size() {
        return targetNodesIterator.size();
    }

    ShardId shardId() {
        return shardId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SearchShardIterator that = (SearchShardIterator) o;
        return shardId.equals(that.shardId) && Objects.equals(clusterAlias, that.clusterAlias);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clusterAlias, shardId);
    }

    @Override
    public int compareTo(SearchShardIterator o) {
        return Comparator.comparing(SearchShardIterator::shardId)
            .thenComparing(SearchShardIterator::getClusterAlias, Comparator.nullsFirst(String::compareTo))
            .compare(this, o);
    }
}

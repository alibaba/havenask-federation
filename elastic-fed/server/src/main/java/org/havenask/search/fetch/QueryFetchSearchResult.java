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

package org.havenask.search.fetch;

import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.io.stream.StreamOutput;
import org.havenask.search.SearchPhaseResult;
import org.havenask.search.SearchShardTarget;
import org.havenask.search.internal.ShardSearchContextId;
import org.havenask.search.query.QuerySearchResult;

import java.io.IOException;

public final class QueryFetchSearchResult extends SearchPhaseResult {

    private final QuerySearchResult queryResult;
    private final FetchSearchResult fetchResult;

    public QueryFetchSearchResult(StreamInput in) throws IOException {
        super(in);
        queryResult = new QuerySearchResult(in);
        fetchResult = new FetchSearchResult(in);
    }

    public QueryFetchSearchResult(QuerySearchResult queryResult, FetchSearchResult fetchResult) {
        this.queryResult = queryResult;
        this.fetchResult = fetchResult;
    }

    @Override
    public ShardSearchContextId getContextId() {
        return queryResult.getContextId();
    }

    @Override
    public SearchShardTarget getSearchShardTarget() {
        return queryResult.getSearchShardTarget();
    }

    @Override
    public void setSearchShardTarget(SearchShardTarget shardTarget) {
        super.setSearchShardTarget(shardTarget);
        queryResult.setSearchShardTarget(shardTarget);
        fetchResult.setSearchShardTarget(shardTarget);
    }

    @Override
    public void setShardIndex(int requestIndex) {
        super.setShardIndex(requestIndex);
        queryResult.setShardIndex(requestIndex);
        fetchResult.setShardIndex(requestIndex);
    }

    @Override
    public QuerySearchResult queryResult() {
        return queryResult;
    }

    @Override
    public FetchSearchResult fetchResult() {
        return fetchResult;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        queryResult.writeTo(out);
        fetchResult.writeTo(out);
    }
}

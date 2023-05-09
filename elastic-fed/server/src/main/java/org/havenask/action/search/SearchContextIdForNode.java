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

import org.havenask.common.Nullable;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.io.stream.StreamOutput;
import org.havenask.common.io.stream.Writeable;
import org.havenask.search.internal.ShardSearchContextId;

import java.io.IOException;

public final class SearchContextIdForNode implements Writeable {
    private final String node;
    private final ShardSearchContextId searchContextId;
    private final String clusterAlias;

    SearchContextIdForNode(@Nullable String clusterAlias, String node, ShardSearchContextId searchContextId) {
        this.node = node;
        this.clusterAlias = clusterAlias;
        this.searchContextId = searchContextId;
    }

    SearchContextIdForNode(StreamInput in) throws IOException {
        this.node = in.readString();
        this.clusterAlias = in.readOptionalString();
        this.searchContextId = new ShardSearchContextId(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(node);
        out.writeOptionalString(clusterAlias);
        searchContextId.writeTo(out);
    }

    public String getNode() {
        return node;
    }

    @Nullable
    public String getClusterAlias() {
        return clusterAlias;
    }

    public ShardSearchContextId getSearchContextId() {
        return searchContextId;
    }

    @Override
    public String toString() {
        return "SearchContextIdForNode{" +
            "node='" + node + '\'' +
            ", seachContextId=" + searchContextId +
            ", clusterAlias='" + clusterAlias + '\'' +
            '}';
    }
}

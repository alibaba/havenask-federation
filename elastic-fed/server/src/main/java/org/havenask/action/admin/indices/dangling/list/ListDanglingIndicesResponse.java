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

package org.havenask.action.admin.indices.dangling.list;

import org.havenask.action.FailedNodeException;
import org.havenask.action.admin.indices.dangling.DanglingIndexInfo;
import org.havenask.action.support.nodes.BaseNodesResponse;
import org.havenask.cluster.ClusterName;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.io.stream.StreamOutput;
import org.havenask.common.xcontent.StatusToXContentObject;
import org.havenask.common.xcontent.XContent;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.rest.RestStatus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * Models a response to a {@link ListDanglingIndicesRequest}. A list request queries every node in the
 * cluster and aggregates their responses. When the aggregated response is converted to {@link XContent},
 * information for each dangling index is presented under the "dangling_indices" key. If any nodes
 * in the cluster failed to answer, the details are presented under the "_nodes.failures" key.
 */
public class ListDanglingIndicesResponse extends BaseNodesResponse<NodeListDanglingIndicesResponse> implements StatusToXContentObject {

    public ListDanglingIndicesResponse(StreamInput in) throws IOException {
        super(in);
    }

    public ListDanglingIndicesResponse(
        ClusterName clusterName,
        List<NodeListDanglingIndicesResponse> nodes,
        List<FailedNodeException> failures
    ) {
        super(clusterName, nodes, failures);
    }

    @Override
    public RestStatus status() {
        return this.hasFailures() ? RestStatus.INTERNAL_SERVER_ERROR : RestStatus.OK;
    }

    // Visible for testing
    static Collection<AggregatedDanglingIndexInfo> resultsByIndexUUID(List<NodeListDanglingIndicesResponse> nodes) {
        Map<String, AggregatedDanglingIndexInfo> byIndexUUID = new HashMap<>();

        for (NodeListDanglingIndicesResponse nodeResponse : nodes) {
            for (DanglingIndexInfo info : nodeResponse.getDanglingIndices()) {
                final String indexUUID = info.getIndexUUID();

                final AggregatedDanglingIndexInfo aggregatedInfo = byIndexUUID.computeIfAbsent(
                    indexUUID,
                    (_uuid) -> new AggregatedDanglingIndexInfo(indexUUID, info.getIndexName(), info.getCreationDateMillis())
                );

                aggregatedInfo.nodeIds.add(info.getNodeId());
            }
        }

        return byIndexUUID.values();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray("dangling_indices");

        for (AggregatedDanglingIndexInfo info : resultsByIndexUUID(this.getNodes())) {
            builder.startObject();

            builder.field("index_name", info.indexName);
            builder.field("index_uuid", info.indexUUID);
            builder.timeField("creation_date_millis", "creation_date", info.creationDateMillis);

            builder.array("node_ids", info.nodeIds.toArray(new String[0]));

            builder.endObject();
        }

        return builder.endArray();
    }

    @Override
    protected List<NodeListDanglingIndicesResponse> readNodesFrom(StreamInput in) throws IOException {
        return in.readList(NodeListDanglingIndicesResponse::new);
    }

    @Override
    protected void writeNodesTo(StreamOutput out, List<NodeListDanglingIndicesResponse> nodes) throws IOException {
        out.writeList(nodes);
    }

    // visible for testing
    static class AggregatedDanglingIndexInfo {
        private final String indexUUID;
        private final String indexName;
        private final long creationDateMillis;
        private final List<String> nodeIds;

        AggregatedDanglingIndexInfo(String indexUUID, String indexName, long creationDateMillis) {
            this.indexUUID = indexUUID;
            this.indexName = indexName;
            this.creationDateMillis = creationDateMillis;
            this.nodeIds = new ArrayList<>();
        }

        // the methods below are used in the unit tests

        public List<String> getNodeIds() {
            return nodeIds;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            AggregatedDanglingIndexInfo that = (AggregatedDanglingIndexInfo) o;
            return creationDateMillis == that.creationDateMillis
                && indexUUID.equals(that.indexUUID)
                && indexName.equals(that.indexName)
                && nodeIds.equals(that.nodeIds);
        }

        @Override
        public int hashCode() {
            return Objects.hash(indexUUID, indexName, creationDateMillis, nodeIds);
        }

        @Override
        public String toString() {
            return String.format(
                Locale.ROOT,
                "AggregatedDanglingIndexInfo{indexUUID='%s', indexName='%s', creationDateMillis=%d, nodeIds=%s}",
                indexUUID,
                indexName,
                creationDateMillis,
                nodeIds
            );
        }
    }
}

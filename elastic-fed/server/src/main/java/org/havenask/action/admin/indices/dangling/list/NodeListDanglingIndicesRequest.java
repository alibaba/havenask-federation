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

import org.havenask.action.support.nodes.BaseNodeRequest;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Used when querying every node in the cluster for dangling indices, in response to a list request.
 */
public class NodeListDanglingIndicesRequest extends BaseNodeRequest {
    /**
     * Filter the response by index UUID. Leave as null to find all indices.
     */
    private final String indexUUID;

    public NodeListDanglingIndicesRequest(String indexUUID) {
        this.indexUUID = indexUUID;
    }

    public NodeListDanglingIndicesRequest(StreamInput in) throws IOException {
        super(in);
        this.indexUUID = in.readOptionalString();
    }

    public String getIndexUUID() {
        return indexUUID;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(indexUUID);
    }
}

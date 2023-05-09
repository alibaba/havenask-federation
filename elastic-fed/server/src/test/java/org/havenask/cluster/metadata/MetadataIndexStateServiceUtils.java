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

package org.havenask.cluster.metadata;

import org.havenask.action.admin.indices.close.CloseIndexResponse;
import org.havenask.cluster.ClusterState;
import org.havenask.cluster.block.ClusterBlock;
import org.havenask.index.Index;
import org.havenask.cluster.metadata.MetadataIndexStateService;

import java.util.Map;

public class MetadataIndexStateServiceUtils {

    private MetadataIndexStateServiceUtils(){
    }

    /**
     * Allows to call {@link MetadataIndexStateService#addIndexClosedBlocks(Index[], Map, ClusterState)} which is a protected method.
     */
    public static ClusterState addIndexClosedBlocks(final Index[] indices, final Map<Index, ClusterBlock> blockedIndices,
                                                    final ClusterState state) {
        return MetadataIndexStateService.addIndexClosedBlocks(indices, blockedIndices, state);
    }

    /**
     * Allows to call {@link MetadataIndexStateService#closeRoutingTable(ClusterState, Map, Map)} which is a protected method.
     */
    public static ClusterState closeRoutingTable(final ClusterState state,
                                                 final Map<Index, ClusterBlock> blockedIndices,
                                                 final Map<Index, CloseIndexResponse.IndexResult> results) {
        return MetadataIndexStateService.closeRoutingTable(state, blockedIndices, results).v1();
    }
}

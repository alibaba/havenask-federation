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

package org.havenask.action.admin.indices.mapping.put;

import org.havenask.cluster.ack.IndicesClusterStateUpdateRequest;

/**
 * Cluster state update request that allows to put a mapping
 */
public class PutMappingClusterStateUpdateRequest extends IndicesClusterStateUpdateRequest<PutMappingClusterStateUpdateRequest> {

    private String type;

    private String source;

    public PutMappingClusterStateUpdateRequest() {

    }

    public String type() {
        return type;
    }

    public PutMappingClusterStateUpdateRequest type(String type) {
        this.type = type;
        return this;
    }

    public String source() {
        return source;
    }

    public PutMappingClusterStateUpdateRequest source(String source) {
        this.source = source;
        return this;
    }
}

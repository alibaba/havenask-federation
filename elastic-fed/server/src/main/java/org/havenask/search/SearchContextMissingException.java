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

package org.havenask.search;

import org.havenask.HavenaskException;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.io.stream.StreamOutput;
import org.havenask.rest.RestStatus;
import org.havenask.search.internal.ShardSearchContextId;

import java.io.IOException;

public class SearchContextMissingException extends HavenaskException {

    private final ShardSearchContextId contextId;

    public SearchContextMissingException(ShardSearchContextId contextId) {
        super("No search context found for id [" + contextId.getId() + "]");
        this.contextId = contextId;
    }

    public ShardSearchContextId contextId() {
        return this.contextId;
    }

    @Override
    public RestStatus status() {
        return RestStatus.NOT_FOUND;
    }

    public SearchContextMissingException(StreamInput in) throws IOException{
        super(in);
        contextId = new ShardSearchContextId(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        contextId.writeTo(out);
    }
}

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

package org.havenask.join.aggregations;

import org.havenask.common.io.stream.StreamInput;
import org.havenask.search.aggregations.InternalAggregations;
import org.havenask.search.aggregations.bucket.InternalSingleBucketAggregation;

import java.io.IOException;
import java.util.Map;

/**
 * Results of the {@link ChildrenToParentAggregator}.
 */
public class InternalParent extends InternalSingleBucketAggregation implements Parent {
    public InternalParent(String name, long docCount, InternalAggregations aggregations, Map<String, Object> metadata) {
        super(name, docCount, aggregations, metadata);
    }

    /**
     * Read from a stream.
     */
    public InternalParent(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ParentAggregationBuilder.NAME;
    }

    @Override
    protected InternalSingleBucketAggregation newAggregation(String name, long docCount, InternalAggregations subAggregations) {
        return new InternalParent(name, docCount, subAggregations, getMetadata());
    }
}

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

package org.havenask.action.admin.indices.segments;

import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.havenask.cluster.routing.ShardRouting;
import org.havenask.cluster.routing.ShardRoutingState;
import org.havenask.cluster.routing.TestShardRouting;
import org.havenask.common.xcontent.ToXContent;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.index.engine.Segment;
import org.havenask.test.HavenaskTestCase;
import org.havenask.action.admin.indices.segments.IndicesSegmentResponse;
import org.havenask.action.admin.indices.segments.ShardSegments;

import java.util.Collections;

import static org.havenask.common.xcontent.XContentFactory.jsonBuilder;

public class IndicesSegmentResponseTests extends HavenaskTestCase {

    public void testToXContentSerialiationWithSortedFields() throws Exception {
        ShardRouting shardRouting = TestShardRouting.newShardRouting("foo", 0, "node_id", true, ShardRoutingState.STARTED);
        Segment segment = new Segment("my");

        SortField sortField = new SortField("foo", SortField.Type.STRING);
        sortField.setMissingValue(SortField.STRING_LAST);
        segment.segmentSort = new Sort(sortField);

        ShardSegments shardSegments = new ShardSegments(shardRouting, Collections.singletonList(segment));
        IndicesSegmentResponse response =
            new IndicesSegmentResponse(new ShardSegments[] { shardSegments }, 1, 1, 0, Collections.emptyList());
        try (XContentBuilder builder = jsonBuilder()) {
            response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        }
    }
}

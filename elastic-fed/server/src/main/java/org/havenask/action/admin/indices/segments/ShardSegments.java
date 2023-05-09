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

package org.havenask.action.admin.indices.segments;

import org.havenask.cluster.routing.ShardRouting;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.io.stream.StreamOutput;
import org.havenask.common.io.stream.Writeable;
import org.havenask.index.engine.Segment;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class ShardSegments implements Writeable, Iterable<Segment> {

    private final ShardRouting shardRouting;

    private final List<Segment> segments;

    ShardSegments(ShardRouting shardRouting, List<Segment> segments) {
        this.shardRouting = shardRouting;
        this.segments = segments;
    }

    ShardSegments(StreamInput in) throws IOException {
        shardRouting = new ShardRouting(in);
        segments = in.readList(Segment::new);
    }

    @Override
    public Iterator<Segment> iterator() {
        return segments.iterator();
    }

    public ShardRouting getShardRouting() {
        return this.shardRouting;
    }

    public List<Segment> getSegments() {
        return this.segments;
    }

    public int getNumberOfCommitted() {
        int count = 0;
        for (Segment segment : segments) {
            if (segment.isCommitted()) {
                count++;
            }
        }
        return count;
    }

    public int getNumberOfSearch() {
        int count = 0;
        for (Segment segment : segments) {
            if (segment.isSearch()) {
                count++;
            }
        }
        return count;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        shardRouting.writeTo(out);
        out.writeList(segments);
    }
}

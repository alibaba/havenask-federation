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

package org.havenask.http;

import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.io.stream.StreamOutput;
import org.havenask.common.io.stream.Writeable;
import org.havenask.common.xcontent.ToXContentFragment;
import org.havenask.common.xcontent.XContentBuilder;

import java.io.IOException;

public class HttpStats implements Writeable, ToXContentFragment {

    private final long serverOpen;
    private final long totalOpen;

    public HttpStats(long serverOpen, long totalOpened) {
        this.serverOpen = serverOpen;
        this.totalOpen = totalOpened;
    }

    public HttpStats(StreamInput in) throws IOException {
        serverOpen = in.readVLong();
        totalOpen = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(serverOpen);
        out.writeVLong(totalOpen);
    }

    public long getServerOpen() {
        return this.serverOpen;
    }

    public long getTotalOpen() {
        return this.totalOpen;
    }

    static final class Fields {
        static final String HTTP = "http";
        static final String CURRENT_OPEN = "current_open";
        static final String TOTAL_OPENED = "total_opened";
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.HTTP);
        builder.field(Fields.CURRENT_OPEN, serverOpen);
        builder.field(Fields.TOTAL_OPENED, totalOpen);
        builder.endObject();
        return builder;
    }
}

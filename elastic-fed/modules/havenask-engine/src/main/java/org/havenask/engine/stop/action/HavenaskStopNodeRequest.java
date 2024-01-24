/*
 * Copyright (c) 2021, Alibaba Group;
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.havenask.engine.stop.action;

import org.havenask.action.support.nodes.BaseNodeRequest;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.io.stream.StreamOutput;

import java.io.IOException;

public class HavenaskStopNodeRequest extends BaseNodeRequest {
    HavenaskStopRequest request;

    public HavenaskStopNodeRequest(StreamInput in) throws IOException {
        super(in);
        request = new HavenaskStopRequest(in);
    }

    public HavenaskStopNodeRequest(HavenaskStopRequest havenaskStopRequest) {
        this.request = havenaskStopRequest;
    }

    public String getRole() {
        return request.getRole();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        request.writeTo(out);
    }
}

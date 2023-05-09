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

package org.havenask.action.admin.cluster.node.tasks.cancel;

import org.havenask.HavenaskException;
import org.havenask.action.TaskOperationFailure;
import org.havenask.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.havenask.common.Strings;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.xcontent.ConstructingObjectParser;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.common.xcontent.XContentParser;
import org.havenask.tasks.TaskInfo;

import java.io.IOException;
import java.util.List;


/**
 * Returns the list of tasks that were cancelled
 */
public class CancelTasksResponse extends ListTasksResponse {

    private static final ConstructingObjectParser<CancelTasksResponse, Void> PARSER =
        setupParser("cancel_tasks_response", CancelTasksResponse::new);

    public CancelTasksResponse(StreamInput in) throws IOException {
        super(in);
    }

    public CancelTasksResponse(List<TaskInfo> tasks, List<TaskOperationFailure> taskFailures, List<? extends HavenaskException>
        nodeFailures) {
        super(tasks, taskFailures, nodeFailures);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return super.toXContent(builder, params);
    }

    public static CancelTasksResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }
}

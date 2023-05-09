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

import org.havenask.LegacyESVersion;
import org.havenask.action.support.tasks.BaseTasksRequest;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.io.stream.StreamOutput;
import org.havenask.tasks.CancellableTask;
import org.havenask.tasks.Task;

import java.io.IOException;
import java.util.Arrays;

/**
 * A request to cancel tasks
 */
public class CancelTasksRequest extends BaseTasksRequest<CancelTasksRequest> {

    public static final String DEFAULT_REASON = "by user request";
    public static final boolean DEFAULT_WAIT_FOR_COMPLETION = false;

    private String reason = DEFAULT_REASON;
    private boolean waitForCompletion = DEFAULT_WAIT_FOR_COMPLETION;

    public CancelTasksRequest() {}

    public CancelTasksRequest(StreamInput in) throws IOException {
        super(in);
        this.reason = in.readString();
        if (in.getVersion().onOrAfter(LegacyESVersion.V_7_8_0)) {
            waitForCompletion = in.readBoolean();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(reason);
        if (out.getVersion().onOrAfter(LegacyESVersion.V_7_8_0)) {
            out.writeBoolean(waitForCompletion);
        }
    }

    @Override
    public boolean match(Task task) {
        return super.match(task) && task instanceof CancellableTask;
    }

    /**
     * Set the reason for canceling the task.
     */
    public CancelTasksRequest setReason(String reason) {
        this.reason = reason;
        return this;
    }

    /**
     * The reason for canceling the task.
     */
    public String getReason() {
        return reason;
    }

    /**
     * If {@code true}, the request blocks until the cancellation of the task and its descendant tasks is completed.
     * Otherwise, the request can return soon after the cancellation is started. Defaults to {@code false}.
     */
    public void setWaitForCompletion(boolean waitForCompletion) {
        this.waitForCompletion = waitForCompletion;
    }

    public boolean waitForCompletion() {
        return waitForCompletion;
    }

    @Override
    public String getDescription() {
        return "reason[" + reason +
            "], waitForCompletion[" + waitForCompletion +
            "], taskId[" + getTaskId() +
            "], parentTaskId[" + getParentTaskId() +
            "], nodes" + Arrays.toString(getNodes()) +
            ", actions" + Arrays.toString(getActions());
    }
}

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

package org.havenask.action.admin.cluster.node.tasks;

import org.havenask.common.bytes.BytesArray;
import org.havenask.common.xcontent.XContentHelper;
import org.havenask.tasks.TaskId;
import org.havenask.tasks.TaskInfo;
import org.havenask.test.HavenaskTestCase;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;

public class TaskTests extends HavenaskTestCase {

    public void testTaskInfoToString() {
        String nodeId = randomAlphaOfLength(10);
        long taskId = randomIntBetween(0, 100000);
        long startTime = randomNonNegativeLong();
        long runningTime = randomNonNegativeLong();
        boolean cancellable = randomBoolean();
        TaskInfo taskInfo = new TaskInfo(new TaskId(nodeId, taskId), "test_type",
            "test_action", "test_description", null, startTime, runningTime, cancellable, TaskId.EMPTY_TASK_ID,
            Collections.singletonMap("foo", "bar"));
        String taskInfoString = taskInfo.toString();
        Map<String, Object> map = XContentHelper.convertToMap(new BytesArray(taskInfoString.getBytes(StandardCharsets.UTF_8)), true).v2();
        assertEquals(((Number)map.get("id")).longValue(), taskId);
        assertEquals(map.get("type"), "test_type");
        assertEquals(map.get("action"), "test_action");
        assertEquals(map.get("description"), "test_description");
        assertEquals(((Number)map.get("start_time_in_millis")).longValue(), startTime);
        assertEquals(((Number)map.get("running_time_in_nanos")).longValue(), runningTime);
        assertEquals(map.get("cancellable"), cancellable);
        assertEquals(map.get("headers"), Collections.singletonMap("foo", "bar"));
    }

}

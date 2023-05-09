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

package org.havenask.action;

import org.havenask.test.HavenaskTestCase;
import org.havenask.action.ActionResponse;
import org.havenask.action.ActionType;

public class ActionTests extends HavenaskTestCase {

    public void testEquals() {
        class FakeAction extends ActionType<ActionResponse> {
            protected FakeAction(String name) {
                super(name, null);
            }
        }
        FakeAction fakeAction1 = new FakeAction("a");
        FakeAction fakeAction2 = new FakeAction("a");
        FakeAction fakeAction3 = new FakeAction("b");
        String s = "Some random other object";
        assertEquals(fakeAction1, fakeAction1);
        assertEquals(fakeAction2, fakeAction2);
        assertNotEquals(fakeAction1, null);
        assertNotEquals(fakeAction1, fakeAction3);
        assertNotEquals(fakeAction1, s);
    }
}

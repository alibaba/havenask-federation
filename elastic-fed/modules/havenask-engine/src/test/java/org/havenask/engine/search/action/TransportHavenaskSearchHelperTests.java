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

package org.havenask.engine.search.action;

import org.havenask.common.UUIDs;
import org.havenask.test.HavenaskTestCase;

public class TransportHavenaskSearchHelperTests extends HavenaskTestCase {
    public void testBuildAndParseHavenaskScrollId() {
        String nodeId = UUIDs.randomBase64UUID();
        String sessionId = UUIDs.randomBase64UUID();
        String scrollId = TransportHavenaskSearchHelper.buildHavenaskScrollId(nodeId, sessionId);

        ParsedHavenaskScrollId parsedScrollId = TransportHavenaskSearchHelper.parseHavenaskScrollId(scrollId);
        assertEquals(nodeId, parsedScrollId.getNodeId());
        assertEquals(sessionId, parsedScrollId.getScrollSessionId());
    }
}

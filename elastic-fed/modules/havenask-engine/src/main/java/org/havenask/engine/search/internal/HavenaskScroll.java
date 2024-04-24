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

package org.havenask.engine.search.internal;

import org.havenask.search.Scroll;

public class HavenaskScroll {
    private String nodeId;
    private String lastEmittedDocId = null;
    private Scroll havenaskScroll;

    public HavenaskScroll(String nodeId, Scroll havenaskScroll) {
        this.nodeId = nodeId;
        this.havenaskScroll = havenaskScroll;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setLastEmittedDocId(String lastEmittedDocId) {
        this.lastEmittedDocId = lastEmittedDocId;
    }

    public String getLastEmittedDocId() {
        return lastEmittedDocId;
    }

    public Scroll getHavenaskScroll() {
        return havenaskScroll;
    }
}
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

import java.util.Objects;

public class HavenaskScroll {
    public static final String SCROLL_ORDER_BY = "ORDER BY `_id` ASC";
    private String nodeId;
    private String lastEmittedDocId = null;
    private Scroll scroll;

    public HavenaskScroll(String nodeId, Scroll scroll) {
        this.nodeId = nodeId;
        this.scroll = scroll;
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

    public Scroll getScroll() {
        return scroll;
    }

    public static boolean hasLastEmittedDoc(HavenaskScroll havenaskScroll) {
        return Objects.nonNull(havenaskScroll) && Objects.nonNull(havenaskScroll.getLastEmittedDocId());
    }
}

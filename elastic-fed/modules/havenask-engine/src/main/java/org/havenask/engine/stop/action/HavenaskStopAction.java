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

import org.havenask.action.ActionType;

public class HavenaskStopAction extends ActionType<HavenaskStopResponse> {

    public static final HavenaskStopAction INSTANCE = new HavenaskStopAction();
    public static final String NAME = "cluster:admin/havenask/stop/searcher";

    private HavenaskStopAction() {
        super(NAME, HavenaskStopResponse::new);
    }
}

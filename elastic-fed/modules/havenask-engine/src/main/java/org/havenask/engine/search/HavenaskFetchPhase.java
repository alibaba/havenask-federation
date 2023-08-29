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

package org.havenask.engine.search;

import java.util.List;

import org.havenask.engine.index.engine.EngineSettings;
import org.havenask.search.fetch.DefaultFetchPhase;
import org.havenask.search.fetch.FetchPhase;
import org.havenask.search.fetch.FetchSubPhase;
import org.havenask.search.internal.SearchContext;

public class HavenaskFetchPhase implements FetchPhase {

    private final List<FetchSubPhase> fetchSubPhases;
    private final DefaultFetchPhase defaultFetchPhase;

    public HavenaskFetchPhase(List<FetchSubPhase> fetchSubPhases) {
        this.fetchSubPhases = fetchSubPhases;
        this.defaultFetchPhase = new DefaultFetchPhase(fetchSubPhases);
    }

    @Override
    public void execute(SearchContext context) {
        if (false == EngineSettings.isHavenaskEngine(context.indexShard().indexSettings().getSettings())) {
            defaultFetchPhase.execute(context);
            return;
        }

        // TODO add havenask fetch phase
    }
}

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

package org.havenask.engine.search.fetch;

import org.havenask.search.SearchHit;
import org.havenask.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;

public interface HavenaskFetchSubPhase {
    class HitContent {
        SearchHit hit;
        Object source;

        public HitContent(SearchHit searchHit, Object source) {
            this.hit = searchHit;
            this.source = source;
        }

        public SearchHit getHit() {
            return hit;
        }
    }

    HavenaskFetchSubPhaseProcessor getProcessor(String indexName, FetchSourceContext fetchSourceContext) throws IOException;
}

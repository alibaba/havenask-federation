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

import org.havenask.client.ha.SqlResponse;
import org.havenask.common.text.Text;
import org.havenask.index.mapper.MapperService;
import org.havenask.search.SearchHit;
import org.havenask.search.internal.SearchContext;

import java.io.IOException;
import java.util.Collections;

public interface FetchSubPhase {

    class HitContent {
        SearchHit hit;
        SqlResponse sqlResponse;
        SearchContext context;
        int index;

        public HitContent(SqlResponse sqlResponse, SearchContext context, int index) {
            this.hit = new SearchHit(
                index,
                (String) sqlResponse.getSqlResult().getData()[index][1],
                new Text(MapperService.SINGLE_MAPPING_NAME),
                Collections.emptyMap(),
                Collections.emptyMap()
            );
            this.sqlResponse = sqlResponse;
            this.context = context;
            this.index = index;
        }

        public SearchHit getHit() {
            return hit;
        }
    }

    FetchSubPhaseProcessor getProcessor(SearchContext searchContext) throws IOException;
}

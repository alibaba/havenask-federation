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

package org.havenask.engine.search.dsl.plan;

import java.io.IOException;

import org.havenask.action.search.SearchResponse;
import org.havenask.action.search.SearchResponseSections;
import org.havenask.engine.search.dsl.DSLSession;
import org.havenask.engine.search.dsl.expression.SourceExpression;
import org.havenask.search.SearchHits;
import org.havenask.search.builder.SearchSourceBuilder;

public class DSLExec implements Executable<SearchResponse> {
    private final SearchSourceBuilder dsl;
    private final SourceExpression sourceExpression;

    public DSLExec(SearchSourceBuilder dsl) {
        this.dsl = dsl;
        this.sourceExpression = new SourceExpression(dsl);
    }

    @Override
    public SearchResponse execute(DSLSession session) throws IOException {
        // exec query
        SearchHits searchHits = SearchHits.empty();
        if (dsl.size() > 0) {
            QueryExec queryExec = new QueryExec(sourceExpression.getQuerySQLExpression(session.getIndex()));
            searchHits = queryExec.execute(session);
        }

        // exec aggregation
        if (dsl.aggregations() != null) {

        }

        // build response
        SearchResponseSections searchResponseSections = new SearchResponseSections(searchHits, null, null, false, false, null, 0);
        // TODO get shards
        return new SearchResponse(searchResponseSections, null, 0, 0, 0, session.getTook(), null, null);
    }
}

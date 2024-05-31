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

import java.util.Map;
import java.util.Objects;

import org.havenask.action.search.SearchResponse;
import org.havenask.action.search.ShardSearchFailure;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.engine.search.action.TransportHavenaskSearchHelper;
import org.havenask.engine.search.dsl.DSLSession;
import org.havenask.engine.search.dsl.expression.ExpressionContext;
import org.havenask.engine.search.dsl.expression.SourceExpression;
import org.havenask.engine.search.internal.HavenaskScroll;
import org.havenask.search.SearchHits;
import org.havenask.search.aggregations.InternalAggregations;
import org.havenask.search.builder.SearchSourceBuilder;
import org.havenask.search.internal.InternalSearchResponse;

public class DSLExec implements Executable<SearchResponse> {
    private final SearchSourceBuilder dsl;
    private final SourceExpression sourceExpression;

    public DSLExec(SearchSourceBuilder dsl, ExpressionContext context) {
        this.dsl = dsl;
        this.sourceExpression = new SourceExpression(dsl, context);
    }

    @Override
    public SearchResponse execute(DSLSession session) throws Exception {
        // exec query
        SearchHits searchHits = SearchHits.empty();
        if (sourceExpression.size() > 0) {
            IndexMetadata indexMetadata = session.getIndexMetadata();
            Map<String, Object> indexMapping = indexMetadata.mapping() != null ? indexMetadata.mapping().getSourceAsMap() : null;
            QueryExec queryExec = new QueryExec(sourceExpression.getQuerySQLExpression(session.getIndex(), indexMapping));
            searchHits = queryExec.execute(session);
            if (Objects.nonNull(sourceExpression.getHavenaskScroll())) {
                int nextLastEmittedDocPos = searchHits.getHits().length - 1;
                if (nextLastEmittedDocPos >= 0) {
                    sourceExpression.setLastEmittedDocId(searchHits.getHits()[nextLastEmittedDocPos].getId());
                }
            }
        }

        // exec aggregation
        InternalAggregations aggregations = InternalAggregations.EMPTY;
        if (sourceExpression.getAggregationSQLExpressions(session.getIndex()).size() > 0) {
            AggExec aggExec = new AggExec(sourceExpression.getAggregationSQLExpressions(session.getIndex()));
            aggregations = aggExec.execute(session);
        }

        IndexMetadata indexMetadata = session.getIndexMetadata();

        // build scrollId
        String scrollId = null;
        if (Objects.nonNull(sourceExpression.getHavenaskScroll())) {
            HavenaskScroll havenaskScroll = sourceExpression.getHavenaskScroll();
            scrollId = TransportHavenaskSearchHelper.buildHavenaskScrollId(havenaskScroll.getNodeId(), session.getSessionId());
        }

        // build response
        InternalSearchResponse internalSearchResponse = new InternalSearchResponse(searchHits, aggregations, null, null, false, false, 1);
        return new SearchResponse(
            internalSearchResponse,
            scrollId,
            indexMetadata.getNumberOfShards(),
            indexMetadata.getNumberOfShards(),
            0,
            session.getTook(),
            ShardSearchFailure.EMPTY_ARRAY,
            SearchResponse.Clusters.EMPTY
        );
    }
}

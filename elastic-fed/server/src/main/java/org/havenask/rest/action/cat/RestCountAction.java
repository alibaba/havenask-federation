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

package org.havenask.rest.action.cat;

import org.apache.lucene.search.TotalHits;
import org.havenask.HavenaskException;
import org.havenask.action.search.SearchRequest;
import org.havenask.action.search.SearchResponse;
import org.havenask.client.node.NodeClient;
import org.havenask.common.Strings;
import org.havenask.common.Table;
import org.havenask.index.query.QueryBuilder;
import org.havenask.rest.RestRequest;
import org.havenask.rest.RestResponse;
import org.havenask.rest.action.RestActions;
import org.havenask.rest.action.RestResponseListener;
import org.havenask.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.havenask.rest.RestRequest.Method.GET;

public class RestCountAction extends AbstractCatAction {

    @Override
    public List<Route> routes() {
        return unmodifiableList(asList(
            new Route(GET, "/_cat/count"),
            new Route(GET, "/_cat/count/{index}")));
    }

    @Override
    public String getName() {
        return "cat_count_action";
    }

    @Override
    protected void documentation(StringBuilder sb) {
        sb.append("/_cat/count\n");
        sb.append("/_cat/count/{index}\n");
    }

    @Override
    public RestChannelConsumer doCatRequest(final RestRequest request, final NodeClient client) {
        String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        SearchRequest countRequest = new SearchRequest(indices);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().size(0).trackTotalHits(true);
        countRequest.source(searchSourceBuilder);
        try {
            request.withContentOrSourceParamParserOrNull(parser -> {
                if (parser == null) {
                    QueryBuilder queryBuilder = RestActions.urlParamsToQueryBuilder(request);
                    if (queryBuilder != null) {
                        searchSourceBuilder.query(queryBuilder);
                    }
                } else {
                    searchSourceBuilder.query(RestActions.getQueryContent(parser));
                }
            });
        } catch (IOException e) {
            throw new HavenaskException("Couldn't parse query", e);
        }
        return channel -> client.search(countRequest, new RestResponseListener<SearchResponse>(channel) {
            @Override
            public RestResponse buildResponse(SearchResponse countResponse) throws Exception {
                assert countResponse.getHits().getTotalHits().relation == TotalHits.Relation.EQUAL_TO;
                return RestTable.buildResponse(buildTable(request, countResponse), channel);
            }
        });
    }

    @Override
    protected Table getTableWithHeader(final RestRequest request) {
        Table table = new Table();
        table.startHeadersWithTimestamp();
        table.addCell("count", "alias:dc,docs.count,docsCount;desc:the document count");
        table.endHeaders();
        return table;
    }

    private Table buildTable(RestRequest request, SearchResponse response) {
        Table table = getTableWithHeader(request);
        table.startRow();
        table.addCell(response.getHits().getTotalHits().value);
        table.endRow();

        return table;
    }
}

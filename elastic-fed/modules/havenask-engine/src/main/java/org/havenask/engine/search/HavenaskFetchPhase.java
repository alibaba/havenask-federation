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

import java.io.IOException;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.TotalHits;
import org.havenask.client.ha.SqlResponse;
import org.havenask.common.xcontent.DeprecationHandler;
import org.havenask.common.xcontent.NamedXContentRegistry;
import org.havenask.common.xcontent.XContentParser;
import org.havenask.common.xcontent.XContentType;
import org.havenask.engine.index.engine.EngineSettings;
import org.havenask.engine.rpc.QrsClient;
import org.havenask.engine.rpc.QrsSqlRequest;
import org.havenask.engine.rpc.QrsSqlResponse;
import org.havenask.search.SearchContextSourcePrinter;
import org.havenask.search.SearchHit;
import org.havenask.search.SearchHits;
import org.havenask.search.fetch.DefaultFetchPhase;
import org.havenask.search.fetch.FetchPhase;
import org.havenask.search.fetch.FetchSubPhase;
import org.havenask.search.internal.SearchContext;
import org.havenask.tasks.TaskCancelledException;

import static org.havenask.engine.search.rest.RestHavenaskSqlAction.SQL_DATABASE;

public class HavenaskFetchPhase implements FetchPhase {
    public static final String IDS_CONTEXT = "havenask_ids";
    private final QrsClient qrsHttpClient;

    private static final Logger LOGGER = LogManager.getLogger(FetchPhase.class);

    // TODO fetchSubPhases
    private final List<FetchSubPhase> fetchSubPhases;
    private final DefaultFetchPhase defaultFetchPhase;

    public HavenaskFetchPhase(QrsClient qrsHttpClient, List<FetchSubPhase> fetchSubPhases) {
        this.qrsHttpClient = qrsHttpClient;
        this.fetchSubPhases = fetchSubPhases;
        this.defaultFetchPhase = new DefaultFetchPhase(fetchSubPhases);
    }

    @Override
    public void execute(SearchContext context) throws IOException {
        if (false == EngineSettings.isHavenaskEngine(context.indexShard().indexSettings().getSettings())) {
            defaultFetchPhase.execute(context);
            return;
        }

        // TODO add havenask fetch phase
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("{}", new SearchContextSourcePrinter(context));
        }

        if (context.isCancelled()) {
            throw new TaskCancelledException("cancelled");
        }

        List<String> ids = context.readerContext().getFromContext(IDS_CONTEXT);
        SqlResponse sqlResponse = fetchWithSql(ids, context);
        TotalHits totalHits = context.queryResult().getTotalHits();
        SearchHit[] hits = new SearchHit[sqlResponse.getRowCount()];
        // TODO transfer sqlResponse2hits
        context.fetchResult().hits(new SearchHits(hits, totalHits, context.queryResult().getMaxScore()));
    }

    private SqlResponse fetchWithSql(List<String> ids, SearchContext context) throws IOException {
        StringBuilder sqlQuery = new StringBuilder();
        sqlQuery.append("select _source,_id,_routing from " + context.readerContext().indexShard().shardId() + "_summary_");
        sqlQuery.append(" where _id in(");
        for (int i = 0; i < ids.size(); i++) {
            sqlQuery.append("'" + ids.get(i) + "'");
            if (i < ids.size() - 1) {
                sqlQuery.append(",");
            }
        }
        sqlQuery.append(")");
        String kvpair = "format:full_json;timeout:10000;databaseName:" + SQL_DATABASE;
        QrsSqlRequest request = new QrsSqlRequest(sqlQuery.toString(), kvpair);
        QrsSqlResponse response = qrsHttpClient.executeSql(request);
        XContentParser parser = XContentType.JSON.xContent()
            .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.IGNORE_DEPRECATIONS, response.getResult());
        SqlResponse sqlResponse = SqlResponse.fromXContent(parser);
        return sqlResponse;
    }
}

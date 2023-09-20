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
import java.util.Collections;
import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.TotalHits;
import org.havenask.client.ha.SqlResponse;
import org.havenask.common.bytes.BytesArray;
import org.havenask.common.text.Text;
import org.havenask.engine.index.engine.EngineSettings;
import org.havenask.engine.index.engine.HavenaskIndexSearcher;
import org.havenask.engine.rpc.QrsClient;
import org.havenask.engine.rpc.QrsSqlRequest;
import org.havenask.engine.rpc.QrsSqlResponse;
import org.havenask.engine.search.fetch.FetchSourcePhase;
import org.havenask.engine.search.fetch.FetchSubPhaseProcessor;
import org.havenask.engine.util.Utils;
import org.havenask.index.mapper.MapperService;
import org.havenask.search.SearchContextSourcePrinter;
import org.havenask.search.SearchHit;
import org.havenask.search.SearchHits;
import org.havenask.search.SearchShardTarget;
import org.havenask.search.fetch.DefaultFetchPhase;
import org.havenask.search.fetch.FetchPhase;
import org.havenask.search.fetch.FetchPhaseExecutionException;
import org.havenask.search.fetch.FetchSubPhase;
import org.havenask.search.internal.SearchContext;
import org.havenask.tasks.TaskCancelledException;
import org.havenask.engine.search.fetch.FetchSubPhase.HitContent;

import static org.havenask.engine.search.rest.RestHavenaskSqlAction.SQL_DATABASE;

public class HavenaskFetchPhase implements FetchPhase {
    private final QrsClient qrsHttpClient;
    private static final int SOURCE_POS = 0;
    private static final int ID_POS = 1;

    private static final Logger LOGGER = LogManager.getLogger(FetchPhase.class);

    // TODO fetchSubPhases
    private final List<FetchSubPhase> fetchSubPhases;
    private final DefaultFetchPhase defaultFetchPhase;
    private final List<org.havenask.engine.search.fetch.FetchSubPhase> HavenaskFetchSubPhases;

    public HavenaskFetchPhase(QrsClient qrsHttpClient, List<FetchSubPhase> fetchSubPhases) {
        this.qrsHttpClient = qrsHttpClient;
        this.fetchSubPhases = fetchSubPhases;
        this.defaultFetchPhase = new DefaultFetchPhase(fetchSubPhases);

        // TODO 目前仅支持source过滤，未来增加更多的subPhase并考虑以plugin形式去支持
        this.HavenaskFetchSubPhases = new ArrayList<>();
        this.HavenaskFetchSubPhases.add(new FetchSourcePhase());
    }

    @Override
    public void execute(SearchContext context) {
        if (false == EngineSettings.isHavenaskEngine(context.indexShard().indexSettings().getSettings())) {
            defaultFetchPhase.execute(context);
            return;
        }

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("{}", new SearchContextSourcePrinter(context));
        }

        if (context.isCancelled()) {
            throw new TaskCancelledException("cancelled");
        }

        if (context.docIdsToLoadSize() == 0) {
            // no individual hits to process, so we shortcut
            context.fetchResult()
                .hits(new SearchHits(new SearchHit[0], context.queryResult().getTotalHits(), context.queryResult().getMaxScore()));
            return;
        }

        try {
            DocIdToIndex[] docs = new DocIdToIndex[context.docIdsToLoadSize()];
            for (int index = 0; index < context.docIdsToLoadSize(); index++) {
                docs[index] = new DocIdToIndex(context.docIdsToLoad()[context.docIdsToLoadFrom() + index], index);
            }
            // make sure that we iterate in doc id order
            Arrays.sort(docs);

            List<String> ids = context.readerContext().getFromContext(HavenaskIndexSearcher.IDS_CONTEXT);
            SqlResponse sqlResponse = fetchWithSql(docs, ids, context);
            transferSqlResponse2FetchResult(docs, sqlResponse, context);
        } catch (IOException e) {
            throw new FetchPhaseExecutionException(context.shardTarget(), "Error running havenask fetch phase", e);
        }
    }

    private SqlResponse fetchWithSql(DocIdToIndex[] docs, List<String> ids, SearchContext context) throws IOException {
        StringBuilder sqlQuery = new StringBuilder();
        String tableName = Utils.getHavenaskTableName(context.indexShard().shardId());
        sqlQuery.append("select _source,_id from " + tableName + "_summary_");
        sqlQuery.append(" where _id in(");
        for (int index = 0; index < context.docIdsToLoadSize(); index++) {
            if (context.isCancelled()) {
                throw new TaskCancelledException("cancelled");
            }
            int docId = docs[index].docId;
            sqlQuery.append("'" + ids.get(docId) + "'");
            if (index < context.docIdsToLoadSize() - 1) {
                sqlQuery.append(",");
            }
        }
        sqlQuery.append(")");
        String kvpair = "format:full_json;timeout:10000;databaseName:" + SQL_DATABASE;
        QrsSqlRequest request = new QrsSqlRequest(sqlQuery.toString(), kvpair);
        QrsSqlResponse response = qrsHttpClient.executeSql(request);
        return SqlResponse.parse(response.getResult());
    }

    private void transferSqlResponse2FetchResult(DocIdToIndex[] docs, SqlResponse sqlResponse, SearchContext context) throws IOException {
        TotalHits totalHits = context.queryResult().getTotalHits();
        SearchHit[] hits = new SearchHit[sqlResponse.getRowCount()];

        List<FetchSubPhaseProcessor> processors = getProcessors(context.shardTarget(), context);

        for (int i = 0; i < sqlResponse.getRowCount(); i++) {
            // TODO add _routing
            SearchHit searchHit = new SearchHit(
                docs[i].docId,
                (String) sqlResponse.getSqlResult().getData()[i][ID_POS],
                new Text(MapperService.SINGLE_MAPPING_NAME),
                Collections.emptyMap(),
                Collections.emptyMap()
            );
            HitContent hit = new HitContent(searchHit, sqlResponse.getSqlResult().getData()[i][SOURCE_POS]);
            if (processors != null && processors.size() > 0) {
                for (FetchSubPhaseProcessor processor : processors) {
                    processor.process(hit);
                }
                hits[i] = hit.getHit();
            } else {
                hits[i] = hit.getHit();
                hits[i].sourceRef(new BytesArray((String) sqlResponse.getSqlResult().getData()[i][SOURCE_POS]));
            }
        }

        context.fetchResult().hits(new SearchHits(hits, totalHits, context.queryResult().getMaxScore()));
    }

    List<FetchSubPhaseProcessor> getProcessors(SearchShardTarget target, SearchContext context) {
        try {
            List<FetchSubPhaseProcessor> processors = new ArrayList<>();
            for (org.havenask.engine.search.fetch.FetchSubPhase fsp : HavenaskFetchSubPhases) {
                FetchSubPhaseProcessor processor = fsp.getProcessor(context);
                if (processor != null) {
                    processors.add(processor);
                }
            }
            return processors;
        } catch (Exception e) {
            throw new FetchPhaseExecutionException(target, "Error building fetch sub-phases", e);
        }
    }

    static class DocIdToIndex implements Comparable<DocIdToIndex> {
        final int docId;
        final int index;

        DocIdToIndex(int docId, int index) {
            this.docId = docId;
            this.index = index;
        }

        @Override
        public int compareTo(DocIdToIndex o) {
            return Integer.compare(docId, o.docId);
        }
    }
}

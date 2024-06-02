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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.havenask.client.ha.SqlResponse;
import org.havenask.common.bytes.BytesArray;
import org.havenask.common.document.DocumentField;
import org.havenask.common.lucene.search.TopDocsAndMaxScore;
import org.havenask.common.text.Text;
import org.havenask.engine.rpc.QrsClient;
import org.havenask.engine.rpc.QrsSqlRequest;
import org.havenask.engine.rpc.QrsSqlResponse;
import org.havenask.engine.search.fetch.HavenaskFetchSourcePhase;
import org.havenask.engine.search.fetch.HavenaskFetchSubPhase;
import org.havenask.engine.search.fetch.HavenaskFetchSubPhaseProcessor;
import org.havenask.index.mapper.MapperService;
import org.havenask.index.mapper.RoutingFieldMapper;
import org.havenask.search.SearchHit;
import org.havenask.search.SearchHits;
import org.havenask.search.aggregations.InternalAggregations;
import org.havenask.search.builder.SearchSourceBuilder;
import org.havenask.search.fetch.subphase.FetchSourceContext;
import org.havenask.search.internal.InternalSearchResponse;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.havenask.engine.search.rest.RestHavenaskSqlAction.SQL_DATABASE;

public class HavenaskSearchFetchProcessor {
    private static final Logger logger = LogManager.getLogger(HavenaskSearchFetchProcessor.class);
    private static final int ID_POS = 0;
    private static final int SCORE_POS = 1;
    private static final int SOURCE_POS = 1;
    private static final int ROUTING_POS = 2;
    private static final Object SOURCE_NOT_FOUND = "{\n" + "\"warn\":\"source not found\"\n" + "}";
    QrsClient qrsClient;
    private final List<HavenaskFetchSubPhase> havenaskFetchSubPhases;

    public HavenaskSearchFetchProcessor(QrsClient qrsClient) {
        this.qrsClient = qrsClient;
        // TODO 目前仅支持source过滤，未来增加更多的subPhase并考虑以plugin形式去支持
        this.havenaskFetchSubPhases = new ArrayList<>();
        this.havenaskFetchSubPhases.add(new HavenaskFetchSourcePhase());
    }

    public InternalSearchResponse executeFetch(
        SqlResponse queryPhaseSqlResponse,
        String tableName,
        SearchSourceBuilder searchSourceBuilder,
        Boolean sourceEnabled
    ) throws Exception {
        if (searchSourceBuilder == null) {
            throw new IllegalArgumentException("request source can not be null!");
        }
        List<String> idList = new ArrayList<>(queryPhaseSqlResponse.getRowCount());
        TopDocsAndMaxScore topDocsAndMaxScore = buildQuerySearchResult(queryPhaseSqlResponse, idList, searchSourceBuilder.from());
        SqlResponse fetchPhaseSqlResponse = (true == sourceEnabled
            && (searchSourceBuilder.fetchSource() == null || true == searchSourceBuilder.fetchSource().fetchSource()))
                ? havenaskFetchWithSql(idList, tableName, searchSourceBuilder.fetchSource(), qrsClient)
                : null;

        return transferSqlResponse2FetchResult(
            tableName,
            idList,
            fetchPhaseSqlResponse,
            topDocsAndMaxScore,
            searchSourceBuilder,
            sourceEnabled
        );
    }

    public SearchHits executeFetchHits(
        SqlResponse queryPhaseSqlResponse,
        String tableName,
        SearchSourceBuilder searchSourceBuilder,
        Boolean sourceEnabled
    ) throws Exception {
        if (searchSourceBuilder == null) {
            throw new IllegalArgumentException("request source can not be null!");
        }
        List<String> idList = new ArrayList<>(queryPhaseSqlResponse.getRowCount());
        TopDocsAndMaxScore topDocsAndMaxScore = buildQuerySearchResult(queryPhaseSqlResponse, idList, searchSourceBuilder.from());
        SqlResponse fetchPhaseSqlResponse = (true == sourceEnabled
            && (searchSourceBuilder.fetchSource() == null || true == searchSourceBuilder.fetchSource().fetchSource()))
                ? havenaskFetchWithSql(idList, tableName, searchSourceBuilder.fetchSource(), qrsClient)
                : null;

        return transferSqlResponse2SearchHits(
            tableName,
            idList,
            fetchPhaseSqlResponse,
            topDocsAndMaxScore,
            searchSourceBuilder,
            sourceEnabled
        );
    }

    public TopDocsAndMaxScore buildQuerySearchResult(SqlResponse queryPhaseSqlResponse, List<String> idList, int from) throws IOException {
        ScoreDoc[] queryScoreDocs = new ScoreDoc[queryPhaseSqlResponse.getRowCount()];
        // TODO: 当前通过sql没有较好的方法来获取totalHits的准确值，后续可能考虑优化
        int offset = Math.max(from, 0);
        int totalHitsValue = queryPhaseSqlResponse.getRowCount() > 0 ? queryPhaseSqlResponse.getRowCount() + offset : 0;
        float maxScore = 0;
        int sqlDataSize = queryPhaseSqlResponse.getRowCount() > 0 ? queryPhaseSqlResponse.getSqlResult().getData()[0].length : 0;
        if (sqlDataSize > 2) {
            throw new IOException("unknow sqlResponse:" + Arrays.deepToString(queryPhaseSqlResponse.getSqlResult().getData()));
        }
        for (int i = 0; i < queryPhaseSqlResponse.getRowCount(); i++) {
            float defaultScore = 1;
            float curScore = sqlDataSize == 1
                ? defaultScore
                : ((Double) queryPhaseSqlResponse.getSqlResult().getData()[i][SCORE_POS]).floatValue();

            queryScoreDocs[i] = new ScoreDoc(i, curScore);
            maxScore = Math.max(maxScore, curScore);
            idList.add(String.valueOf(queryPhaseSqlResponse.getSqlResult().getData()[i][ID_POS]));
        }
        TopDocs topDocs = new TopDocs(new TotalHits(totalHitsValue, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO), queryScoreDocs);
        return new TopDocsAndMaxScore(topDocs, maxScore);
    }

    public SqlResponse havenaskFetchWithSql(
        List<String> idList,
        String tableName,
        FetchSourceContext fetchSourceContext,
        QrsClient qrsClient
    ) throws Exception {
        QrsSqlRequest qrsFetchPhaseSqlRequest = getQrsFetchPhaseSqlRequest(idList, tableName);
        QrsSqlResponse qrsFetchPhaseSqlResponse = qrsClient.executeSql(qrsFetchPhaseSqlRequest);
        SqlResponse fetchPhaseSqlResponse = SqlResponse.parse(qrsFetchPhaseSqlResponse.getResult());
        if (fetchPhaseSqlResponse.getErrorInfo().getErrorCode() != 0) {
            throw new SQLException(
                String.format(
                    Locale.ROOT,
                    "execute fetch phase sql failed after transfer dsl to sql: errorCode: %s, error: %s, message: %s",
                    fetchPhaseSqlResponse.getErrorInfo().getErrorCode(),
                    fetchPhaseSqlResponse.getErrorInfo().getError(),
                    fetchPhaseSqlResponse.getErrorInfo().getMessage()
                )
            );
        }
        if (logger.isDebugEnabled()) {
            logger.debug("fetch idList length: {}, havenask sqlResponse took: {} ms", idList.size(), fetchPhaseSqlResponse.getTotalTime());
        }
        return fetchPhaseSqlResponse;
    }

    public static QrsSqlRequest getQrsFetchPhaseSqlRequest(List<String> idList, String tableName) {
        StringBuilder sqlQuery = new StringBuilder();
        sqlQuery.append("select _id, _source, _routing from ").append('`').append(tableName).append("_summary_` where contain(_id,'");
        for (int i = 0; i < idList.size(); i++) {
            sqlQuery.append(idList.get(i));
            if (i < idList.size() - 1) {
                sqlQuery.append("|");
            }
        }
        sqlQuery.append("')");
        sqlQuery.append(" limit ").append(idList.size());
        String kvpair = "format:full_json;timeout:30000;databaseName:" + SQL_DATABASE;
        return new QrsSqlRequest(sqlQuery.toString(), kvpair);
    }

    public InternalSearchResponse transferSqlResponse2FetchResult(
        String tableName,
        List<String> idList,
        SqlResponse fetchPhaseSqlResponse,
        TopDocsAndMaxScore topDocsAndMaxScore,
        SearchSourceBuilder searchSourceBuilder,
        Boolean sourceEnabled
    ) throws IOException {
        SearchHits searchHits = transferSqlResponse2SearchHits(
            tableName,
            idList,
            fetchPhaseSqlResponse,
            topDocsAndMaxScore,
            searchSourceBuilder,
            sourceEnabled
        );
        return new InternalSearchResponse(searchHits, InternalAggregations.EMPTY, null, null, false, null, 1);
    }

    public SearchHits transferSqlResponse2SearchHits(
        String tableName,
        List<String> idList,
        SqlResponse fetchPhaseSqlResponse,
        TopDocsAndMaxScore topDocsAndMaxScore,
        SearchSourceBuilder searchSourceBuilder,
        Boolean sourceEnabled
    ) throws IOException {
        int loadSize = idList.size();
        TotalHits totalHits = topDocsAndMaxScore.topDocs.totalHits;
        SearchHit[] hits = new SearchHit[loadSize];
        FetchSourceContext fetchSourceContext = searchSourceBuilder.fetchSource();
        List<HavenaskFetchSubPhaseProcessor> processors = getProcessors(tableName, searchSourceBuilder);

        // 记录fetch结果的_id和index的映射关系, query阶段查到的idList是根据_score值排序好的，但fetch结果非有序
        Map<String, Integer> fetchResIdListMap = new HashMap<>();
        if (fetchPhaseSqlResponse != null) {
            for (int i = 0; i < fetchPhaseSqlResponse.getRowCount(); i++) {
                fetchResIdListMap.put((String) fetchPhaseSqlResponse.getSqlResult().getData()[i][ID_POS], i);
            }
        }

        for (int i = 0; i < loadSize; i++) {
            // 根据idList的顺序从fetch结果获取相对应的_source, 如果数据丢失则返回_source not found
            Integer fetchResIndex = fetchResIdListMap.get(idList.get(i));
            Object source = null;
            if (true == sourceEnabled && (fetchSourceContext == null || true == fetchSourceContext.fetchSource())) {
                source = fetchResIndex != null
                    ? fetchPhaseSqlResponse.getSqlResult().getData()[fetchResIndex][SOURCE_POS]
                    : SOURCE_NOT_FOUND;
            }

            Map<String, DocumentField> metaFields = new HashMap<>();
            // set routing when routing is not equal to id
            if (fetchPhaseSqlResponse != null && fetchResIndex != null) {
                Object routing = fetchPhaseSqlResponse.getSqlResult().getData()[fetchResIndex][ROUTING_POS];
                if (routing != null && idList.get(i).equals(routing) == false) {
                    metaFields.put(RoutingFieldMapper.NAME, new DocumentField(RoutingFieldMapper.NAME, Collections.singletonList(routing)));
                }
            }

            SearchHit searchHit = new SearchHit(
                i,
                idList.get(i),
                new Text(MapperService.SINGLE_MAPPING_NAME),
                Collections.emptyMap(),
                metaFields
            );
            searchHit.setIndex(tableName);
            HavenaskFetchSubPhase.HitContent hit = new HavenaskFetchSubPhase.HitContent(searchHit, source);
            hit.getHit().score(topDocsAndMaxScore.topDocs.scoreDocs[i].score);
            if (false == sourceEnabled || (fetchSourceContext != null && false == fetchSourceContext.fetchSource())) {
                hits[i] = hit.getHit();
            } else if (processors != null && !processors.isEmpty()) {
                for (HavenaskFetchSubPhaseProcessor processor : processors) {
                    processor.process(hit);
                }
                hits[i] = hit.getHit();
            } else {
                hits[i] = hit.getHit();
                hits[i].sourceRef(new BytesArray((String) source));
            }
        }
        return new SearchHits(hits, totalHits, topDocsAndMaxScore.maxScore);
    }

    List<HavenaskFetchSubPhaseProcessor> getProcessors(String tableName, SearchSourceBuilder searchSourceBuilder) throws IOException {
        try {
            List<HavenaskFetchSubPhaseProcessor> processors = new ArrayList<>();
            for (HavenaskFetchSubPhase fsp : havenaskFetchSubPhases) {
                HavenaskFetchSubPhaseProcessor processor = fsp.getProcessor(tableName, searchSourceBuilder);
                if (processor != null) {
                    processors.add(processor);
                }
            }
            return processors;
        } catch (Exception e) {
            throw new IOException("Error building fetch sub-phases", e);
        }
    }
}

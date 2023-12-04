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

package org.havenask.engine.search.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.havenask.action.ActionListener;
import org.havenask.action.search.SearchRequest;
import org.havenask.action.search.SearchResponse;
import org.havenask.action.search.ShardSearchFailure;
import org.havenask.action.support.ActionFilters;
import org.havenask.action.support.HandledTransportAction;
import org.havenask.client.ha.SqlResponse;
import org.havenask.cluster.ClusterState;
import org.havenask.cluster.service.ClusterService;
import org.havenask.common.Strings;
import org.havenask.common.bytes.BytesArray;
import org.havenask.common.inject.Inject;
import org.havenask.common.lucene.search.TopDocsAndMaxScore;
import org.havenask.common.text.Text;
import org.havenask.engine.NativeProcessControlService;
import org.havenask.engine.index.engine.HavenaskSearchQueryPhase;
import org.havenask.engine.index.mapper.DenseVectorFieldMapper;
import org.havenask.engine.index.query.ProximaQueryBuilder;
import org.havenask.engine.rpc.QrsClient;
import org.havenask.engine.rpc.QrsSqlRequest;
import org.havenask.engine.rpc.QrsSqlResponse;
import org.havenask.engine.rpc.http.QrsHttpClient;
import org.havenask.engine.search.fetch.HavenaskFetchSourcePhase;
import org.havenask.engine.search.fetch.HavenaskFetchSubPhase;
import org.havenask.engine.search.fetch.HavenaskFetchSubPhaseProcessor;
import org.havenask.engine.search.fetch.HavenaskFetchSubPhase.HitContent;
import org.havenask.index.mapper.MapperService;
import org.havenask.index.query.MatchAllQueryBuilder;
import org.havenask.index.query.MatchQueryBuilder;
import org.havenask.index.query.QueryBuilder;
import org.havenask.index.query.TermQueryBuilder;
import org.havenask.search.SearchHit;
import org.havenask.search.SearchHits;
import org.havenask.search.aggregations.InternalAggregations;
import org.havenask.search.builder.KnnSearchBuilder;
import org.havenask.search.builder.SearchSourceBuilder;
import org.havenask.search.fetch.subphase.FetchSourceContext;
import org.havenask.search.internal.InternalSearchResponse;
import org.havenask.search.profile.SearchProfileShardResults;
import org.havenask.search.suggest.Suggest;
import org.havenask.tasks.Task;
import org.havenask.threadpool.ThreadPool;
import org.havenask.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.havenask.engine.search.rest.RestHavenaskSqlAction.SQL_DATABASE;

public class TransportHavenaskSearchAction extends HandledTransportAction<SearchRequest, SearchResponse> {
    private static final Logger logger = LogManager.getLogger(TransportHavenaskSearchAction.class);
    private static final int ID_POS = 0;
    private static final int SCORE_POS = 1;
    private static final int SOURCE_POS = 1;
    private static final Object SOURCE_NOT_FOUND = "{\n" + "\"warn\":\"source not found\"\n" + "}";
    private static final String SIMILARITY = "similarity";
    private static final String PROPERTIES_FIELD = "properties";
    private static final String VECTOR_SIMILARITY_TYPE_L2_NORM = "L2_NORM";
    private static final String VECTOR_SIMILARITY_TYPE_DOT_PRODUCT = "DOT_PRODUCT";
    private ClusterService clusterService;
    private QrsClient qrsClient;
    private final List<HavenaskFetchSubPhase> havenaskFetchSubPhases;

    @Inject
    public TransportHavenaskSearchAction(
        ClusterService clusterService,
        TransportService transportService,
        NativeProcessControlService nativeProcessControlService,
        ActionFilters actionFilters
    ) {
        super(HavenaskSearchAction.NAME, transportService, actionFilters, SearchRequest::new, ThreadPool.Names.SEARCH);
        this.clusterService = clusterService;
        this.qrsClient = new QrsHttpClient(nativeProcessControlService.getQrsHttpPort());

        // TODO 目前仅支持source过滤，未来增加更多的subPhase并考虑以plugin形式去支持
        this.havenaskFetchSubPhases = new ArrayList<>();
        this.havenaskFetchSubPhases.add(new HavenaskFetchSourcePhase());
    }

    @Override
    protected void doExecute(Task task, SearchRequest request, ActionListener<SearchResponse> listener) {
        try {
            // TODO: 目前的逻辑只有单havenask索引的查询会走到这里，后续如果有多索引的查询，这里需要做相应的修改
            if (request.indices().length != 1) {
                throw new IllegalArgumentException("illegal index count! only support search single havenask index.");
            }
            String tableName = request.indices()[0];

            ClusterState clusterState = clusterService.state();

            SqlResponse queryPhaseSqlResponse = havenaskSearchQuery(clusterState, request, tableName);
            InternalSearchResponse internalSearchResponse = havenaskSearchFetch(
                queryPhaseSqlResponse,
                tableName,
                request.source().fetchSource()
            );

            listener.onResponse(
                new SearchResponse(internalSearchResponse, "", 1, 1, 0, 0, ShardSearchFailure.EMPTY_ARRAY, SearchResponse.Clusters.EMPTY)
            );
        } catch (Exception e) {
            logger.error("Failed to execute havenask search, ", e);
            listener.onFailure(e);
        }
    }

    private SqlResponse havenaskSearchQuery(ClusterState clusterState, SearchRequest request, String tableName) throws IOException {
        String sql = transferSearchRequest2HavenaskSql(clusterState, request, tableName);
        String kvpair = "format:full_json;timeout:10000;databaseName:" + SQL_DATABASE;
        QrsSqlRequest qrsQueryPhaseSqlRequest = new QrsSqlRequest(sql, kvpair);
        QrsSqlResponse qrsQueryPhaseSqlResponse = qrsClient.executeSql(qrsQueryPhaseSqlRequest);
        if (Strings.isNullOrEmpty(qrsQueryPhaseSqlResponse.getResult())) {
            // TODO
        }
        SqlResponse queryPhaseSqlResponse = SqlResponse.parse(qrsQueryPhaseSqlResponse.getResult());
        if (logger.isDebugEnabled()) {
            logger.debug("sql: {}, sqlResponse took: {} ms", sql, queryPhaseSqlResponse.getTotalTime());
        }
        return queryPhaseSqlResponse;
    }

    private String transferSearchRequest2HavenaskSql(ClusterState clusterState, SearchRequest request, String table) throws IOException {
        SearchSourceBuilder dsl = request.source();

        StringBuilder sqlQuery = new StringBuilder();
        QueryBuilder queryBuilder = dsl.query();
        StringBuilder where = new StringBuilder();
        StringBuilder selectParams = new StringBuilder();
        StringBuilder orderBy = new StringBuilder();

        selectParams.append(" _id");

        if (!dsl.knnSearch().isEmpty()) {
            where.append(" where ");
            boolean first = true;
            for (KnnSearchBuilder knnSearchBuilder : dsl.knnSearch()) {
                if (!knnSearchBuilder.getFilterQueries().isEmpty() || knnSearchBuilder.getSimilarity() != null) {
                    throw new IOException("unsupported knn parameter: " + dsl);
                }

                String fieldName = knnSearchBuilder.getField();

                String similarity = getSimilarity(clusterState, table, fieldName);
                if (similarity == null) {
                    throw new IOException(String.format(Locale.ROOT, "field: %s is not a vector type field", fieldName));
                }

                if (false == first) {
                    where.append(" or ");
                    selectParams.append(" + ");
                }

                if (first) {
                    first = false;
                    selectParams.append(", (");
                }

                checkVectorMagnitude(similarity, knnSearchBuilder.getQueryVector());
                selectParams.append(getScoreComputeStr(fieldName, similarity));

                where.append("MATCHINDEX('").append(fieldName).append("', '");
                for (int i = 0; i < knnSearchBuilder.getQueryVector().length; i++) {
                    where.append(knnSearchBuilder.getQueryVector()[i]);
                    if (i < knnSearchBuilder.getQueryVector().length - 1) {
                        where.append(",");
                    }
                }
                where.append("&n=").append(knnSearchBuilder.k()).append("')");
            }
            selectParams.append(") as _score");
            orderBy.append(" order by _score desc");
        } else if (queryBuilder != null) {
            if (queryBuilder instanceof MatchAllQueryBuilder) {} else if (queryBuilder instanceof ProximaQueryBuilder) {
                ProximaQueryBuilder<?> proximaQueryBuilder = (ProximaQueryBuilder<?>) queryBuilder;
                String fieldName = proximaQueryBuilder.getFieldName();

                String similarity = getSimilarity(clusterState, table, fieldName);
                if (similarity == null) {
                    throw new IOException(String.format(Locale.ROOT, "field: %s is not a vector type field", fieldName));
                }

                checkVectorMagnitude(similarity, proximaQueryBuilder.getVector());

                selectParams.append(", ").append(getScoreComputeStr(fieldName, similarity)).append(" as _score");
                where.append(" where MATCHINDEX('").append(proximaQueryBuilder.getFieldName()).append("', '");
                for (int i = 0; i < proximaQueryBuilder.getVector().length; i++) {

                    where.append(proximaQueryBuilder.getVector()[i]);
                    if (i < proximaQueryBuilder.getVector().length - 1) {
                        where.append(",");
                    }
                }
                where.append("&n=").append(proximaQueryBuilder.getSize()).append("')");
                orderBy.append(" order by _score desc");
            } else if (queryBuilder instanceof TermQueryBuilder) {
                TermQueryBuilder termQueryBuilder = (TermQueryBuilder) queryBuilder;
                where.append(" where ").append(termQueryBuilder.fieldName()).append("='").append(termQueryBuilder.value()).append("'");
            } else if (queryBuilder instanceof MatchQueryBuilder) {
                MatchQueryBuilder matchQueryBuilder = (MatchQueryBuilder) queryBuilder;
                where.append(" where MATCHINDEX('")
                    .append(matchQueryBuilder.fieldName())
                    .append("', '")
                    .append(matchQueryBuilder.value())
                    .append("')");
            } else {
                throw new IOException("unsupported DSL: " + dsl);
            }
        }
        sqlQuery.append("select").append(selectParams).append(" from ").append(table);
        sqlQuery.append(where).append(orderBy);
        int size = 0;
        if (dsl.size() >= 0) {
            size += dsl.size();
            if (dsl.from() >= 0) {
                size += dsl.from();
            }
        }

        if (size > 0) {
            sqlQuery.append(" limit ").append(size);
        }
        return sqlQuery.toString();
    }

    @SuppressWarnings("unchecked")
    private String getSimilarity(ClusterState clusterState, String indexName, String fieldName) {
        // TODO: 需要考虑如何优化,
        // 1.similarity的获取方式，
        // 2.针对嵌套的properties如何查询
        Map<String, Object> indexMapping = clusterState.metadata().indices().get(indexName).mapping().getSourceAsMap();
        Object propertiesObj = indexMapping.get(PROPERTIES_FIELD);
        if (propertiesObj instanceof Map) {
            Map<String, Object> propertiesMapping = (Map<String, Object>) propertiesObj;
            Object fieldObj = propertiesMapping.get(fieldName);
            if (fieldObj instanceof Map) {
                Map<String, Object> fieldMapping = (Map<String, Object>) fieldObj;
                return (String) fieldMapping.get(SIMILARITY);
            }
        }

        return null;
    }

    private void checkVectorMagnitude(String similarity, float[] queryVector) {
        if (similarity.equals(VECTOR_SIMILARITY_TYPE_DOT_PRODUCT) && Math.abs(computeSquaredMagnitude(queryVector) - 1.0f) > 1e-4f) {
            throw new IllegalArgumentException(
                "The ["
                    + DenseVectorFieldMapper.Similarity.DOT_PRODUCT.getValue()
                    + "] "
                    + "similarity can only be used with unit-length vectors."
            );
        }
    }

    private float computeSquaredMagnitude(float[] queryVector) {
        float squaredMagnitude = 0;
        for (float v : queryVector) {
            squaredMagnitude += v * v;
        }
        return squaredMagnitude;
    }

    private String getScoreComputeStr(String fieldName, String similarity) throws IOException {
        StringBuilder scoreComputeStr = new StringBuilder();
        if (similarity != null && similarity.equals(VECTOR_SIMILARITY_TYPE_L2_NORM)) {
            // e.g. "(1/(1+vecscore('fieldName')))"
            scoreComputeStr.append("(1/(").append("1+vector_score('").append(fieldName).append("')))");
        } else if (similarity != null && similarity.equals(VECTOR_SIMILARITY_TYPE_DOT_PRODUCT)) {
            // e.g. "((1+vecscore('fieldName'))/2)"
            scoreComputeStr.append("((1+vector_score('").append(fieldName).append("'))/2)");
        } else {
            throw new IOException("unsupported similarity: " + similarity);
        }
        return scoreComputeStr.toString();
    }

    private InternalSearchResponse havenaskSearchFetch(
        SqlResponse queryPhaseSqlResponse,
        String tableName,
        FetchSourceContext fetchSourceContext
    ) throws IOException {
        List<String> idList = new ArrayList<>(queryPhaseSqlResponse.getRowCount());
        TopDocsAndMaxScore topDocsAndMaxScore = buildQuerySearchResult(queryPhaseSqlResponse, idList);
        SqlResponse fetchPhaseSqlResponse = havenaskFetchWithSql(idList, tableName, fetchSourceContext);
        return transferSqlResponse2FetchResult(tableName, idList, fetchPhaseSqlResponse, topDocsAndMaxScore, fetchSourceContext);
    }

    private TopDocsAndMaxScore buildQuerySearchResult(SqlResponse queryPhaseSqlResponse, List<String> idList) throws IOException {
        ScoreDoc[] queryScoreDocs = new ScoreDoc[queryPhaseSqlResponse.getRowCount()];
        float maxScore = 0;
        int sqlDataSize = queryPhaseSqlResponse.getRowCount() > 0 ? queryPhaseSqlResponse.getSqlResult().getData()[0].length : 0;
        if (sqlDataSize > 2) {
            throw new IOException("unknow sqlResponse:" + Arrays.deepToString(queryPhaseSqlResponse.getSqlResult().getData()));
        }
        for (int i = 0; i < queryPhaseSqlResponse.getRowCount(); i++) {
            float defaultScore = queryPhaseSqlResponse.getRowCount() - i;
            float curScore = sqlDataSize == 1
                ? defaultScore
                : ((Double) queryPhaseSqlResponse.getSqlResult().getData()[i][SCORE_POS]).floatValue();

            queryScoreDocs[i] = new ScoreDoc(i, curScore);
            maxScore = Math.max(maxScore, curScore);
            idList.add(String.valueOf(queryPhaseSqlResponse.getSqlResult().getData()[i][ID_POS]));
        }
        TopDocs topDocs = new TopDocs(
            new TotalHits(queryPhaseSqlResponse.getRowCount(), TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO),
            queryScoreDocs
        );
        return new TopDocsAndMaxScore(topDocs, maxScore);
    }

    private SqlResponse havenaskFetchWithSql(List<String> idList, String tableName, FetchSourceContext fetchSourceContext)
        throws IOException {
        QrsSqlRequest qrsFetchPhaseSqlRequest = getQrsFetchPhaseSqlRequest(idList, tableName, fetchSourceContext);
        QrsSqlResponse qrsFetchPhaseSqlResponse = qrsClient.executeSql(qrsFetchPhaseSqlRequest);
        SqlResponse fetchPhaseSqlResponse = SqlResponse.parse(qrsFetchPhaseSqlResponse.getResult());
        if (logger.isDebugEnabled()) {
            logger.debug("fetch ids length: {}, havenask sqlResponse took: {} ms", idList.size(), fetchPhaseSqlResponse.getTotalTime());
        }
        return fetchPhaseSqlResponse;
    }

    private static QrsSqlRequest getQrsFetchPhaseSqlRequest(List<String> idList, String tableName, FetchSourceContext fetchSourceContext) {
        StringBuilder sqlQuery = new StringBuilder();
        sqlQuery.append("select _id");
        if (fetchSourceContext == null || true == fetchSourceContext.fetchSource()) {
            sqlQuery.append(", _source");
        }
        sqlQuery.append(" from ").append(tableName).append("_summary_");
        sqlQuery.append(" where _id in(");
        for (int i = 0; i < idList.size(); i++) {
            sqlQuery.append("'").append(idList.get(i)).append("'");
            if (i < idList.size() - 1) {
                sqlQuery.append(",");
            }
        }
        sqlQuery.append(")");
        sqlQuery.append(" limit ").append(idList.size());
        String kvpair = "format:full_json;timeout:10000;databaseName:" + SQL_DATABASE;
        return new QrsSqlRequest(sqlQuery.toString(), kvpair);
    }

    private InternalSearchResponse transferSqlResponse2FetchResult(
        String tableName,
        List<String> idList,
        SqlResponse fetchPhaseSqlResponse,
        TopDocsAndMaxScore topDocsAndMaxScore,
        FetchSourceContext fetchSourceContext
    ) throws IOException {
        int loadSize = idList.size();
        TotalHits totalHits = topDocsAndMaxScore.topDocs.totalHits;
        SearchHit[] hits = new SearchHit[loadSize];
        List<HavenaskFetchSubPhaseProcessor> processors = getProcessors(tableName, fetchSourceContext);

        // 记录fetch结果的_id和index的映射关系, query阶段查到的idList是根据_score值排序好的，但fetch结果非有序
        Map<String, Integer> fetchResIdListMap = new HashMap<>();
        for (int i = 0; i < fetchPhaseSqlResponse.getRowCount(); i++) {
            fetchResIdListMap.put((String) fetchPhaseSqlResponse.getSqlResult().getData()[i][ID_POS], i);
        }

        for (int i = 0; i < loadSize; i++) {
            // TODO: add _routing
            SearchHit searchHit = new SearchHit(
                i,
                idList.get(i),
                new Text(MapperService.SINGLE_MAPPING_NAME),
                Collections.emptyMap(),
                Collections.emptyMap()
            );

            // 根据idList的顺序从fetch结果获取相对应的_source, 如果数据丢失则返回_source not found
            Integer fetchResIndex = fetchResIdListMap.get(idList.get(i));
            Object source = null;
            if (fetchSourceContext == null || true == fetchSourceContext.fetchSource()) {
                source = fetchResIndex != null
                    ? fetchPhaseSqlResponse.getSqlResult().getData()[fetchResIndex][SOURCE_POS]
                    : SOURCE_NOT_FOUND;
            }
            HitContent hit = new HitContent(searchHit, source);
            hit.getHit().score(topDocsAndMaxScore.topDocs.scoreDocs[i].score);
            if (fetchSourceContext != null && false == fetchSourceContext.fetchSource()) {
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
        return new InternalSearchResponse(
            new SearchHits(hits, totalHits, topDocsAndMaxScore.maxScore),
            InternalAggregations.EMPTY,
            new Suggest(Collections.emptyList()),
            new SearchProfileShardResults(Collections.emptyMap()),
            false,
            false,
            1
        );
    }

    List<HavenaskFetchSubPhaseProcessor> getProcessors(String tableName, FetchSourceContext fetchSourceContext) throws IOException {
        try {
            List<HavenaskFetchSubPhaseProcessor> processors = new ArrayList<>();
            for (HavenaskFetchSubPhase fsp : havenaskFetchSubPhases) {
                HavenaskFetchSubPhaseProcessor processor = fsp.getProcessor(tableName, fetchSourceContext);
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

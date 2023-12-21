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
import org.havenask.action.search.SearchRequest;
import org.havenask.client.ha.SqlResponse;
import org.havenask.common.Strings;
import org.havenask.engine.index.mapper.DenseVectorFieldMapper;
import org.havenask.engine.index.query.ProximaQueryBuilder;
import org.havenask.engine.rpc.QrsClient;
import org.havenask.engine.rpc.QrsSqlRequest;
import org.havenask.engine.rpc.QrsSqlResponse;
import org.havenask.index.query.MatchAllQueryBuilder;
import org.havenask.index.query.MatchQueryBuilder;
import org.havenask.index.query.QueryBuilder;
import org.havenask.index.query.TermQueryBuilder;
import org.havenask.search.builder.KnnSearchBuilder;
import org.havenask.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;

import static org.havenask.engine.search.rest.RestHavenaskSqlAction.SQL_DATABASE;

public class HavenaskSearchQueryProcessor {
    private static final Logger logger = LogManager.getLogger(HavenaskSearchQueryProcessor.class);
    private static final String SIMILARITY = "similarity";
    private static final String PROPERTIES_FIELD = "properties";
    private static final String VECTOR_SIMILARITY_TYPE_L2_NORM = "L2_NORM";
    private static final String VECTOR_SIMILARITY_TYPE_DOT_PRODUCT = "DOT_PRODUCT";
    private static final int DEFAULT_SEARCH_SIZE = 10;
    QrsClient qrsClient;

    public HavenaskSearchQueryProcessor(QrsClient qrsClient) {
        this.qrsClient = qrsClient;
    }

    public SqlResponse executeQuery(SearchRequest request, String tableName, Map<String, Object> indexMapping) throws IOException {
        String sql = transferSearchRequest2HavenaskSql(tableName, request.source(), indexMapping);
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

    public String transferSearchRequest2HavenaskSql(String table, SearchSourceBuilder dsl, Map<String, Object> indexMapping)
        throws IOException {
        if (dsl == null) {
            throw new IllegalArgumentException("request source can not be null!");
        }
        StringBuilder sqlQuery = new StringBuilder();
        QueryBuilder queryBuilder = dsl.query();
        StringBuilder where = new StringBuilder();
        StringBuilder selectParams = new StringBuilder();
        StringBuilder orderBy = new StringBuilder();

        int size = dsl.size() >= 0 ? dsl.size() : DEFAULT_SEARCH_SIZE;
        int from = dsl.from() >= 0 ? dsl.from() : 0;

        selectParams.append(" _id");

        if (!dsl.knnSearch().isEmpty()) {
            where.append(" where ");
            boolean first = true;
            for (KnnSearchBuilder knnSearchBuilder : dsl.knnSearch()) {
                if (!knnSearchBuilder.getFilterQueries().isEmpty() || knnSearchBuilder.getSimilarity() != null) {
                    throw new IOException("unsupported knn parameter: " + dsl);
                }

                String fieldName = knnSearchBuilder.getField();

                if (indexMapping == null) {
                    throw new IllegalArgumentException(
                        String.format(Locale.ROOT, "index mapping is null, field: %s is not a vector type field", fieldName)
                    );
                }
                String similarity = getSimilarity(fieldName, indexMapping);
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

                if (indexMapping == null) {
                    throw new IllegalArgumentException(
                        String.format(Locale.ROOT, "index mapping is null, field: %s is not a vector type field", fieldName)
                    );
                }
                String similarity = getSimilarity(fieldName, indexMapping);
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
        sqlQuery.append("select").append(selectParams).append(" from ").append('`').append(table).append('`');
        sqlQuery.append(where).append(orderBy);

        sqlQuery.append(" limit ").append(size).append(" offset ").append(from);

        return sqlQuery.toString();
    }

    @SuppressWarnings("unchecked")
    private String getSimilarity(String fieldName, Map<String, Object> indexMapping) {
        // TODO: 需要考虑如何优化,
        // 1.similarity的获取方式，
        // 2.针对嵌套的properties如何查询
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
}

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

import static org.havenask.engine.search.rest.RestHavenaskSqlAction.SQL_DATABASE;

import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.havenask.action.search.SearchRequest;
import org.havenask.client.ha.SqlResponse;
import org.havenask.common.Strings;
import org.havenask.engine.index.config.Schema;
import org.havenask.engine.index.mapper.DenseVectorFieldMapper;
import org.havenask.engine.index.query.ProximaQueryBuilder;
import org.havenask.engine.rpc.QrsClient;
import org.havenask.engine.rpc.QrsSqlRequest;
import org.havenask.engine.rpc.QrsSqlResponse;
import org.havenask.index.query.BoolQueryBuilder;
import org.havenask.index.query.MatchAllQueryBuilder;
import org.havenask.index.query.MatchPhraseQueryBuilder;
import org.havenask.index.query.MatchQueryBuilder;
import org.havenask.index.query.QueryBuilder;
import org.havenask.index.query.RangeQueryBuilder;
import org.havenask.index.query.TermQueryBuilder;
import org.havenask.search.builder.KnnSearchBuilder;
import org.havenask.search.builder.SearchSourceBuilder;
import org.havenask.search.sort.FieldSortBuilder;
import org.havenask.search.sort.SortBuilder;

public class HavenaskSearchQueryProcessor {
    private static final Logger logger = LogManager.getLogger(HavenaskSearchQueryProcessor.class);
    private static final String VECTOR_TYPE = "vector";
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
        if (logger.isTraceEnabled()) {
            logger.trace("dsl: {}, sql: {}", request.source(), sql);
        }

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

                String fieldName = Schema.encodeFieldWithDot(knnSearchBuilder.getField());

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
                String fieldName = Schema.encodeFieldWithDot(proximaQueryBuilder.getFieldName());

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
                where.append(" where MATCHINDEX('").append(fieldName).append("', '");
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
                where.append(" where ")
                    .append(Schema.encodeFieldWithDot(termQueryBuilder.fieldName()))
                    .append("='")
                    .append(termQueryBuilder.value())
                    .append("'");
            } else if (queryBuilder instanceof MatchQueryBuilder) {
                MatchQueryBuilder matchQueryBuilder = (MatchQueryBuilder) queryBuilder;
                where.append(" where MATCHINDEX('")
                    .append(Schema.encodeFieldWithDot(matchQueryBuilder.fieldName()))
                    .append("', '")
                    .append(matchQueryBuilder.value())
                    .append("', 'default_op:OR')");

                selectParams.append(", bm25_score() as _score");
                orderBy.append(" order by _score desc");
            } else if (queryBuilder instanceof MatchPhraseQueryBuilder) {
                MatchPhraseQueryBuilder matchQueryBuilder = (MatchPhraseQueryBuilder) queryBuilder;
                where.append(" where ")
                        .append("QUERY('', '")
                        .append(Schema.encodeFieldWithDot(matchQueryBuilder.fieldName()))
                        .append(":")
                        .append("\"")
                        .append(matchQueryBuilder.value())
                        .append("\"")
                        .append("')");
                selectParams.append(", bm25_score() as _score");
                orderBy.append(" order by _score desc");
            } else if (queryBuilder instanceof RangeQueryBuilder) {
                RangeQueryBuilder rangeQueryBuilder = (RangeQueryBuilder) queryBuilder;
                where.append(" where ")
                    .append("QUERY('', '")
                    .append(Schema.encodeFieldWithDot(rangeQueryBuilder.fieldName()))
                    .append(":")
                    .append(rangeQueryBuilder.includeLower() ? "[" : "{")
                    .append(rangeQueryBuilder.from())
                    .append(",")
                    .append(rangeQueryBuilder.to())
                    .append(rangeQueryBuilder.includeUpper() ? "]" : "}")
                    .append("')");
            } else if (queryBuilder instanceof BoolQueryBuilder) {
                BoolQueryBuilder boolQueryBuilder = (BoolQueryBuilder) queryBuilder;
                where.append(" where ");
                boolean first = true;
                for (QueryBuilder subQueryBuilder : boolQueryBuilder.must()) {
                    if (false == first) {
                        where.append(" and ");
                    }
                    if (first) {
                        first = false;
                    }
                    if (subQueryBuilder instanceof TermQueryBuilder) {
                        TermQueryBuilder termQueryBuilder = (TermQueryBuilder) subQueryBuilder;
                        where.append(Schema.encodeFieldWithDot(termQueryBuilder.fieldName()))
                            .append("='")
                            .append(termQueryBuilder.value())
                            .append("'");
                    } else if (subQueryBuilder instanceof MatchQueryBuilder) {
                        MatchQueryBuilder matchQueryBuilder = (MatchQueryBuilder) subQueryBuilder;
                        where.append("MATCHINDEX('")
                            .append(Schema.encodeFieldWithDot(matchQueryBuilder.fieldName()))
                            .append("', '")
                            .append(matchQueryBuilder.value())
                            .append("', 'default_op:OR')");

                        selectParams.append(", bm25_score() as _score");
                        orderBy.append(" order by _score desc");
                    } else if (subQueryBuilder instanceof RangeQueryBuilder) {
                        RangeQueryBuilder rangeQueryBuilder = (RangeQueryBuilder) subQueryBuilder;
                        where.append("QUERY('', '")
                            .append(Schema.encodeFieldWithDot(rangeQueryBuilder.fieldName()))
                            .append(":")
                            .append(rangeQueryBuilder.includeLower() ? "[" : "{")
                            .append(rangeQueryBuilder.from())
                            .append(",")
                            .append(rangeQueryBuilder.to())
                            .append(rangeQueryBuilder.includeUpper() ? "]" : "}")
                            .append("')");
                    } else {
                        throw new IOException("unsupported DSL(unsupported bool filter): " + dsl);
                    }
                }
            } else {
                throw new IOException("unsupported DSL: " + dsl);
            }
        }

        StringBuilder sortBuilder = new StringBuilder();
        if (dsl.sorts() != null && dsl.sorts().size() > 0) {
            sortBuilder.append(" order by ");
            for (SortBuilder<?> sortField : dsl.sorts()) {
                if (sortField instanceof FieldSortBuilder == false) {
                    throw new IOException("unsupported DSL(unsupported sort): " + dsl);
                }

                FieldSortBuilder fieldSortBuilder = (FieldSortBuilder) sortField;

                sortBuilder.append("`").append(Schema.encodeFieldWithDot(fieldSortBuilder.getFieldName())).append("` ").append(sortField.order());

                if (sortBuilder.length() > 0) {
                    sortBuilder.append(", ");
                }
            }

            sortBuilder.delete(sortBuilder.length() - 2, sortBuilder.length());
        }

        if (orderBy.length() == 0) {
            orderBy = sortBuilder;
        }

        sqlQuery.append("select")
            .append(selectParams)
            .append(" from ")
            .append('`')
            .append(table)
            .append('`')
            .append(where)
            .append(orderBy)
            .append(" limit ")
            .append(size)
            .append(" offset ")
            .append(from);

        return sqlQuery.toString();
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

    @SuppressWarnings("unchecked")
    private String getSimilarity(String fieldName, Map<String, Object> indexMapping) {
        Map<String, Object> flattenFields = flattenFields(Map.of(PROPERTIES_FIELD, indexMapping), null);
        String flattenFieldName = "properties_" + fieldName;

        if (flattenFields.get(flattenFieldName) != null && flattenFields.get(flattenFieldName) instanceof Map) {
            Map<String, Object> properties = (Map<String, Object>) flattenFields.get(flattenFieldName);
            if (properties.get("type").equals(VECTOR_TYPE)) {
                return properties.get(SIMILARITY) != null
                    ? ((String) properties.get(SIMILARITY)).toUpperCase(Locale.ROOT)
                    : VECTOR_SIMILARITY_TYPE_L2_NORM;
            }
        }

        return null;
    }

    private static Map<String, Object> flattenFields(Map<String, Object> map, String parentPath) {
        Map<String, Object> flatMap = new HashMap<>();
        String prefix = parentPath != null ? parentPath + "_" : "";
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            if (entry.getValue() instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> value = (Map<String, Object>) entry.getValue();
                String type = (String) value.get("type");
                if (type == null || type.equals("object")) {
                    if (value.containsKey("properties")) {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> properties = (Map<String, Object>) value.get("properties");
                        flatMap.putAll(flattenFields(properties, prefix + entry.getKey()));
                    }
                } else {
                    flatMap.put(prefix + entry.getKey(), entry.getValue());
                }
            }
        }
        return flatMap;
    }
}

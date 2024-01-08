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

package org.havenask.engine.index.engine;

import java.io.IOException;
import java.util.Locale;

import org.havenask.engine.index.mapper.DenseVectorFieldMapper;
import org.havenask.engine.index.query.ProximaQueryBuilder;
import org.havenask.index.mapper.MapperService;
import org.havenask.index.query.MatchAllQueryBuilder;
import org.havenask.index.query.MatchQueryBuilder;
import org.havenask.index.query.QueryBuilder;
import org.havenask.index.query.TermQueryBuilder;
import org.havenask.search.builder.KnnSearchBuilder;
import org.havenask.search.builder.SearchSourceBuilder;

public class QueryTransformer {
    public static String toSql(String table, SearchSourceBuilder dsl, MapperService mapperService, int shardId) throws IOException {
        StringBuilder sqlQuery = new StringBuilder();
        QueryBuilder queryBuilder = dsl.query();
        StringBuilder where = new StringBuilder();
        StringBuilder shardParams = new StringBuilder();
        StringBuilder selectParams = new StringBuilder();
        StringBuilder orderBy = new StringBuilder();

        shardParams.append(String.format(Locale.ROOT, " /*+ SCAN_ATTR(partitionIds='%s')*/", shardId));
        selectParams.append(" _id");

        if (dsl.knnSearch().size() > 0) {
            where.append(" where ");
            boolean first = true;
            for (KnnSearchBuilder knnSearchBuilder : dsl.knnSearch()) {
                if (knnSearchBuilder.getFilterQueries().size() > 0 || knnSearchBuilder.getSimilarity() != null) {
                    throw new IOException("unsupported knn parameter: " + dsl);
                }

                String fieldName = knnSearchBuilder.getField();

                DenseVectorFieldMapper denseVectorFieldMapper = ((DenseVectorFieldMapper) mapperService.documentMapper()
                    .mappers()
                    .getMapper(fieldName));
                if (denseVectorFieldMapper == null) {
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

                DenseVectorFieldMapper.Similarity similarity = denseVectorFieldMapper.getSimilarity();
                checkVectorMagnitude(similarity, computeSquaredMagnitude(knnSearchBuilder.getQueryVector()));
                selectParams.append(getScoreComputeStr(fieldName, similarity));

                where.append("MATCHINDEX('" + fieldName + "', '");
                for (int i = 0; i < knnSearchBuilder.getQueryVector().length; i++) {
                    where.append(knnSearchBuilder.getQueryVector()[i]);
                    if (i < knnSearchBuilder.getQueryVector().length - 1) {
                        where.append(",");
                    }
                }
                where.append("&n=" + knnSearchBuilder.k() + "')");
            }
            selectParams.append(") as _score");
            orderBy.append(" order by _score desc");
        } else if (queryBuilder != null) {
            if (queryBuilder instanceof MatchAllQueryBuilder) {} else if (queryBuilder instanceof ProximaQueryBuilder) {
                ProximaQueryBuilder<?> proximaQueryBuilder = (ProximaQueryBuilder<?>) queryBuilder;
                String fieldName = proximaQueryBuilder.getFieldName();

                DenseVectorFieldMapper denseVectorFieldMapper = ((DenseVectorFieldMapper) mapperService.documentMapper()
                    .mappers()
                    .getMapper(fieldName));
                if (denseVectorFieldMapper == null) {
                    throw new IOException(String.format(Locale.ROOT, "field: %s is not a vector type field", fieldName));
                }

                DenseVectorFieldMapper.Similarity similarity = denseVectorFieldMapper.getSimilarity();
                checkVectorMagnitude(similarity, computeSquaredMagnitude(proximaQueryBuilder.getVector()));

                selectParams.append(", ").append(getScoreComputeStr(fieldName, similarity)).append(" as _score");
                where.append(" where MATCHINDEX('" + proximaQueryBuilder.getFieldName() + "', '");
                for (int i = 0; i < proximaQueryBuilder.getVector().length; i++) {

                    where.append(proximaQueryBuilder.getVector()[i]);
                    if (i < proximaQueryBuilder.getVector().length - 1) {
                        where.append(",");
                    }
                }
                where.append("&n=" + proximaQueryBuilder.getSize() + "')");
                orderBy.append(" order by _score desc");
            } else if (queryBuilder instanceof TermQueryBuilder) {
                TermQueryBuilder termQueryBuilder = (TermQueryBuilder) queryBuilder;
                where.append(" where " + termQueryBuilder.fieldName() + "='" + termQueryBuilder.value() + "'");
            } else if (queryBuilder instanceof MatchQueryBuilder) {
                MatchQueryBuilder matchQueryBuilder = (MatchQueryBuilder) queryBuilder;
                where.append(" where MATCHINDEX('" + matchQueryBuilder.fieldName() + "', '" + matchQueryBuilder.value() + "')");
            } else {
                throw new IOException("unsupported DSL: " + dsl);
            }
        }
        sqlQuery.append("select").append(shardParams).append(selectParams).append(" from ").append(table);
        sqlQuery.append(where).append(orderBy);
        int size = 0;
        if (dsl.size() >= 0) {
            size += dsl.size();
            if (dsl.from() >= 0) {
                size += dsl.from();
            }
        }

        if (size > 0) {
            sqlQuery.append(" limit " + size);
        }
        return sqlQuery.toString();
    }

    private static String getScoreComputeStr(String fieldName, DenseVectorFieldMapper.Similarity similarity) throws IOException {
        StringBuilder scoreComputeStr = new StringBuilder();
        if (similarity != null && similarity == DenseVectorFieldMapper.Similarity.L2_NORM) {
            // e.g. "(1/(1+vecscore('fieldName')))"
            scoreComputeStr.append("(1/(").append("1+vector_score('").append(fieldName).append("')))");
        } else if (similarity != null && similarity == DenseVectorFieldMapper.Similarity.DOT_PRODUCT) {
            // e.g. "((1+vecscore('fieldName'))/2)"
            scoreComputeStr.append("((1+vector_score('").append(fieldName).append("'))/2)");
        } else {
            throw new IOException("unsupported similarity: " + similarity);
        }
        return scoreComputeStr.toString();
    }

    private static float computeSquaredMagnitude(float[] queryVector) {
        float squaredMagnitude = 0;
        for (int i = 0; i < queryVector.length; i++) {
            squaredMagnitude += queryVector[i] * queryVector[i];
        }
        return squaredMagnitude;
    }

    private static void checkVectorMagnitude(DenseVectorFieldMapper.Similarity similarity, float squaredMagnitude) {
        if (similarity == DenseVectorFieldMapper.Similarity.DOT_PRODUCT && Math.abs(squaredMagnitude - 1.0f) > 1e-4f) {
            throw new IllegalArgumentException(
                "The ["
                    + DenseVectorFieldMapper.Similarity.DOT_PRODUCT.getValue()
                    + "] "
                    + "similarity can only be used with unit-length vectors."
            );
        }
    }
}

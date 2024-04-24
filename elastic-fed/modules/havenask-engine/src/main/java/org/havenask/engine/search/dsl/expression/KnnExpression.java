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

package org.havenask.engine.search.dsl.expression;

import org.havenask.engine.index.config.Schema;
import org.havenask.engine.index.mapper.DenseVectorFieldMapper;
import org.havenask.engine.search.dsl.expression.query.BoolExpression;
import org.havenask.search.builder.KnnSearchBuilder;

import java.util.List;
import java.util.Locale;
import java.util.Map;

public class KnnExpression extends Expression {
    private static final String VECTOR_TYPE = "vector";
    private static final String SIMILARITY = "similarity";

    private static final String VECTOR_SIMILARITY_TYPE_L2_NORM = "L2_NORM";
    private static final String VECTOR_SIMILARITY_TYPE_DOT_PRODUCT = "DOT_PRODUCT";

    final String field;
    final float[] queryVector;
    final int k;
    final BoolExpression filterQueries;
    final String similarity;

    public KnnExpression(KnnSearchBuilder knnSearchBuilder, BoolExpression filterQueries, Map<String, Object> flattenMappings) {
        this.field = Schema.encodeFieldWithDot(knnSearchBuilder.getField());
        this.queryVector = knnSearchBuilder.getQueryVector();
        this.k = knnSearchBuilder.k();
        this.filterQueries = filterQueries;

        if (knnSearchBuilder.getSimilarity() != null) {
            throw new IllegalArgumentException("unsupported knn similarity parameter");
        }

        similarity = getSimilarity(field, flattenMappings);
        if (similarity == null) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "field: %s is not a vector type field", field));
        }
        checkVectorMagnitude(similarity, queryVector);
    }

    public String getSortField() {
        return "(" + getScoreComputeStr(field, similarity) + ")";
    }

    public List<String> getFilterFields() {
        return filterQueries.getFields();
    }

    @Override
    public String translate() {
        StringBuilder where = new StringBuilder();
        where.append("MATCHINDEX('").append(field).append("', '");
        for (int i = 0; i < queryVector.length; i++) {
            where.append(queryVector[i]);
            if (i < queryVector.length - 1) {
                where.append(",");
            }
        }
        where.append("&n=").append(k).append("')");
        String filterStr = filterQueries != null ? filterQueries.translate() : "";
        if (filterStr.length() > 0) {
            where.append(" AND ").append(filterStr);
        }

        return where.toString();
    }

    private String getScoreComputeStr(String fieldName, String similarity) throws IllegalArgumentException {
        StringBuilder scoreComputeStr = new StringBuilder();
        if (similarity != null && similarity.equalsIgnoreCase(DenseVectorFieldMapper.Similarity.L2_NORM.toString())) {
            // e.g. "(1/(1+vecscore('fieldName')))"
            scoreComputeStr.append("(1/(").append("1+vector_score('").append(fieldName).append("')))");
        } else if (similarity != null && similarity.equals(DenseVectorFieldMapper.Similarity.DOT_PRODUCT.toString())) {
            // e.g. "((1+vecscore('fieldName'))/2)"
            scoreComputeStr.append("((1+vector_score('").append(fieldName).append("'))/2)");
        } else {
            throw new IllegalArgumentException("unsupported similarity: " + similarity);
        }
        return scoreComputeStr.toString();
    }

    @SuppressWarnings("unchecked")
    private String getSimilarity(String fieldName, Map<String, Object> flattenFields) {
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

}

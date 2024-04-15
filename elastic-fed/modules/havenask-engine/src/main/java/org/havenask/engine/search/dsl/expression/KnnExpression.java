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

import java.io.IOException;


public class KnnExpression extends Expression {
    final String field;
    final float[] queryVector;
    final int k;
    final BoolExpression filterQueries;

    public KnnExpression(KnnSearchBuilder knnSearchBuilder, BoolExpression filterQueries) {
        this.field = Schema.encodeFieldWithDot(knnSearchBuilder.getField());
        this.queryVector = knnSearchBuilder.getQueryVector();
        this.k = knnSearchBuilder.k();
        this.filterQueries = filterQueries;
    }

    public String getSortField() throws IOException {
        return "(" + getScoreComputeStr(field, null) + ") AS _knn_score";
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
        return where.toString();
    }

    private String getScoreComputeStr(String fieldName, String similarity) throws IOException {
        StringBuilder scoreComputeStr = new StringBuilder();
        if (similarity != null && similarity.equalsIgnoreCase(DenseVectorFieldMapper.Similarity.L2_NORM.toString())) {
            // e.g. "(1/(1+vecscore('fieldName')))"
            scoreComputeStr.append("(1/(").append("1+vector_score('").append(fieldName).append("')))");
        } else if (similarity != null && similarity.equals(DenseVectorFieldMapper.Similarity.DOT_PRODUCT.toString())) {
            // e.g. "((1+vecscore('fieldName'))/2)"
            scoreComputeStr.append("((1+vector_score('").append(fieldName).append("'))/2)");
        } else {
            throw new IOException("unsupported similarity: " + similarity);
        }
        return scoreComputeStr.toString();
    }
}

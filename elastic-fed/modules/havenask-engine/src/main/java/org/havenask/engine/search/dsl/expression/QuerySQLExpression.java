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

import org.havenask.index.mapper.IdFieldMapper;

import java.util.ArrayList;
import java.util.List;

public class QuerySQLExpression extends Expression {
    public final String from;
    public final WhereExpression where;
    public final OrderByExpression orderBy;
    public final List<KnnExpression> knnExpressions;
    public final int limit;
    public final int offset;

    public QuerySQLExpression(
        String from,
        WhereExpression where,
        List<KnnExpression> knnExpressions,
        OrderByExpression orderBy,
        int limit,
        int offset
    ) {
        this.from = from;
        this.where = where;
        this.knnExpressions = knnExpressions;
        this.orderBy = orderBy;
        this.limit = limit;
        this.offset = offset;
    }

    @Override
    public String translate() {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ");

        if (false == knnExpressions.isEmpty()) {
            StringBuilder knnSelect = new StringBuilder("");
            for (KnnExpression knnExpression : knnExpressions) {
                knnExpression.getFilterFields().forEach(field -> { knnSelect.append(field).append(", "); });
            }
            if (knnSelect.length() > 0) {
                knnSelect.delete(knnSelect.length() - 2, knnSelect.length());
                sb.append("/*+ SCAN_ATTR(forbidIndex='");
                sb.append(knnSelect);
                sb.append("')*/ ");
            }
        }

        List<String> fields = new ArrayList<>();
        fields.add("`" + IdFieldMapper.NAME + "`");
        String orderByStr = orderBy.translate();
        if (orderByStr.isEmpty() && false == knnExpressions.isEmpty()) {
            StringBuilder knnScore = new StringBuilder("");
            if (knnExpressions.size() > 1) {
                knnScore.append("(");
            }
            knnExpressions.forEach(knnExpression -> {
                knnScore.append(knnExpression.getSortField());
                knnScore.append(" + ");
            });
            knnScore.delete(knnScore.length() - 3, knnScore.length());
            if (knnExpressions.size() > 1) {
                knnScore.append(")");
            }
            knnScore.append(" AS _knn_score");
            fields.add(knnScore.toString());
        }
        if (orderBy.hasScoreSort()) {
            fields.add("bm25_score() AS _score");
        }

        for (String field : fields) {
            sb.append(field).append(", ");
        }
        sb.delete(sb.length() - 2, sb.length());
        sb.append(" FROM `").append(from).append("` ");
        if (where != null) {
            sb.append(where.translate()).append(" ");
        }

        if (false == knnExpressions.isEmpty()) {
            StringBuilder knnWhere = new StringBuilder("");
            for (KnnExpression knnExpression : knnExpressions) {
                knnWhere.append("(").append(knnExpression.translate()).append(") or ");
            }
            knnWhere.delete(knnWhere.length() - 4, knnWhere.length());
            if (where != null && false == where.translate().isEmpty()) {
                if (knnExpressions.size() > 1) {
                    sb.append("AND (").append(knnWhere).append(") ");
                } else {
                    sb.append("AND ").append(knnWhere).append(" ");
                }
            } else {
                sb.append("WHERE ").append(knnWhere).append(" ");
            }
        }

        if (false == orderByStr.isEmpty()) {
            sb.append(orderByStr).append(" ");
        } else if (false == knnExpressions.isEmpty()) {
            sb.append("ORDER BY _knn_score DESC ");
        }
        if (limit > 0) {
            sb.append("LIMIT ").append(limit).append(" ");
        }
        if (offset > 0) {
            sb.append("OFFSET ").append(offset).append(" ");
        }
        return sb.toString();
    }

}

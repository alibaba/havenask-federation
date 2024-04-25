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

import org.havenask.engine.search.internal.HavenaskScroll;
import org.havenask.index.mapper.IdFieldMapper;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class QuerySQLExpression extends Expression {
    public final String from;
    public final WhereExpression where;
    public final OrderByExpression orderBy;
    public final SliceExpression slice;
    public final List<KnnExpression> knnExpressions;
    public final int limit;
    public final int offset;
    public HavenaskScroll havenaskScroll;

    public QuerySQLExpression(
        String from,
        WhereExpression where,
        List<KnnExpression> knnExpressions,
        OrderByExpression orderBy,
        SliceExpression slice,
        int limit,
        int offset,
        HavenaskScroll havenaskScroll
    ) {
        this.from = from;
        this.where = where;
        this.knnExpressions = knnExpressions;
        this.orderBy = orderBy;
        this.slice = slice;
        this.limit = limit;
        this.offset = offset;
        this.havenaskScroll = havenaskScroll;
    }

    @Override
    public String translate() {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ");

        // translate hints
        String knnSelect = generateKnnSelect(knnExpressions);
        if (Objects.nonNull(slice.getSlice()) || Objects.nonNull(knnSelect)) {
            sb.append("/*+ SCAN_ATTR(");

            // slice hint
            if (Objects.nonNull(slice.getSlice())) {
                sb.append(slice.translate());
                sb.append(", ");
            }

            // knn filter hint
            if (Objects.nonNull(knnSelect)) {
                sb.append("forbidIndex='");
                sb.append(knnSelect);
                sb.append("'");
                sb.append(", ");
            }

            sb.delete(sb.length() - 2, sb.length());
            sb.append(")*/ ");
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

        // translate where
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
                if (Objects.nonNull(havenaskScroll) && Objects.nonNull(havenaskScroll.getLastEmittedDocId())) {
                    sb.append("AND `_id` > '").append(havenaskScroll.getLastEmittedDocId()).append("' ");
                }
            } else {
                sb.append("WHERE ").append(knnWhere).append(" ");
            }
        }

        if (Objects.nonNull(havenaskScroll) && Objects.nonNull(havenaskScroll.getLastEmittedDocId())) {
            if ((Objects.nonNull(where) && !where.translate().isEmpty()) || !knnExpressions.isEmpty()) {
                sb.append("AND `_id` > '").append(havenaskScroll.getLastEmittedDocId()).append("' ");
            } else {
                sb.append("WHERE `_id` > '").append(havenaskScroll.getLastEmittedDocId()).append("' ");
            }
        }

        // translate order by
        if (Objects.nonNull(havenaskScroll)) {
            if (false == knnExpressions.isEmpty()) {
                throw new IllegalArgumentException("havenask scroll is not supported when sort is specified");
            } else if (false == orderByStr.isEmpty()) {
                if (!orderByStr.contains(HavenaskScroll.SCROLL_ORDER_BY)) {
                    throw new IllegalArgumentException("havenask scroll is not supported when sort is specified");
                }
            } else {
                sb.append(HavenaskScroll.SCROLL_ORDER_BY).append(" ");
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

    public String generateKnnSelect(List<KnnExpression> knnExpressions) {
        if (knnExpressions.isEmpty()) {
            return null;
        }
        StringBuilder knnSelect = new StringBuilder("");
        for (KnnExpression knnExpression : knnExpressions) {
            knnExpression.getFilterFields().forEach(field -> { knnSelect.append(field).append(", "); });
        }
        if (knnSelect.length() > 0) {
            knnSelect.delete(knnSelect.length() - 2, knnSelect.length());
            return knnSelect.toString();
        } else {
            return null;
        }
    }
}

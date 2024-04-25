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

import org.havenask.search.sort.FieldSortBuilder;
import org.havenask.search.sort.ScoreSortBuilder;
import org.havenask.search.sort.SortBuilder;

import java.util.List;
import java.util.Locale;

public class OrderByExpression extends Expression {
    private final List<SortBuilder<?>> sorts;
    private boolean hasScoreSort = false;
    public static final String LUCENE_DOC_FIELD_NAME = "_doc";

    public OrderByExpression(List<SortBuilder<?>> sorts) {
        this.sorts = sorts;
    }

    public boolean hasScoreSort() {
        return hasScoreSort;
    }

    @Override
    public String translate() {
        if (sorts == null || sorts.isEmpty()) {
            return "";
        } else {
            StringBuilder sb = new StringBuilder();
            sb.append("ORDER BY ");
            for (SortBuilder<?> sort : sorts) {
                if (sort instanceof FieldSortBuilder) {
                    FieldSortBuilder fieldSort = (FieldSortBuilder) sort;

                    String fieldName = fieldSort.getFieldName();
                    if (fieldName.equals(LUCENE_DOC_FIELD_NAME)) {
                        fieldName = "_id";
                    }

                    sb.append("`")
                        .append(fieldName)
                        .append("` ")
                        .append(fieldSort.order().toString().toUpperCase(Locale.ROOT))
                        .append(", ");
                } else if (sort instanceof ScoreSortBuilder) {
                    if (hasScoreSort) {
                        throw new IllegalArgumentException("Multiple score sort is not supported");
                    }

                    sb.append("_score ").append(sort.order().toString().toUpperCase(Locale.ROOT)).append(", ");
                    hasScoreSort = true;
                } else {
                    throw new IllegalArgumentException("Unsupported sort type: " + sort.getClass().getName());
                }
            }
            sb.delete(sb.length() - 2, sb.length());
            return sb.toString();
        }
    }
}

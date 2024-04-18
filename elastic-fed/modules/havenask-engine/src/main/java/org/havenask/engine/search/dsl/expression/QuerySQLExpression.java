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

import java.util.List;

public class QuerySQLExpression extends Expression {
    public final List<String> select;
    public final String from;
    public final WhereExpression where;
    public final OrderByExpression orderBy;
    public final int limit;
    public final int offset;

    public QuerySQLExpression(List<String> select, String from, WhereExpression where, OrderByExpression orderBy, int limit, int offset) {
        this.select = select;
        this.from = from;
        this.where = where;
        this.orderBy = orderBy;
        this.limit = limit;
        this.offset = offset;
    }

    @Override
    public String translate() {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ");
        for (String field : select) {
            sb.append("`").append(field).append("`, ");
        }
        sb.delete(sb.length() - 2, sb.length());
        sb.append(" FROM `").append(from).append("` ");
        if (where != null) {
            sb.append(where.translate()).append(" ");
        }
        if (orderBy != null) {
            sb.append(orderBy.translate()).append(" ");
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

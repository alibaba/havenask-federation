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

package org.havenask.engine.search.dsl.expression.query;

import org.havenask.engine.search.dsl.expression.Expression;

import java.util.List;

public class BoolExpression extends Expression {
    private final List<Expression> must;
    private final List<Expression> should;
    private final List<Expression> mustNot;
    private final List<Expression> filter;
    private final String minimumShouldMatch;

    public BoolExpression(
        List<Expression> must,
        List<Expression> should,
        List<Expression> mustNot,
        List<Expression> filter,
        String minimumShouldMatch
    ) {
        this.must = must;
        this.should = should;
        this.mustNot = mustNot;
        this.filter = filter;
        this.minimumShouldMatch = minimumShouldMatch;
    }

    @Override
    public String translate() {
        if (should.size() > 0 && (must.size() > 0 || mustNot.size() > 0 || filter.size() > 0)) {
            throw new IllegalArgumentException("should can not be used with must, mustNot or filter");
        }

        if (should.size() > 0 && minimumShouldMatch != null) {
            throw new IllegalArgumentException("minimumShouldMatch not supported with should clauses");
        }

        StringBuilder sb = new StringBuilder("(");
        if (must.size() > 0) {
            for (Expression expression : must) {
                sb.append(expression.translate()).append(" AND ");
            }
            sb.delete(sb.length() - 5, sb.length());
        }
        if (mustNot.size() > 0) {
            if (sb.length() > 1) {
                sb.append("AND ");
            }
            sb.append("NOT (");
            for (Expression expression : mustNot) {
                sb.append(expression.translate()).append(" AND ");
            }
            sb.delete(sb.length() - 5, sb.length());
            sb.append(") ");
        }
        if (filter.size() > 0) {
            if (sb.length() > 1) {
                sb.append("AND ");
            }
            sb.append(" (");
            for (Expression expression : filter) {
                sb.append(expression.translate()).append(" AND ");
            }
            sb.delete(sb.length() - 5, sb.length());
            sb.append(") ");
        }

        if (should.size() > 0) {
            for (Expression expression : should) {
                sb.append(expression.translate()).append(" OR ");
            }
            sb.delete(sb.length() - 4, sb.length());
        }
        if (sb.length() > 1) {
            sb.append(")");
        } else {
            // delete (
            sb.delete(0, sb.length());
        }
        return sb.toString();
    }
}

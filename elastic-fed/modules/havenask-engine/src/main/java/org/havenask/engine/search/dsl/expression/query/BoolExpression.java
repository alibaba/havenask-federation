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

        StringBuilder shouldStr = new StringBuilder();
        if (should.size() > 0) {
            shouldStr.append("(");
            for (Expression expression : should) {
                shouldStr.append(expression.translate()).append(" OR ");
            }
            shouldStr.delete(shouldStr.length() - 4, shouldStr.length());
            shouldStr.append(")");
            return shouldStr.toString();
        }

        StringBuilder mustStr = new StringBuilder();
        if (must.size() > 0) {
            mustStr.append("(");
            for (Expression expression : must) {
                mustStr.append(expression.translate()).append(" AND ");
            }
            mustStr.delete(mustStr.length() - 5, mustStr.length());
            mustStr.append(")");
        }

        StringBuilder filterStr = new StringBuilder();
        if (filter.size() > 0) {
            filterStr.append("(");
            for (Expression expression : filter) {
                filterStr.append(expression.translate()).append(" AND ");
            }
            filterStr.delete(filterStr.length() - 5, filterStr.length());
            filterStr.append(")");
        }

        StringBuilder mustNotStr = new StringBuilder();
        if (mustNot.size() > 0) {
            mustNotStr.append("NOT (");
            for (Expression expression : mustNot) {
                mustNotStr.append(expression.translate()).append(" AND ");
            }
            mustNotStr.delete(mustNotStr.length() - 5, mustNotStr.length());
            mustNotStr.append(")");
        }

        StringBuilder sb = new StringBuilder();
        if (mustStr.length() > 0) {
            sb.append(mustStr);
        }
        if (filterStr.length() > 0) {
            if (sb.length() > 0) {
                sb.append(" AND ");
            }
            sb.append(filterStr);
        }
        if (mustNotStr.length() > 0) {
            if (sb.length() > 0) {
                sb.append(" AND ");
            }
            sb.append(mustNotStr);
        }

        return sb.toString();
    }
}

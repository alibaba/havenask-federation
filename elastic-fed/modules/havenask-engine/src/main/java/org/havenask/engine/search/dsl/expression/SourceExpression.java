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
import java.util.stream.Collectors;

import org.havenask.engine.search.dsl.expression.query.BoolExpression;
import org.havenask.engine.search.dsl.expression.query.TermExpression;
import org.havenask.index.query.BoolQueryBuilder;
import org.havenask.index.query.QueryBuilder;
import org.havenask.index.query.TermQueryBuilder;
import org.havenask.search.builder.SearchSourceBuilder;

public class SourceExpression extends Expression {
    private final SearchSourceBuilder searchSourceBuilder;

    private final WhereExpression where;
    private final OrderByExpression orderBy;
    private QuerySQLExpression querySQLExpression;

    public SourceExpression(SearchSourceBuilder searchSourceBuilder) {
        this.searchSourceBuilder = searchSourceBuilder;
        this.where = new WhereExpression(visitQuery(searchSourceBuilder.query()));
        this.orderBy = new OrderByExpression(searchSourceBuilder.sorts());
    }

    public QuerySQLExpression getQuerySQLExpression(String index) {
        if (querySQLExpression == null) {
            querySQLExpression = new QuerySQLExpression(
                List.of("_id"),
                index,
                where,
                orderBy,
                searchSourceBuilder.size(),
                searchSourceBuilder.from()
            );
        }

        return querySQLExpression;
    }

    public static Expression visitQuery(QueryBuilder query) {
        if (query instanceof BoolQueryBuilder) {
            return visitBoolQuery((BoolQueryBuilder) query);
        } else if (query instanceof TermQueryBuilder) {
            return new TermExpression((TermQueryBuilder) query);
        } else {
            throw new IllegalArgumentException("Unsupported query type: " + query.getClass().getName());
        }
    }

    private static Expression visitBoolQuery(BoolQueryBuilder query) {
        List<Expression> must = query.must().stream().map(SourceExpression::visitQuery).collect(Collectors.toList());
        List<Expression> mustNot = query.mustNot().stream().map(SourceExpression::visitQuery).collect(Collectors.toList());
        List<Expression> should = query.should().stream().map(SourceExpression::visitQuery).collect(Collectors.toList());
        List<Expression> filter = query.filter().stream().map(SourceExpression::visitQuery).collect(Collectors.toList());
        return new BoolExpression(must, should, mustNot, filter, query.minimumShouldMatch());
    }

    @Override
    public String translate() {
        return "";
    }
}

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
import org.havenask.engine.search.dsl.expression.ExpressionContext;
import org.havenask.index.query.BoolQueryBuilder;
import org.havenask.index.query.ExistsQueryBuilder;
import org.havenask.index.query.MatchAllQueryBuilder;
import org.havenask.index.query.MatchPhraseQueryBuilder;
import org.havenask.index.query.MatchQueryBuilder;
import org.havenask.index.query.QueryBuilder;
import org.havenask.index.query.QueryStringQueryBuilder;
import org.havenask.index.query.RangeQueryBuilder;
import org.havenask.index.query.TermQueryBuilder;
import org.havenask.index.query.TermsQueryBuilder;
import org.havenask.index.query.WrapperQueryBuilder;

import java.util.List;
import java.util.stream.Collectors;

public abstract class QueryExpression extends Expression {
    public static Expression visitQuery(QueryBuilder query, ExpressionContext context) {
        if (query == null) {
            return new MatchAllExpression();
        }

        if (query instanceof BoolQueryBuilder) {
            return visitBoolQuery((BoolQueryBuilder) query, context);
        } else if (query instanceof TermQueryBuilder) {
            return new TermExpression((TermQueryBuilder) query);
        } else if (query instanceof MatchAllQueryBuilder) {
            return new MatchAllExpression();
        } else if (query instanceof RangeQueryBuilder) {
            return new RangeExpression((RangeQueryBuilder) query);
        } else if (query instanceof MatchQueryBuilder) {
            return new MatchExpression(((MatchQueryBuilder) query));
        } else if (query instanceof QueryStringQueryBuilder) {
            return new QueryStringExpression((QueryStringQueryBuilder) query);
        } else if (query instanceof MatchPhraseQueryBuilder) {
            return new MatchPhraseExpression((MatchPhraseQueryBuilder) query);
        } else if (query instanceof TermsQueryBuilder) {
            return new TermsExpression((TermsQueryBuilder) query);
        } else if (query instanceof ExistsQueryBuilder) {
            return new ExistExpression((ExistsQueryBuilder) query);
        } else if (query instanceof WrapperQueryBuilder) {
            return new WrapperQueryExpression((WrapperQueryBuilder) query, context);
        } else {
            throw new IllegalArgumentException("Unsupported query type: " + query.getClass().getName());
        }
    }

    public static BoolExpression visitBoolQuery(BoolQueryBuilder query, ExpressionContext context) {
        List<Expression> must = query.must().stream().map((q) -> visitQuery(q, context)).collect(Collectors.toList());
        List<Expression> mustNot = query.mustNot().stream().map((q) -> visitQuery(q, context)).collect(Collectors.toList());
        List<Expression> should = query.should().stream().map((q) -> visitQuery(q, context)).collect(Collectors.toList());
        List<Expression> filter = query.filter().stream().map((q) -> visitQuery(q, context)).collect(Collectors.toList());
        return new BoolExpression(must, should, mustNot, filter, query.minimumShouldMatch());
    }

    public abstract String fieldName();
}

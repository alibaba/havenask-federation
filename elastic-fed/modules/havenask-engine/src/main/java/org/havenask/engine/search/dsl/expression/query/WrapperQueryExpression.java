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

import org.havenask.common.xcontent.LoggingDeprecationHandler;
import org.havenask.common.xcontent.NamedXContentRegistry;
import org.havenask.common.xcontent.XContentFactory;
import org.havenask.common.xcontent.XContentParser;
import org.havenask.engine.search.dsl.expression.Expression;
import org.havenask.engine.search.dsl.expression.ExpressionContext;
import org.havenask.index.query.AbstractQueryBuilder;
import org.havenask.index.query.BoolQueryBuilder;
import org.havenask.index.query.QueryBuilder;
import org.havenask.index.query.WrapperQueryBuilder;

import java.io.IOException;

import static org.havenask.index.query.AbstractQueryBuilder.DEFAULT_BOOST;

public class WrapperQueryExpression extends QueryExpression {
    private final WrapperQueryBuilder wrapperQueryBuilder;
    private final QueryBuilder queryBuilder;
    private final Expression queryExpression;

    public WrapperQueryExpression(WrapperQueryBuilder wrapperQueryBuilder, ExpressionContext context) {
        this.wrapperQueryBuilder = wrapperQueryBuilder;
        this.queryBuilder = source(context.getNamedXContentRegistry());
        this.queryExpression = QueryExpression.visitQuery(queryBuilder, context);
    }

    private QueryBuilder source(NamedXContentRegistry namedXContentRegistry) {
        try (
            XContentParser qSourceParser = XContentFactory.xContent(wrapperQueryBuilder.source())
                .createParser(namedXContentRegistry, LoggingDeprecationHandler.INSTANCE, wrapperQueryBuilder.source())
        ) {
            final QueryBuilder queryBuilder = AbstractQueryBuilder.parseInnerQueryBuilder(qSourceParser);
            if (wrapperQueryBuilder.boost() != DEFAULT_BOOST || wrapperQueryBuilder.queryName() != null) {
                final BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
                boolQueryBuilder.must(queryBuilder);
                return boolQueryBuilder;
            }
            return queryBuilder;
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to parse source query", e);
        }
    }

    public QueryBuilder getQueryBuilder() {
        return queryBuilder;
    }

    @Override
    public String fieldName() {
        return "";
    }

    @Override
    public String translate() {
        return queryExpression.translate();
    }
}

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

import org.havenask.engine.index.config.Schema;
import org.havenask.index.query.RangeQueryBuilder;

public class RangeExpression extends QueryExpression {
    private final RangeQueryBuilder rangeQueryBuilder;

    public RangeExpression(RangeQueryBuilder rangeQueryBuilder) {
        this.rangeQueryBuilder = rangeQueryBuilder;
    }

    @Override
    public String translate() {
        StringBuilder sb = new StringBuilder();
        // sb.append("QUERY('', '")
        // .append(Schema.encodeFieldWithDot(rangeQueryBuilder.fieldName()))
        // .append(":")
        // .append(rangeQueryBuilder.includeLower() ? "[" : "(")
        // .append(rangeQueryBuilder.from())
        // .append(",")
        // .append(rangeQueryBuilder.to())
        // .append(rangeQueryBuilder.includeUpper() ? "]" : ")")
        // .append("')");

        // `field` >= 1 AND `field` < 10
        StringBuilder from = new StringBuilder();
        if (rangeQueryBuilder.from() != null) {
            from.append("`");
            from.append(Schema.encodeFieldWithDot(rangeQueryBuilder.fieldName()));
            if (rangeQueryBuilder.includeLower()) {
                from.append("` >= ");
            } else {
                from.append("` > ");
            }
            from.append(rangeQueryBuilder.from());
        }

        StringBuilder to = new StringBuilder();
        if (rangeQueryBuilder.to() != null) {
            to.append("`");
            to.append(Schema.encodeFieldWithDot(rangeQueryBuilder.fieldName()));
            if (rangeQueryBuilder.includeUpper()) {
                to.append("` <= ");
            } else {
                to.append("` < ");
            }
            to.append(rangeQueryBuilder.to());
        }

        if (from.length() > 0 && to.length() > 0) {
            sb.append(from).append(" AND ").append(to);
        } else if (from.length() > 0) {
            sb.append(from);
        } else if (to.length() > 0) {
            sb.append(to);
        }

        return sb.toString();
    }

    @Override
    public String fieldName() {
        return Schema.encodeFieldWithDot(rangeQueryBuilder.fieldName());
    }
}

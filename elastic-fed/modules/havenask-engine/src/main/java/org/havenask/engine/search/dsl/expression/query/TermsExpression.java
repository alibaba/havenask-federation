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
import org.havenask.index.query.TermsQueryBuilder;

public class TermsExpression extends QueryExpression {
    private final TermsQueryBuilder termsQueryBuilder;

    public TermsExpression(TermsQueryBuilder termsQueryBuilder) {
        this.termsQueryBuilder = termsQueryBuilder;
    }

    @Override
    public String fieldName() {
        return Schema.encodeFieldWithDot(termsQueryBuilder.fieldName());
    }

    @Override
    public String translate() {
        // contain(`nid`, '1|2|3')
        StringBuilder sb = new StringBuilder();
        sb.append("contain(`").append(Schema.encodeFieldWithDot(termsQueryBuilder.fieldName())).append("`, '");
        for (Object value : termsQueryBuilder.values()) {
            sb.append(value).append("|");
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append("')");
        return sb.toString();
    }
}

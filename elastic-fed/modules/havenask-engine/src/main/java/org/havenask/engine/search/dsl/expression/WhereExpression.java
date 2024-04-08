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

public class WhereExpression extends Expression {
    private final Expression expression;

    public WhereExpression(Expression expression) {
        this.expression = expression;
    }

    @Override
    public String translate() {
        String where = expression.translate();
        if (where == null || where.isEmpty()) {
            return "WHERE 1=1";
        } else {
            return "WHERE " + where;
        }
    }
}
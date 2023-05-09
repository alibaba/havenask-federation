/*
*Copyright (c) 2021, Alibaba Group;
*Licensed under the Apache License, Version 2.0 (the "License");
*you may not use this file except in compliance with the License.
*You may obtain a copy of the License at

*   http://www.apache.org/licenses/LICENSE-2.0

*Unless required by applicable law or agreed to in writing, software
*distributed under the License is distributed on an "AS IS" BASIS,
*WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*See the License for the specific language governing permissions and
*limitations under the License.
*/

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright Havenask Contributors. See
 * GitHub history for details.
 */

package org.havenask.painless.node;

import org.havenask.painless.Location;
import org.havenask.painless.phase.UserTreeVisitor;

import java.util.Objects;

/**
 * Represents an if/else block.
 */
public class SIfElse extends AStatement {

    private final AExpression conditionNode;
    private final SBlock ifBlockNode;
    private final SBlock elseBlockNode;

    public SIfElse(int identifier, Location location, AExpression conditionNode, SBlock ifBlockNode, SBlock elseBlockNode) {
        super(identifier, location);

        this.conditionNode = Objects.requireNonNull(conditionNode);
        this.ifBlockNode = ifBlockNode;
        this.elseBlockNode = elseBlockNode;
    }

    public AExpression getConditionNode() {
        return conditionNode;
    }

    public SBlock getIfBlockNode() {
        return ifBlockNode;
    }

    public SBlock getElseBlockNode() {
        return elseBlockNode;
    }

    @Override
    public <Scope> void visit(UserTreeVisitor<Scope> userTreeVisitor, Scope scope) {
        userTreeVisitor.visitIfElse(this, scope);
    }

    @Override
    public <Scope> void visitChildren(UserTreeVisitor<Scope> userTreeVisitor, Scope scope) {
        conditionNode.visit(userTreeVisitor, scope);

        if (ifBlockNode != null) {
            ifBlockNode.visit(userTreeVisitor, scope);
        }

        if (elseBlockNode != null) {
            elseBlockNode.visit(userTreeVisitor, scope);
        }
    }
}

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

package org.havenask.painless.ir;

import org.havenask.painless.ClassWriter;
import org.havenask.painless.Location;
import org.havenask.painless.MethodWriter;
import org.havenask.painless.lookup.PainlessConstructor;
import org.havenask.painless.lookup.PainlessMethod;
import org.havenask.painless.phase.IRTreeVisitor;
import org.havenask.painless.symbol.WriteScope;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.Method;

import java.util.ArrayList;
import java.util.List;

public class MapInitializationNode extends ExpressionNode {

    /* ---- begin tree structure ---- */

    private final List<ExpressionNode> keyNodes = new ArrayList<>();
    private final List<ExpressionNode> valueNodes = new ArrayList<>();

    public void addArgumentNode(ExpressionNode keyNode, ExpressionNode valueNode) {
        keyNodes.add(keyNode);
        valueNodes.add(valueNode);
    }

    public ExpressionNode getKeyNode(int index) {
        return keyNodes.get(index);
    }

    public ExpressionNode getValueNode(int index) {
        return valueNodes.get(index);
    }

    public int getArgumentsSize() {
        return keyNodes.size();
    }

    public List<ExpressionNode> getKeyNodes() {
        return keyNodes;
    }

    public List<ExpressionNode> getValueNodes() {
        return valueNodes;
    }

    /* ---- end tree structure, begin node data ---- */

    private PainlessConstructor constructor;
    private PainlessMethod method;

    public void setConstructor(PainlessConstructor constructor) {
        this.constructor = constructor;
    }

    public PainlessConstructor getConstructor() {
        return constructor;
    }

    public void setMethod(PainlessMethod method) {
        this.method = method;
    }

    public PainlessMethod getMethod() {
        return method;
    }

    /* ---- end node data, begin visitor ---- */

    @Override
    public <Scope> void visit(IRTreeVisitor<Scope> irTreeVisitor, Scope scope) {
        irTreeVisitor.visitMapInitialization(this, scope);
    }

    @Override
    public <Scope> void visitChildren(IRTreeVisitor<Scope> irTreeVisitor, Scope scope) {
        for (ExpressionNode keyNode : keyNodes) {
            keyNode.visit(irTreeVisitor, scope);
        }

        for (ExpressionNode valueNode : valueNodes) {
            valueNode.visit(irTreeVisitor, scope);
        }
    }

    /* ---- end visitor ---- */

    public MapInitializationNode(Location location) {
        super(location);
    }

    @Override
    protected void write(ClassWriter classWriter, MethodWriter methodWriter, WriteScope writeScope) {
        methodWriter.writeDebugInfo(getLocation());

        methodWriter.newInstance(MethodWriter.getType(getExpressionType()));
        methodWriter.dup();
        methodWriter.invokeConstructor(
                    Type.getType(constructor.javaConstructor.getDeclaringClass()), Method.getMethod(constructor.javaConstructor));

        for (int index = 0; index < getArgumentsSize(); ++index) {
            methodWriter.dup();
            getKeyNode(index).write(classWriter, methodWriter, writeScope);
            getValueNode(index).write(classWriter, methodWriter, writeScope);
            methodWriter.invokeMethodCall(method);
            methodWriter.pop();
        }
    }
}

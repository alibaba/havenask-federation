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
import org.havenask.painless.DefBootstrap;
import org.havenask.painless.Location;
import org.havenask.painless.MethodWriter;
import org.havenask.painless.Operation;
import org.havenask.painless.lookup.PainlessLookupUtility;
import org.havenask.painless.lookup.def;
import org.havenask.painless.phase.IRTreeVisitor;
import org.havenask.painless.symbol.WriteScope;
import org.objectweb.asm.Label;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

public class UnaryMathNode extends UnaryNode {

    /* ---- begin node data ---- */

    private Operation operation;
    private Class<?> unaryType;
    private boolean cat;
    private boolean originallyExplicit; // record whether there was originally an explicit cast

    public void setOperation(Operation operation) {
        this.operation = operation;
    }

    public Operation getOperation() {
        return operation;
    }

    public void setUnaryType(Class<?> unaryType) {
        this.unaryType = unaryType;
    }

    public Class<?> getUnaryType() {
        return unaryType;
    }

    public String getUnaryCanonicalTypeName() {
        return PainlessLookupUtility.typeToCanonicalTypeName(unaryType);
    }

    public void setCat(boolean cat) {
        this.cat = cat;
    }

    public boolean getCat() {
        return cat;
    }

    public void setOriginallyExplicit(boolean originallyExplicit) {
        this.originallyExplicit = originallyExplicit;
    }

    public boolean getOriginallyExplicit() {
        return originallyExplicit;
    }

    /* ---- end node data, begin visitor ---- */

    @Override
    public <Scope> void visit(IRTreeVisitor<Scope> irTreeVisitor, Scope scope) {
        irTreeVisitor.visitUnaryMath(this, scope);
    }

    @Override
    public <Scope> void visitChildren(IRTreeVisitor<Scope> irTreeVisitor, Scope scope) {
        getChildNode().visit(irTreeVisitor, scope);
    }

    /* ---- end visitor ---- */

    public UnaryMathNode(Location location) {
        super(location);
    }

    @Override
    public void write(ClassWriter classWriter, MethodWriter methodWriter, WriteScope writeScope) {
        methodWriter.writeDebugInfo(getLocation());

        if (operation == Operation.NOT) {
            Label fals = new Label();
            Label end = new Label();

            getChildNode().write(classWriter, methodWriter, writeScope);

            methodWriter.ifZCmp(Opcodes.IFEQ, fals);

            methodWriter.push(false);
            methodWriter.goTo(end);
            methodWriter.mark(fals);
            methodWriter.push(true);
            methodWriter.mark(end);
        } else {
            getChildNode().write(classWriter, methodWriter, writeScope);

            // Def calls adopt the wanted return value. If there was a narrowing cast,
            // we need to flag that so that it's done at runtime.
            int defFlags = 0;

            if (originallyExplicit) {
                defFlags |= DefBootstrap.OPERATOR_EXPLICIT_CAST;
            }

            Type actualType = MethodWriter.getType(getExpressionType());
            Type childType = MethodWriter.getType(getChildNode().getExpressionType());

            if (operation == Operation.BWNOT) {
                if (getUnaryType() == def.class) {
                    org.objectweb.asm.Type descriptor = org.objectweb.asm.Type.getMethodType(actualType, childType);
                    methodWriter.invokeDefCall("not", descriptor, DefBootstrap.UNARY_OPERATOR, defFlags);
                } else {
                    if (getUnaryType() == int.class) {
                        methodWriter.push(-1);
                    } else if (getUnaryType() == long.class) {
                        methodWriter.push(-1L);
                    } else {
                        throw new IllegalStateException("unexpected unary math operation [" + operation + "] " +
                                "for type [" + getExpressionCanonicalTypeName() + "]");
                    }

                    methodWriter.math(MethodWriter.XOR, actualType);
                }
            } else if (operation == Operation.SUB) {
                if (getUnaryType() == def.class) {
                    org.objectweb.asm.Type descriptor = org.objectweb.asm.Type.getMethodType(actualType, childType);
                    methodWriter.invokeDefCall("neg", descriptor, DefBootstrap.UNARY_OPERATOR, defFlags);
                } else {
                    methodWriter.math(MethodWriter.NEG, actualType);
                }
            } else if (operation == Operation.ADD) {
                if (getUnaryType() == def.class) {
                    org.objectweb.asm.Type descriptor = org.objectweb.asm.Type.getMethodType(actualType, childType);
                    methodWriter.invokeDefCall("plus", descriptor, DefBootstrap.UNARY_OPERATOR, defFlags);
                }
            } else {
                throw new IllegalStateException("unexpected unary math operation [" + operation + "] " +
                        "for type [" + getExpressionCanonicalTypeName() + "]");
            }
        }
    }
}

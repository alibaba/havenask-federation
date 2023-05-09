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
import org.havenask.painless.lookup.PainlessClassBinding;
import org.havenask.painless.lookup.PainlessInstanceBinding;
import org.havenask.painless.lookup.PainlessMethod;
import org.havenask.painless.phase.IRTreeVisitor;
import org.havenask.painless.symbol.FunctionTable.LocalFunction;
import org.havenask.painless.symbol.WriteScope;
import org.objectweb.asm.Label;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.Method;

import static org.havenask.painless.WriterConstants.CLASS_TYPE;

public class InvokeCallMemberNode extends ArgumentsNode {

    /* ---- begin node data ---- */

    private LocalFunction localFunction;
    private PainlessMethod importedMethod;
    private PainlessClassBinding classBinding;
    private int classBindingOffset;
    private PainlessInstanceBinding instanceBinding;
    private String bindingName;

    public void setLocalFunction(LocalFunction localFunction) {
        this.localFunction = localFunction;
    }

    public LocalFunction getLocalFunction() {
        return localFunction;
    }

    public void setImportedMethod(PainlessMethod importedMethod) {
        this.importedMethod = importedMethod;
    }

    public PainlessMethod getImportedMethod() {
        return importedMethod;
    }

    public void setClassBinding(PainlessClassBinding classBinding) {
        this.classBinding = classBinding;
    }

    public PainlessClassBinding getClassBinding() {
        return classBinding;
    }

    public void setClassBindingOffset(int classBindingOffset) {
        this.classBindingOffset = classBindingOffset;
    }

    public int getClassBindingOffset() {
        return classBindingOffset;
    }

    public void setInstanceBinding(PainlessInstanceBinding instanceBinding) {
        this.instanceBinding = instanceBinding;
    }

    public PainlessInstanceBinding getInstanceBinding() {
        return instanceBinding;
    }

    public void setBindingName(String bindingName) {
        this.bindingName = bindingName;
    }

    public String getBindingName() {
        return bindingName;
    }

    /* ---- end node data, begin visitor ---- */

    @Override
    public <Scope> void visit(IRTreeVisitor<Scope> irTreeVisitor, Scope scope) {
        irTreeVisitor.visitInvokeCallMember(this, scope);
    }

    @Override
    public <Scope> void visitChildren(IRTreeVisitor<Scope> irTreeVisitor, Scope scope) {
        for (ExpressionNode argumentNode : getArgumentNodes()) {
            argumentNode.visit(irTreeVisitor, scope);
        }
    }

    /* ---- end visitor ---- */

    public InvokeCallMemberNode(Location location) {
        super(location);
    }

    @Override
    public void write(ClassWriter classWriter, MethodWriter methodWriter, WriteScope writeScope) {
        methodWriter.writeDebugInfo(getLocation());

        if (localFunction != null) {
            if (localFunction.isStatic() == false) {
                methodWriter.loadThis();
            }

            for (ExpressionNode argumentNode : getArgumentNodes()) {
                argumentNode.write(classWriter, methodWriter, writeScope);
           }

            if (localFunction.isStatic()) {
                methodWriter.invokeStatic(CLASS_TYPE, localFunction.getAsmMethod());
            } else {
                methodWriter.invokeVirtual(CLASS_TYPE, localFunction.getAsmMethod());
            }
        } else if (importedMethod != null) {
            for (ExpressionNode argumentNode : getArgumentNodes()) {
                argumentNode.write(classWriter, methodWriter, writeScope);
           }

            methodWriter.invokeStatic(Type.getType(importedMethod.targetClass),
                    new Method(importedMethod.javaMethod.getName(), importedMethod.methodType.toMethodDescriptorString()));
        } else if (classBinding != null) {
            Type type = Type.getType(classBinding.javaConstructor.getDeclaringClass());
            int javaConstructorParameterCount = classBinding.javaConstructor.getParameterCount() - classBindingOffset;

            Label nonNull = new Label();

            methodWriter.loadThis();
            methodWriter.getField(CLASS_TYPE, bindingName, type);
            methodWriter.ifNonNull(nonNull);
            methodWriter.loadThis();
            methodWriter.newInstance(type);
            methodWriter.dup();

            if (classBindingOffset == 1) {
                methodWriter.loadThis();
            }

            for (int argument = 0; argument < javaConstructorParameterCount; ++argument) {
                getArgumentNodes().get(argument).write(classWriter, methodWriter, writeScope);
           }

            methodWriter.invokeConstructor(type, Method.getMethod(classBinding.javaConstructor));
            methodWriter.putField(CLASS_TYPE, bindingName, type);

            methodWriter.mark(nonNull);
            methodWriter.loadThis();
            methodWriter.getField(CLASS_TYPE, bindingName, type);

            for (int argument = 0; argument < classBinding.javaMethod.getParameterCount(); ++argument) {
                getArgumentNodes().get(argument + javaConstructorParameterCount).write(classWriter, methodWriter, writeScope);
            }

            methodWriter.invokeVirtual(type, Method.getMethod(classBinding.javaMethod));
        } else if (instanceBinding != null) {
            Type type = Type.getType(instanceBinding.targetInstance.getClass());

            methodWriter.loadThis();
            methodWriter.getStatic(CLASS_TYPE, bindingName, type);

            for (int argument = 0; argument < instanceBinding.javaMethod.getParameterCount(); ++argument) {
                getArgumentNodes().get(argument).write(classWriter, methodWriter, writeScope);
            }

            methodWriter.invokeVirtual(type, Method.getMethod(instanceBinding.javaMethod));
        } else {
            throw new IllegalStateException("invalid unbound call");
        }
    }
}

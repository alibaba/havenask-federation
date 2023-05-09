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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.havenask.painless.phase;

import org.havenask.painless.Location;
import org.havenask.painless.PainlessError;
import org.havenask.painless.PainlessExplainError;
import org.havenask.painless.ScriptClassInfo;
import org.havenask.painless.ScriptClassInfo.MethodArgument;
import org.havenask.painless.ir.BinaryImplNode;
import org.havenask.painless.ir.BlockNode;
import org.havenask.painless.ir.CatchNode;
import org.havenask.painless.ir.ConstantNode;
import org.havenask.painless.ir.DeclarationNode;
import org.havenask.painless.ir.ExpressionNode;
import org.havenask.painless.ir.FieldNode;
import org.havenask.painless.ir.FunctionNode;
import org.havenask.painless.ir.IRNode;
import org.havenask.painless.ir.InvokeCallMemberNode;
import org.havenask.painless.ir.InvokeCallNode;
import org.havenask.painless.ir.LoadFieldMemberNode;
import org.havenask.painless.ir.LoadVariableNode;
import org.havenask.painless.ir.NullNode;
import org.havenask.painless.ir.ReturnNode;
import org.havenask.painless.ir.StaticNode;
import org.havenask.painless.ir.ThrowNode;
import org.havenask.painless.ir.TryNode;
import org.havenask.painless.lookup.PainlessLookup;
import org.havenask.painless.lookup.PainlessMethod;
import org.havenask.painless.node.AStatement;
import org.havenask.painless.node.SExpression;
import org.havenask.painless.node.SFunction;
import org.havenask.painless.node.SReturn;
import org.havenask.painless.symbol.Decorations.Converter;
import org.havenask.painless.symbol.Decorations.IRNodeDecoration;
import org.havenask.painless.symbol.Decorations.MethodEscape;
import org.havenask.painless.symbol.FunctionTable.LocalFunction;
import org.havenask.painless.symbol.ScriptScope;
import org.havenask.script.ScriptException;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.commons.Method;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class PainlessUserTreeToIRTreePhase extends DefaultUserTreeToIRTreePhase {

    @Override
    public void visitFunction(SFunction userFunctionNode, ScriptScope scriptScope) {
        String functionName = userFunctionNode.getFunctionName();

        // This injects additional ir nodes required for
        // the "execute" method. This includes injection of ir nodes
        // to convert get methods into local variables for those
        // that are used and adds additional sandboxing by wrapping
        // the main "execute" block with several exceptions.
        if ("execute".equals(functionName)) {
            ScriptClassInfo scriptClassInfo = scriptScope.getScriptClassInfo();
            LocalFunction localFunction =
                    scriptScope.getFunctionTable().getFunction(functionName, scriptClassInfo.getExecuteArguments().size());
            Class<?> returnType = localFunction.getReturnType();

            boolean methodEscape = scriptScope.getCondition(userFunctionNode, MethodEscape.class);
            BlockNode irBlockNode = (BlockNode)visit(userFunctionNode.getBlockNode(), scriptScope);

            if (methodEscape == false) {
                ExpressionNode irExpressionNode;

                if (returnType == void.class) {
                    irExpressionNode = null;
                } else {
                    if (returnType.isPrimitive()) {
                        ConstantNode irConstantNode = new ConstantNode(userFunctionNode.getLocation());
                        irConstantNode.setExpressionType(returnType);

                        if (returnType == boolean.class) {
                            irConstantNode.setConstant(false);
                        } else if (returnType == byte.class
                                || returnType == char.class
                                || returnType == short.class
                                || returnType == int.class) {
                            irConstantNode.setConstant(0);
                        } else if (returnType == long.class) {
                            irConstantNode.setConstant(0L);
                        } else if (returnType == float.class) {
                            irConstantNode.setConstant(0f);
                        } else if (returnType == double.class) {
                            irConstantNode.setConstant(0d);
                        } else {
                            throw userFunctionNode.createError(new IllegalStateException("illegal tree structure"));
                        }

                        irExpressionNode = irConstantNode;
                    } else {
                        irExpressionNode = new NullNode(userFunctionNode.getLocation());
                        irExpressionNode.setExpressionType(returnType);
                    }
                }

                ReturnNode irReturnNode = new ReturnNode(userFunctionNode.getLocation());
                irReturnNode.setExpressionNode(irExpressionNode);

                irBlockNode.addStatementNode(irReturnNode);
            }

            List<String> parameterNames = new ArrayList<>(scriptClassInfo.getExecuteArguments().size());

            for (MethodArgument methodArgument : scriptClassInfo.getExecuteArguments()) {
                parameterNames.add(methodArgument.getName());
            }

            FunctionNode irFunctionNode = new FunctionNode(userFunctionNode.getLocation());
            irFunctionNode.setBlockNode(irBlockNode);
            irFunctionNode.setName("execute");
            irFunctionNode.setReturnType(returnType);
            irFunctionNode.getTypeParameters().addAll(localFunction.getTypeParameters());
            irFunctionNode.getParameterNames().addAll(parameterNames);
            irFunctionNode.setStatic(false);
            irFunctionNode.setVarArgs(false);
            irFunctionNode.setSynthetic(false);
            irFunctionNode.setMaxLoopCounter(scriptScope.getCompilerSettings().getMaxLoopCounter());

            injectStaticFieldsAndGetters();
            injectGetsDeclarations(irBlockNode, scriptScope);
            injectNeedsMethods(scriptScope);
            injectSandboxExceptions(irFunctionNode);

            scriptScope.putDecoration(userFunctionNode, new IRNodeDecoration(irFunctionNode));
        } else {
            super.visitFunction(userFunctionNode, scriptScope);
        }
    }

    // adds static fields and getter methods required by PainlessScript for exception handling
    protected void injectStaticFieldsAndGetters() {
        Location internalLocation = new Location("$internal$ScriptInjectionPhase$injectStaticFieldsAndGetters", 0);
        int modifiers = Opcodes.ACC_PUBLIC | Opcodes.ACC_STATIC;

        FieldNode irFieldNode = new FieldNode(internalLocation);
        irFieldNode.setModifiers(modifiers);
        irFieldNode.setFieldType(String.class);
        irFieldNode.setName("$NAME");

        irClassNode.addFieldNode(irFieldNode);

        irFieldNode = new FieldNode(internalLocation);
        irFieldNode.setModifiers(modifiers);
        irFieldNode.setFieldType(String.class);
        irFieldNode.setName("$SOURCE");

        irClassNode.addFieldNode(irFieldNode);

        irFieldNode = new FieldNode(internalLocation);
        irFieldNode.setModifiers(modifiers);
        irFieldNode.setFieldType(BitSet.class);
        irFieldNode.setName("$STATEMENTS");

        irClassNode.addFieldNode(irFieldNode);

        FunctionNode irFunctionNode = new FunctionNode(internalLocation);
        irFunctionNode.setName("getName");
        irFunctionNode.setReturnType(String.class);
        irFunctionNode.setStatic(false);
        irFunctionNode.setVarArgs(false);
        irFunctionNode.setSynthetic(true);
        irFunctionNode.setMaxLoopCounter(0);

        irClassNode.addFunctionNode(irFunctionNode);

        BlockNode irBlockNode = new BlockNode(internalLocation);
        irBlockNode.setAllEscape(true);

        irFunctionNode.setBlockNode(irBlockNode);

        ReturnNode irReturnNode = new ReturnNode(internalLocation);

        irBlockNode.addStatementNode(irReturnNode);

        LoadFieldMemberNode irLoadFieldMemberNode = new LoadFieldMemberNode(internalLocation);
        irLoadFieldMemberNode.setExpressionType(String.class);
        irLoadFieldMemberNode.setName("$NAME");
        irLoadFieldMemberNode.setStatic(true);

        irReturnNode.setExpressionNode(irLoadFieldMemberNode);

        irFunctionNode = new FunctionNode(internalLocation);
        irFunctionNode.setName("getSource");
        irFunctionNode.setReturnType(String.class);
        irFunctionNode.setStatic(false);
        irFunctionNode.setVarArgs(false);
        irFunctionNode.setSynthetic(true);
        irFunctionNode.setMaxLoopCounter(0);

        irClassNode.addFunctionNode(irFunctionNode);

        irBlockNode = new BlockNode(internalLocation);
        irBlockNode.setAllEscape(true);

        irFunctionNode.setBlockNode(irBlockNode);

        irReturnNode = new ReturnNode(internalLocation);

        irBlockNode.addStatementNode(irReturnNode);

        irLoadFieldMemberNode = new LoadFieldMemberNode(internalLocation);
        irLoadFieldMemberNode.setExpressionType(String.class);
        irLoadFieldMemberNode.setName("$SOURCE");
        irLoadFieldMemberNode.setStatic(true);

        irReturnNode.setExpressionNode(irLoadFieldMemberNode);

        irFunctionNode = new FunctionNode(internalLocation);
        irFunctionNode.setName("getStatements");
        irFunctionNode.setReturnType(BitSet.class);
        irFunctionNode.setStatic(false);
        irFunctionNode.setVarArgs(false);
        irFunctionNode.setSynthetic(true);
        irFunctionNode.setMaxLoopCounter(0);

        irClassNode.addFunctionNode(irFunctionNode);

        irBlockNode = new BlockNode(internalLocation);
        irBlockNode.setAllEscape(true);

        irFunctionNode.setBlockNode(irBlockNode);

        irReturnNode = new ReturnNode(internalLocation);

        irBlockNode.addStatementNode(irReturnNode);

        irLoadFieldMemberNode = new LoadFieldMemberNode(internalLocation);
        irLoadFieldMemberNode.setExpressionType(BitSet.class);
        irLoadFieldMemberNode.setName("$STATEMENTS");
        irLoadFieldMemberNode.setStatic(true);

        irReturnNode.setExpressionNode(irLoadFieldMemberNode);
    }

    // convert gets methods to a new set of inserted ir nodes as necessary -
    // requires the gets method name be modified from "getExample" to "example"
    // if a get method variable isn't used it's declaration node is removed from
    // the ir tree permanently so there is no frivolous variable slotting
    protected void injectGetsDeclarations(BlockNode irBlockNode, ScriptScope scriptScope) {
        Location internalLocation = new Location("$internal$ScriptInjectionPhase$injectGetsDeclarations", 0);

        for (int i = 0; i < scriptScope.getScriptClassInfo().getGetMethods().size(); ++i) {
            Method getMethod = scriptScope.getScriptClassInfo().getGetMethods().get(i);
            String name = getMethod.getName().substring(3);
            name = Character.toLowerCase(name.charAt(0)) + name.substring(1);

            if (scriptScope.getUsedVariables().contains(name)) {
                Class<?> returnType = scriptScope.getScriptClassInfo().getGetReturns().get(i);

                DeclarationNode irDeclarationNode = new DeclarationNode(internalLocation);
                irDeclarationNode.setName(name);
                irDeclarationNode.setDeclarationType(returnType);
                irBlockNode.getStatementsNodes().add(0, irDeclarationNode);

                InvokeCallMemberNode irInvokeCallMemberNode = new InvokeCallMemberNode(internalLocation);
                irInvokeCallMemberNode.setExpressionType(irDeclarationNode.getDeclarationType());
                irInvokeCallMemberNode.setLocalFunction(new LocalFunction(
                        getMethod.getName(), returnType, Collections.emptyList(), true, false));
                irDeclarationNode.setExpressionNode(irInvokeCallMemberNode);
            }
        }
    }

    // injects needs methods as defined by ScriptClassInfo
    protected void injectNeedsMethods(ScriptScope scriptScope) {
        Location internalLocation = new Location("$internal$ScriptInjectionPhase$injectNeedsMethods", 0);

        for (org.objectweb.asm.commons.Method needsMethod : scriptScope.getScriptClassInfo().getNeedsMethods()) {
            String name = needsMethod.getName();
            name = name.substring(5);
            name = Character.toLowerCase(name.charAt(0)) + name.substring(1);

            FunctionNode irFunctionNode = new FunctionNode(internalLocation);
            irFunctionNode.setName(needsMethod.getName());
            irFunctionNode.setReturnType(boolean.class);
            irFunctionNode.setStatic(false);
            irFunctionNode.setVarArgs(false);
            irFunctionNode.setSynthetic(true);
            irFunctionNode.setMaxLoopCounter(0);

            irClassNode.addFunctionNode(irFunctionNode);

            BlockNode irBlockNode = new BlockNode(internalLocation);
            irBlockNode.setAllEscape(true);

            irFunctionNode.setBlockNode(irBlockNode);

            ReturnNode irReturnNode = new ReturnNode(internalLocation);

            irBlockNode.addStatementNode(irReturnNode);

            ConstantNode irConstantNode = new ConstantNode(internalLocation);
            irConstantNode.setExpressionType(boolean.class);
            irConstantNode.setConstant(scriptScope.getUsedVariables().contains(name));

            irReturnNode.setExpressionNode(irConstantNode);
        }
    }

    // decorate the execute method with nodes to wrap the user statements with
    // the sandboxed errors as follows:
    // } catch (PainlessExplainError e) {
    //     throw this.convertToScriptException(e, e.getHeaders($DEFINITION))
    // }
    // and
    // } catch (PainlessError | BootstrapMethodError | OutOfMemoryError | StackOverflowError | Exception e) {
    //     throw this.convertToScriptException(e, e.getHeaders())
    // }
    protected void injectSandboxExceptions(FunctionNode irFunctionNode) {
        try {
            Location internalLocation = new Location("$internal$ScriptInjectionPhase$injectSandboxExceptions", 0);
            BlockNode irBlockNode = irFunctionNode.getBlockNode();

            TryNode irTryNode = new TryNode(internalLocation);
            irTryNode.setBlockNode(irBlockNode);

            CatchNode irCatchNode = new CatchNode(internalLocation);
            irCatchNode.setExceptionType(PainlessExplainError.class);
            irCatchNode.setSymbol("#painlessExplainError");

            irTryNode.addCatchNode(irCatchNode);

            BlockNode irCatchBlockNode = new BlockNode(internalLocation);
            irCatchBlockNode.setAllEscape(true);

            irCatchNode.setBlockNode(irCatchBlockNode);

            ThrowNode irThrowNode = new ThrowNode(internalLocation);

            irCatchBlockNode.addStatementNode(irThrowNode);

            InvokeCallMemberNode irInvokeCallMemberNode = new InvokeCallMemberNode(internalLocation);
            irInvokeCallMemberNode.setExpressionType(ScriptException.class);
            irInvokeCallMemberNode.setLocalFunction(
                    new LocalFunction(
                            "convertToScriptException",
                            ScriptException.class,
                            Arrays.asList(Throwable.class, Map.class),
                            true,
                            false
                    )
            );

            irThrowNode.setExpressionNode(irInvokeCallMemberNode);

            LoadVariableNode irLoadVariableNode = new LoadVariableNode(internalLocation);
            irLoadVariableNode.setExpressionType(ScriptException.class);
            irLoadVariableNode.setName("#painlessExplainError");

            irInvokeCallMemberNode.addArgumentNode(irLoadVariableNode);

            BinaryImplNode irBinaryImplNode = new BinaryImplNode(internalLocation);
            irBinaryImplNode.setExpressionType(Map.class);

            irInvokeCallMemberNode.addArgumentNode(irBinaryImplNode);

            irLoadVariableNode = new LoadVariableNode(internalLocation);
            irLoadVariableNode.setExpressionType(PainlessExplainError.class);
            irLoadVariableNode.setName("#painlessExplainError");

            irBinaryImplNode.setLeftNode(irLoadVariableNode);

            InvokeCallNode irInvokeCallNode = new InvokeCallNode(internalLocation);
            irInvokeCallNode.setExpressionType(Map.class);
            irInvokeCallNode.setBox(PainlessExplainError.class);
            irInvokeCallNode.setMethod(
                    new PainlessMethod(
                            PainlessExplainError.class.getMethod(
                                    "getHeaders",
                                    PainlessLookup.class),
                            PainlessExplainError.class,
                            null,
                            Collections.emptyList(),
                            null,
                            null,
                            null
                    )
            );

            irBinaryImplNode.setRightNode(irInvokeCallNode);

            LoadFieldMemberNode irLoadFieldMemberNode = new LoadFieldMemberNode(internalLocation);
            irLoadFieldMemberNode.setExpressionType(PainlessLookup.class);
            irLoadFieldMemberNode.setName("$DEFINITION");
            irLoadFieldMemberNode.setStatic(true);

            irInvokeCallNode.addArgumentNode(irLoadFieldMemberNode);

            for (Class<?> throwable : new Class<?>[] {
                    PainlessError.class, BootstrapMethodError.class, OutOfMemoryError.class, StackOverflowError.class, Exception.class}) {

                String name = throwable.getSimpleName();
                name = "#" + Character.toLowerCase(name.charAt(0)) + name.substring(1);

                irCatchNode = new CatchNode(internalLocation);
                irCatchNode.setExceptionType(throwable);
                irCatchNode.setSymbol(name);

                irTryNode.addCatchNode(irCatchNode);

                irCatchBlockNode = new BlockNode(internalLocation);
                irCatchBlockNode.setAllEscape(true);

                irCatchNode.setBlockNode(irCatchBlockNode);

                irThrowNode = new ThrowNode(internalLocation);

                irCatchBlockNode.addStatementNode(irThrowNode);

                irInvokeCallMemberNode = new InvokeCallMemberNode(internalLocation);
                irInvokeCallMemberNode.setExpressionType(ScriptException.class);
                irInvokeCallMemberNode.setLocalFunction(
                        new LocalFunction(
                                "convertToScriptException",
                                ScriptException.class,
                                Arrays.asList(Throwable.class, Map.class),
                                true,
                                false
                        )
                );

                irThrowNode.setExpressionNode(irInvokeCallMemberNode);

                irLoadVariableNode = new LoadVariableNode(internalLocation);
                irLoadVariableNode.setExpressionType(ScriptException.class);
                irLoadVariableNode.setName(name);

                irInvokeCallMemberNode.addArgumentNode(irLoadVariableNode);

                irBinaryImplNode = new BinaryImplNode(internalLocation);
                irBinaryImplNode.setExpressionType(Map.class);

                irInvokeCallMemberNode.addArgumentNode(irBinaryImplNode);

                StaticNode irStaticNode = new StaticNode(internalLocation);
                irStaticNode.setExpressionType(Collections.class);

                irBinaryImplNode.setLeftNode(irStaticNode);

                irInvokeCallNode = new InvokeCallNode(internalLocation);
                irInvokeCallNode.setExpressionType(Map.class);
                irInvokeCallNode.setBox(Collections.class);
                irInvokeCallNode.setMethod(
                        new PainlessMethod(
                                Collections.class.getMethod("emptyMap"),
                                Collections.class,
                                null,
                                Collections.emptyList(),
                                null,
                                null,
                                null
                        )
                );

                irBinaryImplNode.setRightNode(irInvokeCallNode);
            }

            irBlockNode = new BlockNode(internalLocation);
            irBlockNode.setAllEscape(true);
            irBlockNode.addStatementNode(irTryNode);

            irFunctionNode.setBlockNode(irBlockNode);
        } catch (Exception exception) {
            throw new RuntimeException(exception);
        }
    }

    @Override
    public void visitExpression(SExpression userExpressionNode, ScriptScope scriptScope) {
        // sets IRNodeDecoration with ReturnNode or StatementExpressionNode
        super.visitExpression(userExpressionNode, scriptScope);
        injectConverter(userExpressionNode, scriptScope);
    }

    @Override
    public void visitReturn(SReturn userReturnNode, ScriptScope scriptScope) {
        super.visitReturn(userReturnNode, scriptScope);
        injectConverter(userReturnNode, scriptScope);
    }

    public void injectConverter(AStatement userStatementNode, ScriptScope scriptScope) {
        Converter converter = scriptScope.getDecoration(userStatementNode, Converter.class);
        if (converter == null) {
            return;
        }

        IRNodeDecoration irNodeDecoration = scriptScope.getDecoration(userStatementNode, IRNodeDecoration.class);
        IRNode irNode = irNodeDecoration.getIRNode();

        if ((irNode instanceof ReturnNode) == false) {
            // Shouldn't have a Converter decoration if StatementExpressionNode, should be ReturnNode if explicit return
            throw userStatementNode.createError(new IllegalStateException("illegal tree structure"));
        }

        ReturnNode returnNode = (ReturnNode) irNode;

        // inject converter
        InvokeCallMemberNode irInvokeCallMemberNode = new InvokeCallMemberNode(userStatementNode.getLocation());
        irInvokeCallMemberNode.setLocalFunction(converter.getConverter());
        ExpressionNode returnExpression = returnNode.getExpressionNode();
        returnNode.setExpressionNode(irInvokeCallMemberNode);
        irInvokeCallMemberNode.addArgumentNode(returnExpression);

    }
}

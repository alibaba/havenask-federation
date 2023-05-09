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

import org.havenask.painless.DefBootstrap;
import org.havenask.painless.Location;
import org.havenask.painless.MethodWriter;
import org.havenask.painless.Operation;
import org.havenask.painless.WriterConstants;
import org.havenask.painless.ir.BinaryImplNode;
import org.havenask.painless.ir.BinaryMathNode;
import org.havenask.painless.ir.BlockNode;
import org.havenask.painless.ir.BooleanNode;
import org.havenask.painless.ir.BreakNode;
import org.havenask.painless.ir.CastNode;
import org.havenask.painless.ir.CatchNode;
import org.havenask.painless.ir.ClassNode;
import org.havenask.painless.ir.ComparisonNode;
import org.havenask.painless.ir.ConditionNode;
import org.havenask.painless.ir.ConditionalNode;
import org.havenask.painless.ir.ConstantNode;
import org.havenask.painless.ir.ContinueNode;
import org.havenask.painless.ir.DeclarationBlockNode;
import org.havenask.painless.ir.DeclarationNode;
import org.havenask.painless.ir.DefInterfaceReferenceNode;
import org.havenask.painless.ir.DoWhileLoopNode;
import org.havenask.painless.ir.DupNode;
import org.havenask.painless.ir.ElvisNode;
import org.havenask.painless.ir.ExpressionNode;
import org.havenask.painless.ir.FieldNode;
import org.havenask.painless.ir.FlipArrayIndexNode;
import org.havenask.painless.ir.FlipCollectionIndexNode;
import org.havenask.painless.ir.FlipDefIndexNode;
import org.havenask.painless.ir.ForEachLoopNode;
import org.havenask.painless.ir.ForEachSubArrayNode;
import org.havenask.painless.ir.ForEachSubIterableNode;
import org.havenask.painless.ir.ForLoopNode;
import org.havenask.painless.ir.FunctionNode;
import org.havenask.painless.ir.IRNode;
import org.havenask.painless.ir.IfElseNode;
import org.havenask.painless.ir.IfNode;
import org.havenask.painless.ir.InstanceofNode;
import org.havenask.painless.ir.InvokeCallDefNode;
import org.havenask.painless.ir.InvokeCallMemberNode;
import org.havenask.painless.ir.InvokeCallNode;
import org.havenask.painless.ir.ListInitializationNode;
import org.havenask.painless.ir.LoadBraceDefNode;
import org.havenask.painless.ir.LoadBraceNode;
import org.havenask.painless.ir.LoadDotArrayLengthNode;
import org.havenask.painless.ir.LoadDotDefNode;
import org.havenask.painless.ir.LoadDotNode;
import org.havenask.painless.ir.LoadDotShortcutNode;
import org.havenask.painless.ir.LoadFieldMemberNode;
import org.havenask.painless.ir.LoadListShortcutNode;
import org.havenask.painless.ir.LoadMapShortcutNode;
import org.havenask.painless.ir.LoadVariableNode;
import org.havenask.painless.ir.MapInitializationNode;
import org.havenask.painless.ir.NewArrayNode;
import org.havenask.painless.ir.NewObjectNode;
import org.havenask.painless.ir.NullNode;
import org.havenask.painless.ir.NullSafeSubNode;
import org.havenask.painless.ir.ReferenceNode;
import org.havenask.painless.ir.ReturnNode;
import org.havenask.painless.ir.StatementExpressionNode;
import org.havenask.painless.ir.StatementNode;
import org.havenask.painless.ir.StaticNode;
import org.havenask.painless.ir.StoreBraceDefNode;
import org.havenask.painless.ir.StoreBraceNode;
import org.havenask.painless.ir.StoreDotDefNode;
import org.havenask.painless.ir.StoreDotNode;
import org.havenask.painless.ir.StoreDotShortcutNode;
import org.havenask.painless.ir.StoreFieldMemberNode;
import org.havenask.painless.ir.StoreListShortcutNode;
import org.havenask.painless.ir.StoreMapShortcutNode;
import org.havenask.painless.ir.StoreNode;
import org.havenask.painless.ir.StoreVariableNode;
import org.havenask.painless.ir.StringConcatenationNode;
import org.havenask.painless.ir.ThrowNode;
import org.havenask.painless.ir.TryNode;
import org.havenask.painless.ir.TypedCaptureReferenceNode;
import org.havenask.painless.ir.TypedInterfaceReferenceNode;
import org.havenask.painless.ir.UnaryMathNode;
import org.havenask.painless.ir.WhileLoopNode;
import org.havenask.painless.lookup.PainlessCast;
import org.havenask.painless.lookup.PainlessClassBinding;
import org.havenask.painless.lookup.PainlessField;
import org.havenask.painless.lookup.PainlessInstanceBinding;
import org.havenask.painless.lookup.PainlessLookup;
import org.havenask.painless.lookup.PainlessLookupUtility;
import org.havenask.painless.lookup.PainlessMethod;
import org.havenask.painless.lookup.def;
import org.havenask.painless.node.AExpression;
import org.havenask.painless.node.ANode;
import org.havenask.painless.node.AStatement;
import org.havenask.painless.node.EAssignment;
import org.havenask.painless.node.EBinary;
import org.havenask.painless.node.EBooleanComp;
import org.havenask.painless.node.EBooleanConstant;
import org.havenask.painless.node.EBrace;
import org.havenask.painless.node.ECall;
import org.havenask.painless.node.ECallLocal;
import org.havenask.painless.node.EComp;
import org.havenask.painless.node.EConditional;
import org.havenask.painless.node.EDecimal;
import org.havenask.painless.node.EDot;
import org.havenask.painless.node.EElvis;
import org.havenask.painless.node.EExplicit;
import org.havenask.painless.node.EFunctionRef;
import org.havenask.painless.node.EInstanceof;
import org.havenask.painless.node.ELambda;
import org.havenask.painless.node.EListInit;
import org.havenask.painless.node.EMapInit;
import org.havenask.painless.node.ENewArray;
import org.havenask.painless.node.ENewArrayFunctionRef;
import org.havenask.painless.node.ENewObj;
import org.havenask.painless.node.ENull;
import org.havenask.painless.node.ENumeric;
import org.havenask.painless.node.ERegex;
import org.havenask.painless.node.EString;
import org.havenask.painless.node.ESymbol;
import org.havenask.painless.node.EUnary;
import org.havenask.painless.node.SBlock;
import org.havenask.painless.node.SBreak;
import org.havenask.painless.node.SCatch;
import org.havenask.painless.node.SClass;
import org.havenask.painless.node.SContinue;
import org.havenask.painless.node.SDeclBlock;
import org.havenask.painless.node.SDeclaration;
import org.havenask.painless.node.SDo;
import org.havenask.painless.node.SEach;
import org.havenask.painless.node.SExpression;
import org.havenask.painless.node.SFor;
import org.havenask.painless.node.SFunction;
import org.havenask.painless.node.SIf;
import org.havenask.painless.node.SIfElse;
import org.havenask.painless.node.SReturn;
import org.havenask.painless.node.SThrow;
import org.havenask.painless.node.STry;
import org.havenask.painless.node.SWhile;
import org.havenask.painless.symbol.Decorations.AccessDepth;
import org.havenask.painless.symbol.Decorations.AllEscape;
import org.havenask.painless.symbol.Decorations.BinaryType;
import org.havenask.painless.symbol.Decorations.CapturesDecoration;
import org.havenask.painless.symbol.Decorations.ComparisonType;
import org.havenask.painless.symbol.Decorations.Compound;
import org.havenask.painless.symbol.Decorations.CompoundType;
import org.havenask.painless.symbol.Decorations.ContinuousLoop;
import org.havenask.painless.symbol.Decorations.DowncastPainlessCast;
import org.havenask.painless.symbol.Decorations.EncodingDecoration;
import org.havenask.painless.symbol.Decorations.Explicit;
import org.havenask.painless.symbol.Decorations.ExpressionPainlessCast;
import org.havenask.painless.symbol.Decorations.GetterPainlessMethod;
import org.havenask.painless.symbol.Decorations.IRNodeDecoration;
import org.havenask.painless.symbol.Decorations.InstanceType;
import org.havenask.painless.symbol.Decorations.IterablePainlessMethod;
import org.havenask.painless.symbol.Decorations.ListShortcut;
import org.havenask.painless.symbol.Decorations.MapShortcut;
import org.havenask.painless.symbol.Decorations.MethodEscape;
import org.havenask.painless.symbol.Decorations.MethodNameDecoration;
import org.havenask.painless.symbol.Decorations.Negate;
import org.havenask.painless.symbol.Decorations.ParameterNames;
import org.havenask.painless.symbol.Decorations.Read;
import org.havenask.painless.symbol.Decorations.ReferenceDecoration;
import org.havenask.painless.symbol.Decorations.ReturnType;
import org.havenask.painless.symbol.Decorations.SemanticVariable;
import org.havenask.painless.symbol.Decorations.SetterPainlessMethod;
import org.havenask.painless.symbol.Decorations.ShiftType;
import org.havenask.painless.symbol.Decorations.Shortcut;
import org.havenask.painless.symbol.Decorations.StandardConstant;
import org.havenask.painless.symbol.Decorations.StandardLocalFunction;
import org.havenask.painless.symbol.Decorations.StandardPainlessClassBinding;
import org.havenask.painless.symbol.Decorations.StandardPainlessConstructor;
import org.havenask.painless.symbol.Decorations.StandardPainlessField;
import org.havenask.painless.symbol.Decorations.StandardPainlessInstanceBinding;
import org.havenask.painless.symbol.Decorations.StandardPainlessMethod;
import org.havenask.painless.symbol.Decorations.StaticType;
import org.havenask.painless.symbol.Decorations.TargetType;
import org.havenask.painless.symbol.Decorations.TypeParameters;
import org.havenask.painless.symbol.Decorations.UnaryType;
import org.havenask.painless.symbol.Decorations.UpcastPainlessCast;
import org.havenask.painless.symbol.Decorations.ValueType;
import org.havenask.painless.symbol.Decorations.Write;
import org.havenask.painless.symbol.FunctionTable;
import org.havenask.painless.symbol.FunctionTable.LocalFunction;
import org.havenask.painless.symbol.ScriptScope;
import org.havenask.painless.symbol.SemanticScope.Variable;
import org.objectweb.asm.Opcodes;

import java.lang.invoke.CallSite;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.invoke.MethodType;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class DefaultUserTreeToIRTreePhase implements UserTreeVisitor<ScriptScope> {

    protected ClassNode irClassNode;

    /**
     * This injects additional ir nodes required for resolving the def type at runtime.
     * This includes injection of ir nodes to add a function to call
     * {@link DefBootstrap#bootstrap(PainlessLookup, FunctionTable, Map, Lookup, String, MethodType, int, int, Object...)}
     * to do the runtime resolution, and several supporting static fields.
     */
    protected void injectBootstrapMethod(ScriptScope scriptScope) {
        // adds static fields required for def bootstrapping
        Location internalLocation = new Location("$internal$injectStaticFields", 0);
        int modifiers = Opcodes.ACC_PUBLIC | Opcodes.ACC_STATIC;

        FieldNode irFieldNode = new FieldNode(internalLocation);
        irFieldNode.setModifiers(modifiers);
        irFieldNode.setFieldType(PainlessLookup.class);
        irFieldNode.setName("$DEFINITION");

        irClassNode.addFieldNode(irFieldNode);

        irFieldNode = new FieldNode(internalLocation);
        irFieldNode.setModifiers(modifiers);
        irFieldNode.setFieldType(FunctionTable.class);
        irFieldNode.setName("$FUNCTIONS");

        irClassNode.addFieldNode(irFieldNode);

        irFieldNode = new FieldNode(internalLocation);
        irFieldNode.setModifiers(modifiers);
        irFieldNode.setFieldType(Map.class);
        irFieldNode.setName("$COMPILERSETTINGS");

        irClassNode.addFieldNode(irFieldNode);

        // adds the bootstrap method required for dynamic binding for def type resolution
        internalLocation = new Location("$internal$injectDefBootstrapMethod", 0);

        try {
            FunctionNode irFunctionNode = new FunctionNode(internalLocation);
            irFunctionNode.setReturnType(CallSite.class);
            irFunctionNode.setName("$bootstrapDef");
            irFunctionNode.getTypeParameters().addAll(
                    Arrays.asList(Lookup.class, String.class, MethodType.class, int.class, int.class, Object[].class));
            irFunctionNode.getParameterNames().addAll(
                    Arrays.asList("methodHandlesLookup", "name", "type", "initialDepth", "flavor", "args"));
            irFunctionNode.setStatic(true);
            irFunctionNode.setVarArgs(true);
            irFunctionNode.setSynthetic(true);
            irFunctionNode.setMaxLoopCounter(0);

            irClassNode.addFunctionNode(irFunctionNode);

            BlockNode blockNode = new BlockNode(internalLocation);
            blockNode.setAllEscape(true);

            irFunctionNode.setBlockNode(blockNode);

            ReturnNode returnNode = new ReturnNode(internalLocation);

            blockNode.addStatementNode(returnNode);

            BinaryImplNode irBinaryImplNode = new BinaryImplNode(internalLocation);
            irBinaryImplNode.setExpressionType(CallSite.class);

            returnNode.setExpressionNode(irBinaryImplNode);

            StaticNode staticNode = new StaticNode(internalLocation);
            staticNode.setExpressionType(DefBootstrap.class);

            irBinaryImplNode.setLeftNode(staticNode);

            InvokeCallNode invokeCallNode = new InvokeCallNode(internalLocation);
            invokeCallNode.setExpressionType(CallSite.class);
            invokeCallNode.setMethod(new PainlessMethod(
                            DefBootstrap.class.getMethod("bootstrap",
                                    PainlessLookup.class,
                                    FunctionTable.class,
                                    Map.class,
                                    Lookup.class,
                                    String.class,
                                    MethodType.class,
                                    int.class,
                                    int.class,
                                    Object[].class),
                            DefBootstrap.class,
                            CallSite.class,
                            Arrays.asList(
                                    PainlessLookup.class,
                                    FunctionTable.class,
                                    Map.class,
                                    Lookup.class,
                                    String.class,
                                    MethodType.class,
                                    int.class,
                                    int.class,
                                    Object[].class),
                            null,
                            null,
                            null
                    )
            );
            invokeCallNode.setBox(DefBootstrap.class);

            irBinaryImplNode.setRightNode(invokeCallNode);

            LoadFieldMemberNode irLoadFieldMemberNode = new LoadFieldMemberNode(internalLocation);
            irLoadFieldMemberNode.setExpressionType(PainlessLookup.class);
            irLoadFieldMemberNode.setName("$DEFINITION");
            irLoadFieldMemberNode.setStatic(true);

            invokeCallNode.addArgumentNode(irLoadFieldMemberNode);

            irLoadFieldMemberNode = new LoadFieldMemberNode(internalLocation);
            irLoadFieldMemberNode.setExpressionType(FunctionTable.class);
            irLoadFieldMemberNode.setName("$FUNCTIONS");
            irLoadFieldMemberNode.setStatic(true);

            invokeCallNode.addArgumentNode(irLoadFieldMemberNode);

            irLoadFieldMemberNode = new LoadFieldMemberNode(internalLocation);
            irLoadFieldMemberNode.setExpressionType(Map.class);
            irLoadFieldMemberNode.setName("$COMPILERSETTINGS");
            irLoadFieldMemberNode.setStatic(true);

            invokeCallNode.addArgumentNode(irLoadFieldMemberNode);

            LoadVariableNode irLoadVariableNode = new LoadVariableNode(internalLocation);
            irLoadVariableNode.setExpressionType(Lookup.class);
            irLoadVariableNode.setName("methodHandlesLookup");

            invokeCallNode.addArgumentNode(irLoadVariableNode);

            irLoadVariableNode = new LoadVariableNode(internalLocation);
            irLoadVariableNode.setExpressionType(String.class);
            irLoadVariableNode.setName("name");

            invokeCallNode.addArgumentNode(irLoadVariableNode);

            irLoadVariableNode = new LoadVariableNode(internalLocation);
            irLoadVariableNode.setExpressionType(MethodType.class);
            irLoadVariableNode.setName("type");

            invokeCallNode.addArgumentNode(irLoadVariableNode);

            irLoadVariableNode = new LoadVariableNode(internalLocation);
            irLoadVariableNode.setExpressionType(int.class);
            irLoadVariableNode.setName("initialDepth");

            invokeCallNode.addArgumentNode(irLoadVariableNode);

            irLoadVariableNode = new LoadVariableNode(internalLocation);
            irLoadVariableNode.setExpressionType(int.class);
            irLoadVariableNode.setName("flavor");

            invokeCallNode.addArgumentNode(irLoadVariableNode);

            irLoadVariableNode = new LoadVariableNode(internalLocation);
            irLoadVariableNode.setExpressionType(Object[].class);
            irLoadVariableNode.setName("args");

            invokeCallNode.addArgumentNode(irLoadVariableNode);
        } catch (Exception exception) {
            throw new IllegalStateException(exception);
        }
    }

    protected ExpressionNode injectCast(AExpression userExpressionNode, ScriptScope scriptScope) {
        ExpressionNode irExpressionNode = (ExpressionNode)visit(userExpressionNode, scriptScope);

        if (irExpressionNode == null) {
            return null;
        }

        ExpressionPainlessCast expressionPainlessCast = scriptScope.getDecoration(userExpressionNode, ExpressionPainlessCast.class);

        if (expressionPainlessCast == null) {
            return irExpressionNode;
        }

        PainlessCast painlessCast = expressionPainlessCast.getExpressionPainlessCast();
        Class<?> targetType = painlessCast.targetType;

        if (painlessCast.boxTargetType != null) {
            targetType = PainlessLookupUtility.typeToBoxedType(painlessCast.boxTargetType);
        } else if (painlessCast.unboxTargetType != null) {
            targetType = painlessCast.unboxTargetType;
        }

        CastNode irCastNode = new CastNode(irExpressionNode.getLocation());
        irCastNode.setExpressionType(targetType);
        irCastNode.setCast(painlessCast);
        irCastNode.setChildNode(irExpressionNode);

        return irCastNode;
    }

    /**
     * This helper generates a set of ir nodes that are required for an assignment
     * handling both regular assignment and compound assignment. It only stubs out
     * the compound assignment.
     * @param accessDepth The number of arguments to dup for an additional read.
     * @param location The location for errors.
     * @param isNullSafe Whether or not the null safe operator is used.
     * @param irPrefixNode The prefix node for this store/load. The 'a.b' of 'a.b.c', etc.
     * @param irIndexNode The index node if this is a brace access.
     * @param irLoadNode The load node if this a read.
     * @param irStoreNode The store node if this is a write.
     * @return The root node for this assignment.
     */
    protected ExpressionNode buildLoadStore(int accessDepth, Location location, boolean isNullSafe,
            ExpressionNode irPrefixNode, ExpressionNode irIndexNode, ExpressionNode irLoadNode, StoreNode irStoreNode) {

        // build out the load structure for load/compound assignment or the store structure for just store
        ExpressionNode irExpressionNode = irLoadNode != null ? irLoadNode : irStoreNode;

        if (irPrefixNode != null) {
            // this load/store is a dot or brace load/store

            if (irIndexNode != null) {
                // this load/store requires an index
                BinaryImplNode binaryImplNode = new BinaryImplNode(location);

                if (isNullSafe) {
                    // the null-safe structure is slightly different from the standard structure since
                    // both the index and expression are not written to the stack if the prefix is null
                    binaryImplNode.setExpressionType(irExpressionNode.getExpressionType());
                    binaryImplNode.setLeftNode(irIndexNode);
                    binaryImplNode.setRightNode(irExpressionNode);
                    irExpressionNode = binaryImplNode;
                } else {
                    binaryImplNode.setExpressionType(void.class);
                    binaryImplNode.setLeftNode(irPrefixNode);
                    binaryImplNode.setRightNode(irIndexNode);
                    irPrefixNode = binaryImplNode;
                }
            }

            if (irLoadNode != null && irStoreNode != null) {
                // this is a compound assignment and requires and additional dup to re-access the prefix
                DupNode dupNode = new DupNode(location);
                dupNode.setExpressionType(void.class);
                dupNode.setSize(accessDepth);
                dupNode.setDepth(0);
                dupNode.setChildNode(irPrefixNode);
                irPrefixNode = dupNode;
            }

            // build the structure to combine the prefix and the load/store
            BinaryImplNode binaryImplNode = new BinaryImplNode(location);
            binaryImplNode.setExpressionType(irExpressionNode.getExpressionType());

            if (isNullSafe) {
                // build the structure for a null safe load
                NullSafeSubNode irNullSafeSubNode = new NullSafeSubNode(location);
                irNullSafeSubNode.setExpressionType(irExpressionNode.getExpressionType());
                irNullSafeSubNode.setChildNode(irExpressionNode);
                binaryImplNode.setLeftNode(irPrefixNode);
                binaryImplNode.setRightNode(irNullSafeSubNode);
            } else {
                // build the structure for a standard load/store
                binaryImplNode.setLeftNode(irPrefixNode);
                binaryImplNode.setRightNode(irExpressionNode);
            }

            irExpressionNode = binaryImplNode;
        }

        if (irLoadNode != null && irStoreNode != null) {
            // this is a compound assignment and the store is the root
            irStoreNode.setChildNode(irExpressionNode);
            irExpressionNode = irStoreNode;
        }

        return irExpressionNode;
    }

    protected IRNode visit(ANode userNode, ScriptScope scriptScope) {
        if (userNode == null) {
            return null;
        } else {
            userNode.visit(this, scriptScope);
            return scriptScope.getDecoration(userNode, IRNodeDecoration.class).getIRNode();
        }
    }

    @Override
    public void visitClass(SClass userClassNode, ScriptScope scriptScope) {
        irClassNode = new ClassNode(userClassNode.getLocation());

        for (SFunction userFunctionNode : userClassNode.getFunctionNodes()) {
            irClassNode.addFunctionNode((FunctionNode)visit(userFunctionNode, scriptScope));
        }

        irClassNode.setScriptScope(scriptScope);

        injectBootstrapMethod(scriptScope);
        scriptScope.putDecoration(userClassNode, new IRNodeDecoration(irClassNode));
    }

    @Override
    public void visitFunction(SFunction userFunctionNode, ScriptScope scriptScope) {
        String functionName = userFunctionNode.getFunctionName();
        int functionArity = userFunctionNode.getCanonicalTypeNameParameters().size();
        LocalFunction localFunction = scriptScope.getFunctionTable().getFunction(functionName, functionArity);
        Class<?> returnType = localFunction.getReturnType();
        boolean methodEscape = scriptScope.getCondition(userFunctionNode, MethodEscape.class);

        BlockNode irBlockNode = (BlockNode)visit(userFunctionNode.getBlockNode(), scriptScope);

        if (methodEscape == false) {
            ExpressionNode irExpressionNode;

            if (returnType == void.class) {
                irExpressionNode = null;
            } else if (userFunctionNode.isAutoReturnEnabled()) {
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
            } else {
                throw userFunctionNode.createError(new IllegalStateException("illegal tree structure"));
            }

            ReturnNode irReturnNode = new ReturnNode(userFunctionNode.getLocation());
            irReturnNode.setExpressionNode(irExpressionNode);

            irBlockNode.addStatementNode(irReturnNode);
        }

        FunctionNode irFunctionNode = new FunctionNode(userFunctionNode.getLocation());
        irFunctionNode.setBlockNode(irBlockNode);
        irFunctionNode.setName(userFunctionNode.getFunctionName());
        irFunctionNode.setReturnType(returnType);
        irFunctionNode.getTypeParameters().addAll(localFunction.getTypeParameters());
        irFunctionNode.getParameterNames().addAll(userFunctionNode.getParameterNames());
        irFunctionNode.setStatic(userFunctionNode.isStatic());
        irFunctionNode.setVarArgs(false);
        irFunctionNode.setSynthetic(userFunctionNode.isSynthetic());
        irFunctionNode.setMaxLoopCounter(scriptScope.getCompilerSettings().getMaxLoopCounter());

        scriptScope.putDecoration(userFunctionNode, new IRNodeDecoration(irFunctionNode));
    }

    @Override
    public void visitBlock(SBlock userBlockNode, ScriptScope scriptScope) {
        BlockNode irBlockNode = new BlockNode(userBlockNode.getLocation());

        for (AStatement userStatementNode : userBlockNode.getStatementNodes()) {
            irBlockNode.addStatementNode((StatementNode)visit(userStatementNode, scriptScope));
        }

        irBlockNode.setAllEscape(scriptScope.getCondition(userBlockNode, AllEscape.class));

        scriptScope.putDecoration(userBlockNode, new IRNodeDecoration(irBlockNode));
    }

    @Override
    public void visitIf(SIf userIfNode, ScriptScope scriptScope) {
        IfNode irIfNode = new IfNode(userIfNode.getLocation());
        irIfNode.setConditionNode(injectCast(userIfNode.getConditionNode(), scriptScope));
        irIfNode.setBlockNode((BlockNode)visit(userIfNode.getIfBlockNode(), scriptScope));

        scriptScope.putDecoration(userIfNode, new IRNodeDecoration(irIfNode));
    }

    @Override
    public void visitIfElse(SIfElse userIfElseNode, ScriptScope scriptScope) {
        IfElseNode irIfElseNode = new IfElseNode(userIfElseNode.getLocation());
        irIfElseNode.setConditionNode(injectCast(userIfElseNode.getConditionNode(), scriptScope));
        irIfElseNode.setBlockNode((BlockNode)visit(userIfElseNode.getIfBlockNode(), scriptScope));
        irIfElseNode.setElseBlockNode((BlockNode)visit(userIfElseNode.getElseBlockNode(), scriptScope));

        scriptScope.putDecoration(userIfElseNode, new IRNodeDecoration(irIfElseNode));
    }

    @Override
    public void visitWhile(SWhile userWhileNode, ScriptScope scriptScope) {
        WhileLoopNode irWhileLoopNode = new WhileLoopNode(userWhileNode.getLocation());
        irWhileLoopNode.setConditionNode(injectCast(userWhileNode.getConditionNode(), scriptScope));
        irWhileLoopNode.setBlockNode((BlockNode)visit(userWhileNode.getBlockNode(), scriptScope));
        irWhileLoopNode.setContinuous(scriptScope.getCondition(userWhileNode, ContinuousLoop.class));

        scriptScope.putDecoration(userWhileNode, new IRNodeDecoration(irWhileLoopNode));
    }

    @Override
    public void visitDo(SDo userDoNode, ScriptScope scriptScope) {
        DoWhileLoopNode irDoWhileLoopNode = new DoWhileLoopNode(userDoNode.getLocation());
        irDoWhileLoopNode.setConditionNode(injectCast(userDoNode.getConditionNode(), scriptScope));
        irDoWhileLoopNode.setBlockNode((BlockNode)visit(userDoNode.getBlockNode(), scriptScope));
        irDoWhileLoopNode.setContinuous(scriptScope.getCondition(userDoNode, ContinuousLoop.class));

        scriptScope.putDecoration(userDoNode, new IRNodeDecoration(irDoWhileLoopNode));
    }

    @Override
    public void visitFor(SFor userForNode, ScriptScope scriptScope) {
        ForLoopNode irForLoopNode = new ForLoopNode(userForNode.getLocation());
        irForLoopNode.setInitialzerNode(visit(userForNode.getInitializerNode(), scriptScope));
        irForLoopNode.setConditionNode(injectCast(userForNode.getConditionNode(), scriptScope));
        irForLoopNode.setAfterthoughtNode((ExpressionNode)visit(userForNode.getAfterthoughtNode(), scriptScope));
        irForLoopNode.setBlockNode((BlockNode)visit(userForNode.getBlockNode(), scriptScope));
        irForLoopNode.setContinuous(scriptScope.getCondition(userForNode, ContinuousLoop.class));

        scriptScope.putDecoration(userForNode, new IRNodeDecoration(irForLoopNode));
    }

    @Override
    public void visitEach(SEach userEachNode, ScriptScope scriptScope) {
        Variable variable = scriptScope.getDecoration(userEachNode, SemanticVariable.class).getSemanticVariable();
        PainlessCast painlessCast = scriptScope.hasDecoration(userEachNode, ExpressionPainlessCast.class) ?
                scriptScope.getDecoration(userEachNode, ExpressionPainlessCast.class).getExpressionPainlessCast() : null;
        ExpressionNode irIterableNode = (ExpressionNode)visit(userEachNode.getIterableNode(), scriptScope);
        Class<?> iterableValueType = scriptScope.getDecoration(userEachNode.getIterableNode(), ValueType.class).getValueType();
        BlockNode irBlockNode = (BlockNode)visit(userEachNode.getBlockNode(), scriptScope);

        ConditionNode irConditionNode;

        if (iterableValueType.isArray()) {
            ForEachSubArrayNode irForEachSubArrayNode = new ForEachSubArrayNode(userEachNode.getLocation());
            irForEachSubArrayNode.setConditionNode(irIterableNode);
            irForEachSubArrayNode.setBlockNode(irBlockNode);
            irForEachSubArrayNode.setVariableType(variable.getType());
            irForEachSubArrayNode.setVariableName(variable.getName());
            irForEachSubArrayNode.setCast(painlessCast);
            irForEachSubArrayNode.setArrayType(iterableValueType);
            irForEachSubArrayNode.setArrayName("#array" + userEachNode.getLocation().getOffset());
            irForEachSubArrayNode.setIndexType(int.class);
            irForEachSubArrayNode.setIndexName("#index" + userEachNode.getLocation().getOffset());
            irForEachSubArrayNode.setIndexedType(iterableValueType.getComponentType());
            irForEachSubArrayNode.setContinuous(false);
            irConditionNode = irForEachSubArrayNode;
        } else if (iterableValueType == def.class || Iterable.class.isAssignableFrom(iterableValueType)) {
            ForEachSubIterableNode irForEachSubIterableNode = new ForEachSubIterableNode(userEachNode.getLocation());
            irForEachSubIterableNode.setConditionNode(irIterableNode);
            irForEachSubIterableNode.setBlockNode(irBlockNode);
            irForEachSubIterableNode.setVariableType(variable.getType());
            irForEachSubIterableNode.setVariableName(variable.getName());
            irForEachSubIterableNode.setCast(painlessCast);
            irForEachSubIterableNode.setIteratorType(Iterator.class);
            irForEachSubIterableNode.setIteratorName("#itr" + userEachNode.getLocation().getOffset());
            irForEachSubIterableNode.setMethod(iterableValueType == def.class ? null :
                    scriptScope.getDecoration(userEachNode, IterablePainlessMethod.class).getIterablePainlessMethod());
            irForEachSubIterableNode.setContinuous(false);
            irConditionNode = irForEachSubIterableNode;
        } else {
            throw userEachNode.createError(new IllegalStateException("illegal tree structure"));
        }

        ForEachLoopNode irForEachLoopNode = new ForEachLoopNode(userEachNode.getLocation());
        irForEachLoopNode.setConditionNode(irConditionNode);

        scriptScope.putDecoration(userEachNode, new IRNodeDecoration(irForEachLoopNode));
    }

    @Override
    public void visitDeclBlock(SDeclBlock userDeclBlockNode, ScriptScope scriptScope) {
        DeclarationBlockNode irDeclarationBlockNode = new DeclarationBlockNode(userDeclBlockNode.getLocation());

        for (SDeclaration userDeclarationNode : userDeclBlockNode.getDeclarationNodes()) {
            irDeclarationBlockNode.addDeclarationNode((DeclarationNode)visit(userDeclarationNode, scriptScope));
        }

        scriptScope.putDecoration(userDeclBlockNode, new IRNodeDecoration(irDeclarationBlockNode));
    }

    @Override
    public void visitDeclaration(SDeclaration userDeclarationNode, ScriptScope scriptScope) {
        Variable variable = scriptScope.getDecoration(userDeclarationNode, SemanticVariable.class).getSemanticVariable();

        DeclarationNode irDeclarationNode = new DeclarationNode(userDeclarationNode.getLocation());
        irDeclarationNode.setExpressionNode(injectCast(userDeclarationNode.getValueNode(), scriptScope));
        irDeclarationNode.setDeclarationType(variable.getType());
        irDeclarationNode.setName(variable.getName());

        scriptScope.putDecoration(userDeclarationNode, new IRNodeDecoration(irDeclarationNode));
    }

    @Override
    public void visitReturn(SReturn userReturnNode, ScriptScope scriptScope) {
        ReturnNode irReturnNode = new ReturnNode(userReturnNode.getLocation());
        irReturnNode.setExpressionNode(injectCast(userReturnNode.getValueNode(), scriptScope));

        scriptScope.putDecoration(userReturnNode, new IRNodeDecoration(irReturnNode));
    }

    @Override
    public void visitExpression(SExpression userExpressionNode, ScriptScope scriptScope) {
        StatementNode irStatementNode;
        ExpressionNode irExpressionNode = injectCast(userExpressionNode.getStatementNode(), scriptScope);

        if (scriptScope.getCondition(userExpressionNode, MethodEscape.class)) {
            ReturnNode irReturnNode = new ReturnNode(userExpressionNode.getLocation());
            irReturnNode.setExpressionNode(irExpressionNode);
            irStatementNode = irReturnNode;
        } else {
            StatementExpressionNode irStatementExpressionNode = new StatementExpressionNode(userExpressionNode.getLocation());
            irStatementExpressionNode.setExpressionNode(irExpressionNode);
            irStatementNode = irStatementExpressionNode;
        }

        scriptScope.putDecoration(userExpressionNode, new IRNodeDecoration(irStatementNode));
    }

    @Override
    public void visitTry(STry userTryNode, ScriptScope scriptScope) {
        TryNode irTryNode = new TryNode(userTryNode.getLocation());

        for (SCatch userCatchNode : userTryNode.getCatchNodes()) {
            irTryNode.addCatchNode((CatchNode)visit(userCatchNode, scriptScope));
        }

        irTryNode.setBlockNode((BlockNode)visit(userTryNode.getBlockNode(), scriptScope));

        scriptScope.putDecoration(userTryNode, new IRNodeDecoration(irTryNode));
    }

    @Override
    public void visitCatch(SCatch userCatchNode, ScriptScope scriptScope) {
        Variable variable = scriptScope.getDecoration(userCatchNode, SemanticVariable.class).getSemanticVariable();

        CatchNode irCatchNode = new CatchNode(userCatchNode.getLocation());
        irCatchNode.setExceptionType(variable.getType());
        irCatchNode.setSymbol(variable.getName());
        irCatchNode.setBlockNode((BlockNode)visit(userCatchNode.getBlockNode(), scriptScope));

        scriptScope.putDecoration(userCatchNode, new IRNodeDecoration(irCatchNode));
    }

    @Override
    public void visitThrow(SThrow userThrowNode, ScriptScope scriptScope) {
        ThrowNode irThrowNode = new ThrowNode(userThrowNode.getLocation());
        irThrowNode.setExpressionNode(injectCast(userThrowNode.getExpressionNode(), scriptScope));

        scriptScope.putDecoration(userThrowNode, new IRNodeDecoration(irThrowNode));
    }

    @Override
    public void visitContinue(SContinue userContinueNode, ScriptScope scriptScope) {
        ContinueNode irContinueNode = new ContinueNode(userContinueNode.getLocation());

        scriptScope.putDecoration(userContinueNode, new IRNodeDecoration(irContinueNode));
    }

    @Override
    public void visitBreak(SBreak userBreakNode, ScriptScope scriptScope) {
        BreakNode irBreakNode = new BreakNode(userBreakNode.getLocation());

        scriptScope.putDecoration(userBreakNode, new IRNodeDecoration(irBreakNode));
    }

    @Override
    public void visitAssignment(EAssignment userAssignmentNode, ScriptScope scriptScope) {
        boolean read = scriptScope.getCondition(userAssignmentNode, Read.class);
        Class<?> compoundType = scriptScope.hasDecoration(userAssignmentNode, CompoundType.class) ?
                scriptScope.getDecoration(userAssignmentNode, CompoundType.class).getCompoundType() : null;

        ExpressionNode irAssignmentNode;
        // add a cast node if necessary for the value node for the assignment
        ExpressionNode irValueNode = injectCast(userAssignmentNode.getRightNode(), scriptScope);

        // handles a compound assignment using the stub generated from buildLoadStore
        if (compoundType != null) {
            boolean concatenate = userAssignmentNode.getOperation() == Operation.ADD && compoundType == String.class;
            scriptScope.setCondition(userAssignmentNode.getLeftNode(), Compound.class);
            StoreNode irStoreNode = (StoreNode)visit(userAssignmentNode.getLeftNode(), scriptScope);
            ExpressionNode irLoadNode = irStoreNode.getChildNode();
            ExpressionNode irCompoundNode;

            // handles when the operation is a string concatenation
            if (concatenate) {
                StringConcatenationNode stringConcatenationNode = new StringConcatenationNode(irStoreNode.getLocation());
                stringConcatenationNode.setExpressionType(String.class);
                irCompoundNode = stringConcatenationNode;

                // must handle the StringBuilder case for java version <= 8
                if (irLoadNode instanceof BinaryImplNode && WriterConstants.INDY_STRING_CONCAT_BOOTSTRAP_HANDLE == null) {
                    ((DupNode)((BinaryImplNode)irLoadNode).getLeftNode()).setDepth(1);
                }
            // handles when the operation is mathematical
            } else {
                BinaryMathNode irBinaryMathNode = new BinaryMathNode(irStoreNode.getLocation());
                irBinaryMathNode.setLeftNode(irLoadNode);
                irBinaryMathNode.setExpressionType(compoundType);
                irBinaryMathNode.setBinaryType(compoundType);
                irBinaryMathNode.setOperation(userAssignmentNode.getOperation());
                // add a compound assignment flag to the binary math node
                irBinaryMathNode.setFlags(DefBootstrap.OPERATOR_COMPOUND_ASSIGNMENT);
                irCompoundNode = irBinaryMathNode;
            }

            PainlessCast downcast = scriptScope.hasDecoration(userAssignmentNode, DowncastPainlessCast.class) ?
                    scriptScope.getDecoration(userAssignmentNode, DowncastPainlessCast.class).getDowncastPainlessCast() : null;

            // no need to downcast so the binary math node is the value for the store node
            if (downcast == null) {
                irCompoundNode.setExpressionType(irStoreNode.getStoreType());
                irStoreNode.setChildNode(irCompoundNode);
            // add a cast node to do a downcast as the value for the store node
            } else {
                CastNode irCastNode = new CastNode(irCompoundNode.getLocation());
                irCastNode.setExpressionType(downcast.targetType);
                irCastNode.setCast(downcast);
                irCastNode.setChildNode(irCompoundNode);
                irStoreNode.setChildNode(irCastNode);
            }

            // the value is also read from this assignment
            if (read) {
                int accessDepth = scriptScope.getDecoration(userAssignmentNode.getLeftNode(), AccessDepth.class).getAccessDepth();
                DupNode irDupNode;

                // the value is read from prior to assignment (post-increment)
                if (userAssignmentNode.postIfRead()) {
                    irDupNode = new DupNode(irLoadNode.getLocation());
                    irDupNode.setExpressionType(irLoadNode.getExpressionType());
                    irDupNode.setSize(MethodWriter.getType(irLoadNode.getExpressionType()).getSize());
                    irDupNode.setDepth(accessDepth);
                    irDupNode.setChildNode(irLoadNode);
                    irLoadNode = irDupNode;
                // the value is read from after the assignment (pre-increment/compound)
                } else {
                    irDupNode = new DupNode(irStoreNode.getLocation());
                    irDupNode.setExpressionType(irStoreNode.getStoreType());
                    irDupNode.setSize(MethodWriter.getType(irStoreNode.getExpressionType()).getSize());
                    irDupNode.setDepth(accessDepth);
                    irDupNode.setChildNode(irStoreNode.getChildNode());
                    irStoreNode.setChildNode(irDupNode);
                }
            }

            PainlessCast upcast = scriptScope.hasDecoration(userAssignmentNode, UpcastPainlessCast.class) ?
                    scriptScope.getDecoration(userAssignmentNode, UpcastPainlessCast.class).getUpcastPainlessCast() : null;

            // upcast the stored value if necessary
            if (upcast != null) {
                CastNode irCastNode = new CastNode(irLoadNode.getLocation());
                irCastNode.setExpressionType(upcast.targetType);
                irCastNode.setCast(upcast);
                irCastNode.setChildNode(irLoadNode);
                irLoadNode = irCastNode;
            }

            if (concatenate) {
                StringConcatenationNode irStringConcatenationNode = (StringConcatenationNode)irCompoundNode;
                irStringConcatenationNode.addArgumentNode(irLoadNode);
                irStringConcatenationNode.addArgumentNode(irValueNode);
            } else {
                BinaryMathNode irBinaryMathNode = (BinaryMathNode)irCompoundNode;
                irBinaryMathNode.setLeftNode(irLoadNode);
                irBinaryMathNode.setRightNode(irValueNode);
            }

            irAssignmentNode = irStoreNode;
        // handles a standard assignment
        } else {
            irAssignmentNode = (ExpressionNode)visit(userAssignmentNode.getLeftNode(), scriptScope);

            // the value is read from after the assignment
            if (read) {
                int accessDepth = scriptScope.getDecoration(userAssignmentNode.getLeftNode(), AccessDepth.class).getAccessDepth();

                DupNode irDupNode = new DupNode(irValueNode.getLocation());
                irDupNode.setExpressionType(irValueNode.getExpressionType());
                irDupNode.setSize(MethodWriter.getType(irValueNode.getExpressionType()).getSize());
                irDupNode.setDepth(accessDepth);
                irDupNode.setChildNode(irValueNode);
                irValueNode = irDupNode;
            }

            if (irAssignmentNode instanceof BinaryImplNode) {
                ((StoreNode)((BinaryImplNode)irAssignmentNode).getRightNode()).setChildNode(irValueNode);
            } else {
                ((StoreNode)irAssignmentNode).setChildNode(irValueNode);
            }
        }

        scriptScope.putDecoration(userAssignmentNode, new IRNodeDecoration(irAssignmentNode));
    }

    @Override
    public void visitUnary(EUnary userUnaryNode, ScriptScope scriptScope) {
        Class<?> unaryType = scriptScope.hasDecoration(userUnaryNode, UnaryType.class) ?
                scriptScope.getDecoration(userUnaryNode, UnaryType.class).getUnaryType() : null;

        IRNode irNode;

        if (scriptScope.getCondition(userUnaryNode.getChildNode(), Negate.class)) {
            irNode = visit(userUnaryNode.getChildNode(), scriptScope);
        } else {
            UnaryMathNode irUnaryMathNode = new UnaryMathNode(userUnaryNode.getLocation());
            irUnaryMathNode.setExpressionType(scriptScope.getDecoration(userUnaryNode, ValueType.class).getValueType());
            irUnaryMathNode.setUnaryType(unaryType);
            irUnaryMathNode.setOperation(userUnaryNode.getOperation());
            irUnaryMathNode.setOriginallyExplicit(scriptScope.getCondition(userUnaryNode, Explicit.class));
            irUnaryMathNode.setChildNode(injectCast(userUnaryNode.getChildNode(), scriptScope));
            irNode = irUnaryMathNode;
        }

        scriptScope.putDecoration(userUnaryNode, new IRNodeDecoration(irNode));
    }

    @Override
    public void visitBinary(EBinary userBinaryNode, ScriptScope scriptScope) {
        ExpressionNode irExpressionNode;

        Operation operation = userBinaryNode.getOperation();
        Class<?> valueType = scriptScope.getDecoration(userBinaryNode, ValueType.class).getValueType();

        if (operation == Operation.ADD && valueType == String.class) {
            StringConcatenationNode stringConcatenationNode = new StringConcatenationNode(userBinaryNode.getLocation());
            stringConcatenationNode.addArgumentNode((ExpressionNode)visit(userBinaryNode.getLeftNode(), scriptScope));
            stringConcatenationNode.addArgumentNode((ExpressionNode)visit(userBinaryNode.getRightNode(), scriptScope));
            irExpressionNode = stringConcatenationNode;
        } else {
            Class<?> shiftType = scriptScope.hasDecoration(userBinaryNode, ShiftType.class) ?
                    scriptScope.getDecoration(userBinaryNode, ShiftType.class).getShiftType() : null;

            BinaryMathNode irBinaryMathNode = new BinaryMathNode(userBinaryNode.getLocation());

            if (operation == Operation.MATCH || operation == Operation.FIND) {
                irBinaryMathNode.setRegexLimit(scriptScope.getCompilerSettings().getRegexLimitFactor());
            }
            irBinaryMathNode.setBinaryType(scriptScope.getDecoration(userBinaryNode, BinaryType.class).getBinaryType());
            irBinaryMathNode.setShiftType(shiftType);
            irBinaryMathNode.setOperation(operation);

            if (scriptScope.getCondition(userBinaryNode, Explicit.class)) {
                irBinaryMathNode.setFlags(DefBootstrap.OPERATOR_EXPLICIT_CAST);
            }

            irBinaryMathNode.setLeftNode(injectCast(userBinaryNode.getLeftNode(), scriptScope));
            irBinaryMathNode.setRightNode(injectCast(userBinaryNode.getRightNode(), scriptScope));
            irExpressionNode = irBinaryMathNode;
        }

        irExpressionNode.setExpressionType(valueType);
        scriptScope.putDecoration(userBinaryNode, new IRNodeDecoration(irExpressionNode));
    }

    @Override
    public void visitBooleanComp(EBooleanComp userBooleanCompNode, ScriptScope scriptScope) {
        BooleanNode irBooleanNode = new BooleanNode(userBooleanCompNode.getLocation());
        irBooleanNode.setExpressionType(scriptScope.getDecoration(userBooleanCompNode, ValueType.class).getValueType());
        irBooleanNode.setOperation(userBooleanCompNode.getOperation());
        irBooleanNode.setLeftNode(injectCast(userBooleanCompNode.getLeftNode(), scriptScope));
        irBooleanNode.setRightNode(injectCast(userBooleanCompNode.getRightNode(), scriptScope));

        scriptScope.putDecoration(userBooleanCompNode, new IRNodeDecoration(irBooleanNode));
    }

    @Override
    public void visitComp(EComp userCompNode, ScriptScope scriptScope) {
        ComparisonNode irComparisonNode = new ComparisonNode(userCompNode.getLocation());
        irComparisonNode.setExpressionType(scriptScope.getDecoration(userCompNode, ValueType.class).getValueType());
        irComparisonNode.setComparisonType(scriptScope.getDecoration(userCompNode, ComparisonType.class).getComparisonType());
        irComparisonNode.setOperation(userCompNode.getOperation());
        irComparisonNode.setLeftNode(injectCast(userCompNode.getLeftNode(), scriptScope));
        irComparisonNode.setRightNode(injectCast(userCompNode.getRightNode(), scriptScope));

        scriptScope.putDecoration(userCompNode, new IRNodeDecoration(irComparisonNode));
    }

    @Override
    public void visitExplicit(EExplicit userExplicitNode, ScriptScope scriptScope) {
        scriptScope.putDecoration(userExplicitNode, new IRNodeDecoration(injectCast(userExplicitNode.getChildNode(), scriptScope)));
    }

    @Override
    public void visitInstanceof(EInstanceof userInstanceofNode, ScriptScope scriptScope) {
        InstanceofNode irInstanceofNode = new InstanceofNode(userInstanceofNode.getLocation());
        irInstanceofNode.setExpressionType(scriptScope.getDecoration(userInstanceofNode, ValueType.class).getValueType());
        irInstanceofNode.setInstanceType(scriptScope.getDecoration(userInstanceofNode, InstanceType.class).getInstanceType());
        irInstanceofNode.setChildNode((ExpressionNode)visit(userInstanceofNode.getExpressionNode(), scriptScope));

        scriptScope.putDecoration(userInstanceofNode, new IRNodeDecoration(irInstanceofNode));
    }

    @Override
    public void visitConditional(EConditional userConditionalNode, ScriptScope scriptScope) {
        ConditionalNode irConditionalNode = new ConditionalNode(userConditionalNode.getLocation());
        irConditionalNode.setExpressionType(scriptScope.getDecoration(userConditionalNode, ValueType.class).getValueType());
        irConditionalNode.setConditionNode(injectCast(userConditionalNode.getConditionNode(), scriptScope));
        irConditionalNode.setLeftNode(injectCast(userConditionalNode.getTrueNode(), scriptScope));
        irConditionalNode.setRightNode(injectCast(userConditionalNode.getFalseNode(), scriptScope));

        scriptScope.putDecoration(userConditionalNode, new IRNodeDecoration(irConditionalNode));
    }

    @Override
    public void visitElvis(EElvis userElvisNode, ScriptScope scriptScope) {
        ElvisNode irElvisNode = new ElvisNode(userElvisNode.getLocation());
        irElvisNode.setExpressionType(scriptScope.getDecoration(userElvisNode, ValueType.class).getValueType());
        irElvisNode.setLeftNode(injectCast(userElvisNode.getLeftNode(), scriptScope));
        irElvisNode.setRightNode(injectCast(userElvisNode.getRightNode(), scriptScope));

        scriptScope.putDecoration(userElvisNode, new IRNodeDecoration(irElvisNode));
    }

    @Override
    public void visitListInit(EListInit userListInitNode, ScriptScope scriptScope) {
        ListInitializationNode irListInitializationNode = new ListInitializationNode(userListInitNode.getLocation());

        irListInitializationNode.setExpressionType(scriptScope.getDecoration(userListInitNode, ValueType.class).getValueType());
        irListInitializationNode.setConstructor(
                scriptScope.getDecoration(userListInitNode, StandardPainlessConstructor.class).getStandardPainlessConstructor());
        irListInitializationNode.setMethod(
                scriptScope.getDecoration(userListInitNode, StandardPainlessMethod.class).getStandardPainlessMethod());

        for (AExpression userValueNode : userListInitNode.getValueNodes()) {
            irListInitializationNode.addArgumentNode(injectCast(userValueNode, scriptScope));
        }

        scriptScope.putDecoration(userListInitNode, new IRNodeDecoration(irListInitializationNode));
    }

    @Override
    public void visitMapInit(EMapInit userMapInitNode, ScriptScope scriptScope) {
        MapInitializationNode irMapInitializationNode = new MapInitializationNode(userMapInitNode.getLocation());

        irMapInitializationNode.setExpressionType(scriptScope.getDecoration(userMapInitNode, ValueType.class).getValueType());
        irMapInitializationNode.setConstructor(
                scriptScope.getDecoration(userMapInitNode, StandardPainlessConstructor.class).getStandardPainlessConstructor());
        irMapInitializationNode.setMethod(
                scriptScope.getDecoration(userMapInitNode, StandardPainlessMethod.class).getStandardPainlessMethod());


        for (int i = 0; i < userMapInitNode.getKeyNodes().size(); ++i) {
            irMapInitializationNode.addArgumentNode(
                    injectCast(userMapInitNode.getKeyNodes().get(i), scriptScope),
                    injectCast(userMapInitNode.getValueNodes().get(i), scriptScope));
        }

        scriptScope.putDecoration(userMapInitNode, new IRNodeDecoration(irMapInitializationNode));
    }

    @Override
    public void visitNewArray(ENewArray userNewArrayNode, ScriptScope scriptScope) {
        NewArrayNode irNewArrayNode = new NewArrayNode(userNewArrayNode.getLocation());

        irNewArrayNode.setExpressionType(scriptScope.getDecoration(userNewArrayNode, ValueType.class).getValueType());
        irNewArrayNode.setInitialize(userNewArrayNode.isInitializer());

        for (AExpression userArgumentNode : userNewArrayNode.getValueNodes()) {
            irNewArrayNode.addArgumentNode(injectCast(userArgumentNode, scriptScope));
        }

        scriptScope.putDecoration(userNewArrayNode, new IRNodeDecoration(irNewArrayNode));
    }

    @Override
    public void visitNewObj(ENewObj userNewObjectNode, ScriptScope scriptScope) {
        NewObjectNode irNewObjectNode = new NewObjectNode(userNewObjectNode.getLocation());

        irNewObjectNode.setExpressionType(scriptScope.getDecoration(userNewObjectNode, ValueType.class).getValueType());
        irNewObjectNode.setRead(scriptScope.getCondition(userNewObjectNode, Read.class));
        irNewObjectNode.setConstructor(
                scriptScope.getDecoration(userNewObjectNode, StandardPainlessConstructor.class).getStandardPainlessConstructor());

        for (AExpression userArgumentNode : userNewObjectNode.getArgumentNodes()) {
            irNewObjectNode.addArgumentNode(injectCast(userArgumentNode, scriptScope));
        }

        scriptScope.putDecoration(userNewObjectNode, new IRNodeDecoration(irNewObjectNode));
    }

    @Override
    public void visitCallLocal(ECallLocal callLocalNode, ScriptScope scriptScope) {
        InvokeCallMemberNode irInvokeCallMemberNode = new InvokeCallMemberNode(callLocalNode.getLocation());

        if (scriptScope.hasDecoration(callLocalNode, StandardLocalFunction.class)) {
            irInvokeCallMemberNode.setLocalFunction(
                    scriptScope.getDecoration(callLocalNode, StandardLocalFunction.class).getLocalFunction());
        } else if (scriptScope.hasDecoration(callLocalNode, StandardPainlessMethod.class)) {
            irInvokeCallMemberNode.setImportedMethod(
                    scriptScope.getDecoration(callLocalNode, StandardPainlessMethod.class).getStandardPainlessMethod());
        } else if (scriptScope.hasDecoration(callLocalNode, StandardPainlessClassBinding.class)) {
            PainlessClassBinding painlessClassBinding =
                    scriptScope.getDecoration(callLocalNode, StandardPainlessClassBinding.class).getPainlessClassBinding();
            String bindingName = scriptScope.getNextSyntheticName("class_binding");

            FieldNode irFieldNode = new FieldNode(callLocalNode.getLocation());
            irFieldNode.setModifiers(Modifier.PRIVATE);
            irFieldNode.setFieldType(painlessClassBinding.javaConstructor.getDeclaringClass());
            irFieldNode.setName(bindingName);
            irClassNode.addFieldNode(irFieldNode);

            irInvokeCallMemberNode.setClassBinding(painlessClassBinding);
            irInvokeCallMemberNode.setClassBindingOffset(
                    (int)scriptScope.getDecoration(callLocalNode, StandardConstant.class).getStandardConstant());
            irInvokeCallMemberNode.setBindingName(bindingName);
        } else if (scriptScope.hasDecoration(callLocalNode, StandardPainlessInstanceBinding.class)) {
            PainlessInstanceBinding painlessInstanceBinding =
                    scriptScope.getDecoration(callLocalNode, StandardPainlessInstanceBinding.class).getPainlessInstanceBinding();
            String bindingName = scriptScope.getNextSyntheticName("instance_binding");

            FieldNode irFieldNode = new FieldNode(callLocalNode.getLocation());
            irFieldNode.setModifiers(Modifier.PUBLIC | Modifier.STATIC);
            irFieldNode.setFieldType(painlessInstanceBinding.targetInstance.getClass());
            irFieldNode.setName(bindingName);
            irClassNode.addFieldNode(irFieldNode);

            irInvokeCallMemberNode.setInstanceBinding(painlessInstanceBinding);
            irInvokeCallMemberNode.setBindingName(bindingName);

            scriptScope.addStaticConstant(bindingName, painlessInstanceBinding.targetInstance);
        } else {
            throw callLocalNode.createError(new IllegalStateException("illegal tree structure"));
        }

        for (AExpression userArgumentNode : callLocalNode.getArgumentNodes()) {
            irInvokeCallMemberNode.addArgumentNode(injectCast(userArgumentNode, scriptScope));
        }

        irInvokeCallMemberNode.setExpressionType(scriptScope.getDecoration(callLocalNode, ValueType.class).getValueType());

        scriptScope.putDecoration(callLocalNode, new IRNodeDecoration(irInvokeCallMemberNode));
    }

    @Override
    public void visitBooleanConstant(EBooleanConstant userBooleanConstantNode, ScriptScope scriptScope) {
        ConstantNode irConstantNode = new ConstantNode(userBooleanConstantNode.getLocation());
        irConstantNode.setExpressionType(scriptScope.getDecoration(userBooleanConstantNode, ValueType.class).getValueType());
        irConstantNode.setConstant(scriptScope.getDecoration(userBooleanConstantNode, StandardConstant.class).getStandardConstant());

        scriptScope.putDecoration(userBooleanConstantNode, new IRNodeDecoration(irConstantNode));
    }

    @Override
    public void visitNumeric(ENumeric userNumericNode, ScriptScope scriptScope) {
        ConstantNode irConstantNode = new ConstantNode(userNumericNode.getLocation());
        irConstantNode.setExpressionType(scriptScope.getDecoration(userNumericNode, ValueType.class).getValueType());
        irConstantNode.setConstant(scriptScope.getDecoration(userNumericNode, StandardConstant.class).getStandardConstant());

        scriptScope.putDecoration(userNumericNode, new IRNodeDecoration(irConstantNode));
    }

    @Override
    public void visitDecimal(EDecimal userDecimalNode, ScriptScope scriptScope) {
        ConstantNode irConstantNode = new ConstantNode(userDecimalNode.getLocation());
        irConstantNode.setExpressionType(scriptScope.getDecoration(userDecimalNode, ValueType.class).getValueType());
        irConstantNode.setConstant(scriptScope.getDecoration(userDecimalNode, StandardConstant.class).getStandardConstant());

        scriptScope.putDecoration(userDecimalNode, new IRNodeDecoration(irConstantNode));
    }

    @Override
    public void visitString(EString userStringNode, ScriptScope scriptScope) {
        ConstantNode irConstantNode = new ConstantNode(userStringNode.getLocation());
        irConstantNode.setExpressionType(scriptScope.getDecoration(userStringNode, ValueType.class).getValueType());
        irConstantNode.setConstant(scriptScope.getDecoration(userStringNode, StandardConstant.class).getStandardConstant());

        scriptScope.putDecoration(userStringNode, new IRNodeDecoration(irConstantNode));
    }

    @Override
    public void visitNull(ENull userNullNode, ScriptScope scriptScope) {
        NullNode irNullNode = new NullNode(userNullNode.getLocation());
        irNullNode.setExpressionType(scriptScope.getDecoration(userNullNode, ValueType.class).getValueType());

        scriptScope.putDecoration(userNullNode, new IRNodeDecoration(irNullNode));
    }

    @Override
    public void visitRegex(ERegex userRegexNode, ScriptScope scriptScope) {
        String memberFieldName = scriptScope.getNextSyntheticName("regex");

        FieldNode irFieldNode = new FieldNode(userRegexNode.getLocation());
        irFieldNode.setModifiers(Modifier.FINAL | Modifier.STATIC | Modifier.PRIVATE);
        irFieldNode.setFieldType(Pattern.class);
        irFieldNode.setName(memberFieldName);

        irClassNode.addFieldNode(irFieldNode);

        try {
            StatementExpressionNode irStatementExpressionNode = new StatementExpressionNode(userRegexNode.getLocation());

            BlockNode blockNode = irClassNode.getClinitBlockNode();
            blockNode.addStatementNode(irStatementExpressionNode);

            StoreFieldMemberNode irStoreFieldMemberNode = new StoreFieldMemberNode(userRegexNode.getLocation());
            irStoreFieldMemberNode.setExpressionType(void.class);
            irStoreFieldMemberNode.setStoreType(Pattern.class);
            irStoreFieldMemberNode.setName(memberFieldName);
            irStoreFieldMemberNode.setStatic(true);

            irStatementExpressionNode.setExpressionNode(irStoreFieldMemberNode);

            BinaryImplNode irBinaryImplNode = new BinaryImplNode(userRegexNode.getLocation());
            irBinaryImplNode.setExpressionType(Pattern.class);

            irStoreFieldMemberNode.setChildNode(irBinaryImplNode);

            StaticNode irStaticNode = new StaticNode(userRegexNode.getLocation());
            irStaticNode.setExpressionType(Pattern.class);

            irBinaryImplNode.setLeftNode(irStaticNode);

            InvokeCallNode invokeCallNode = new InvokeCallNode(userRegexNode.getLocation());
            invokeCallNode.setExpressionType(Pattern.class);
            invokeCallNode.setBox(Pattern.class);
            invokeCallNode.setMethod(new PainlessMethod(
                            Pattern.class.getMethod("compile", String.class, int.class),
                            Pattern.class,
                            Pattern.class,
                            Arrays.asList(String.class, int.class),
                            null,
                            null,
                            null
                    )
            );

            irBinaryImplNode.setRightNode(invokeCallNode);

            ConstantNode irConstantNode = new ConstantNode(userRegexNode.getLocation());
            irConstantNode.setExpressionType(String.class);
            irConstantNode.setConstant(userRegexNode.getPattern());

            invokeCallNode.addArgumentNode(irConstantNode);

            irConstantNode = new ConstantNode(userRegexNode.getLocation());
            irConstantNode.setExpressionType(int.class);
            irConstantNode.setConstant(scriptScope.getDecoration(userRegexNode, StandardConstant.class).getStandardConstant());

            invokeCallNode.addArgumentNode(irConstantNode);
        } catch (Exception exception) {
            throw userRegexNode.createError(new IllegalStateException("illegal tree structure"));
        }

        LoadFieldMemberNode irLoadFieldMemberNode = new LoadFieldMemberNode(userRegexNode.getLocation());
        irLoadFieldMemberNode.setExpressionType(Pattern.class);
        irLoadFieldMemberNode.setName(memberFieldName);
        irLoadFieldMemberNode.setStatic(true);

        scriptScope.putDecoration(userRegexNode, new IRNodeDecoration(irLoadFieldMemberNode));
    }

    @Override
    public void visitLambda(ELambda userLambdaNode, ScriptScope scriptScope) {
        ReferenceNode irReferenceNode;

        if (scriptScope.hasDecoration(userLambdaNode, TargetType.class)) {
            TypedInterfaceReferenceNode typedInterfaceReferenceNode = new TypedInterfaceReferenceNode(userLambdaNode.getLocation());
            typedInterfaceReferenceNode.setReference(scriptScope.getDecoration(userLambdaNode, ReferenceDecoration.class).getReference());
            irReferenceNode = typedInterfaceReferenceNode;
        } else {
            DefInterfaceReferenceNode defInterfaceReferenceNode = new DefInterfaceReferenceNode(userLambdaNode.getLocation());
            defInterfaceReferenceNode.setDefReferenceEncoding(
                    scriptScope.getDecoration(userLambdaNode, EncodingDecoration.class).getEncoding());
            irReferenceNode = defInterfaceReferenceNode;
        }

        FunctionNode irFunctionNode = new FunctionNode(userLambdaNode.getLocation());
        irFunctionNode.setBlockNode((BlockNode)visit(userLambdaNode.getBlockNode(), scriptScope));
        irFunctionNode.setName(scriptScope.getDecoration(userLambdaNode, MethodNameDecoration.class).getMethodName());
        irFunctionNode.setReturnType(scriptScope.getDecoration(userLambdaNode, ReturnType.class).getReturnType());
        irFunctionNode.getTypeParameters().addAll(scriptScope.getDecoration(userLambdaNode, TypeParameters.class).getTypeParameters());
        irFunctionNode.getParameterNames().addAll(scriptScope.getDecoration(userLambdaNode, ParameterNames.class).getParameterNames());
        irFunctionNode.setStatic(true);
        irFunctionNode.setVarArgs(false);
        irFunctionNode.setSynthetic(true);
        irFunctionNode.setMaxLoopCounter(scriptScope.getCompilerSettings().getMaxLoopCounter());
        irClassNode.addFunctionNode(irFunctionNode);

        irReferenceNode.setExpressionType(scriptScope.getDecoration(userLambdaNode, ValueType.class).getValueType());

        List<Variable> captures = scriptScope.getDecoration(userLambdaNode, CapturesDecoration.class).getCaptures();

        for (Variable capture : captures) {
            irReferenceNode.addCapture(capture.getName());
        }

        scriptScope.putDecoration(userLambdaNode, new IRNodeDecoration(irReferenceNode));
    }

    @Override
    public void visitFunctionRef(EFunctionRef userFunctionRefNode, ScriptScope scriptScope) {
        ReferenceNode irReferenceNode;

        TargetType targetType = scriptScope.getDecoration(userFunctionRefNode, TargetType.class);
        CapturesDecoration capturesDecoration = scriptScope.getDecoration(userFunctionRefNode, CapturesDecoration.class);

        if (targetType == null) {
            DefInterfaceReferenceNode defInterfaceReferenceNode = new DefInterfaceReferenceNode(userFunctionRefNode.getLocation());
            defInterfaceReferenceNode.setDefReferenceEncoding(
                    scriptScope.getDecoration(userFunctionRefNode, EncodingDecoration.class).getEncoding());
            irReferenceNode = defInterfaceReferenceNode;
        } else if (capturesDecoration != null && capturesDecoration.getCaptures().get(0).getType() == def.class) {
            TypedCaptureReferenceNode typedCaptureReferenceNode = new TypedCaptureReferenceNode(userFunctionRefNode.getLocation());
            typedCaptureReferenceNode.setMethodName(userFunctionRefNode.getMethodName());
            irReferenceNode = typedCaptureReferenceNode;
        } else {
            TypedInterfaceReferenceNode typedInterfaceReferenceNode = new TypedInterfaceReferenceNode(userFunctionRefNode.getLocation());
            typedInterfaceReferenceNode.setReference(
                    scriptScope.getDecoration(userFunctionRefNode, ReferenceDecoration.class).getReference());
            irReferenceNode = typedInterfaceReferenceNode;
        }

        irReferenceNode.setExpressionType(scriptScope.getDecoration(userFunctionRefNode, ValueType.class).getValueType());

        if (capturesDecoration != null) {
            irReferenceNode.addCapture(capturesDecoration.getCaptures().get(0).getName());
        }

        scriptScope.putDecoration(userFunctionRefNode, new IRNodeDecoration(irReferenceNode));
    }

    @Override
    public void visitNewArrayFunctionRef(ENewArrayFunctionRef userNewArrayFunctionRefNode, ScriptScope scriptScope) {
        ReferenceNode irReferenceNode;

        if (scriptScope.hasDecoration(userNewArrayFunctionRefNode, TargetType.class)) {
            TypedInterfaceReferenceNode typedInterfaceReferenceNode =
                    new TypedInterfaceReferenceNode(userNewArrayFunctionRefNode.getLocation());
            typedInterfaceReferenceNode.setReference(
                    scriptScope.getDecoration(userNewArrayFunctionRefNode, ReferenceDecoration.class).getReference());
            irReferenceNode = typedInterfaceReferenceNode;
        } else {
            DefInterfaceReferenceNode defInterfaceReferenceNode = new DefInterfaceReferenceNode(userNewArrayFunctionRefNode.getLocation());
            defInterfaceReferenceNode.setDefReferenceEncoding(
                    scriptScope.getDecoration(userNewArrayFunctionRefNode, EncodingDecoration.class).getEncoding());
            irReferenceNode = defInterfaceReferenceNode;
        }

        Class<?> returnType = scriptScope.getDecoration(userNewArrayFunctionRefNode, ReturnType.class).getReturnType();

        LoadVariableNode irLoadVariableNode = new LoadVariableNode(userNewArrayFunctionRefNode.getLocation());
        irLoadVariableNode.setExpressionType(int.class);
        irLoadVariableNode.setName("size");

        NewArrayNode irNewArrayNode = new NewArrayNode(userNewArrayFunctionRefNode.getLocation());
        irNewArrayNode.setExpressionType(returnType);
        irNewArrayNode.setInitialize(false);

        irNewArrayNode.addArgumentNode(irLoadVariableNode);

        ReturnNode irReturnNode = new ReturnNode(userNewArrayFunctionRefNode.getLocation());
        irReturnNode.setExpressionNode(irNewArrayNode);

        BlockNode irBlockNode = new BlockNode(userNewArrayFunctionRefNode.getLocation());
        irBlockNode.setAllEscape(true);
        irBlockNode.addStatementNode(irReturnNode);

        FunctionNode irFunctionNode = new FunctionNode(userNewArrayFunctionRefNode.getLocation());
        irFunctionNode.setMaxLoopCounter(0);
        irFunctionNode.setName(scriptScope.getDecoration(userNewArrayFunctionRefNode, MethodNameDecoration.class).getMethodName());
        irFunctionNode.setReturnType(returnType);
        irFunctionNode.addTypeParameter(int.class);
        irFunctionNode.addParameterName("size");
        irFunctionNode.setStatic(true);
        irFunctionNode.setVarArgs(false);
        irFunctionNode.setSynthetic(true);
        irFunctionNode.setBlockNode(irBlockNode);

        irClassNode.addFunctionNode(irFunctionNode);

        irReferenceNode.setExpressionType(scriptScope.getDecoration(userNewArrayFunctionRefNode, ValueType.class).getValueType());

        scriptScope.putDecoration(userNewArrayFunctionRefNode, new IRNodeDecoration(irReferenceNode));
    }

    /**
     * This handles both load and store for symbol accesses as necessary. This uses buildLoadStore to
     * stub out the appropriate load and store ir nodes.
     */
    @Override
    public void visitSymbol(ESymbol userSymbolNode, ScriptScope scriptScope) {
        ExpressionNode irExpressionNode;

        if (scriptScope.hasDecoration(userSymbolNode, StaticType.class)) {
            Class<?> staticType = scriptScope.getDecoration(userSymbolNode, StaticType.class).getStaticType();
            StaticNode staticNode = new StaticNode(userSymbolNode.getLocation());
            staticNode.setExpressionType(staticType);
            irExpressionNode = staticNode;
        } else if (scriptScope.hasDecoration(userSymbolNode, ValueType.class)) {
            boolean read = scriptScope.getCondition(userSymbolNode, Read.class);
            boolean write = scriptScope.getCondition(userSymbolNode, Write.class);
            boolean compound = scriptScope.getCondition(userSymbolNode, Compound.class);
            Location location = userSymbolNode.getLocation();
            String symbol = userSymbolNode.getSymbol();
            Class<?> valueType = scriptScope.getDecoration(userSymbolNode, ValueType.class).getValueType();

            StoreNode irStoreNode = null;
            ExpressionNode irLoadNode = null;

            if (write || compound) {
                StoreVariableNode irStoreVariableNode = new StoreVariableNode(location);
                irStoreVariableNode.setExpressionType(read ? valueType : void.class);
                irStoreVariableNode.setStoreType(valueType);
                irStoreVariableNode.setName(symbol);
                irStoreNode = irStoreVariableNode;
            }

            if (write == false || compound) {
                LoadVariableNode irLoadVariableNode = new LoadVariableNode(location);
                irLoadVariableNode.setExpressionType(valueType);
                irLoadVariableNode.setName(symbol);
                irLoadNode = irLoadVariableNode;
            }

            scriptScope.putDecoration(userSymbolNode, new AccessDepth(0));
            irExpressionNode = buildLoadStore(0, location, false, null, null, irLoadNode, irStoreNode);
        } else {
            throw userSymbolNode.createError(new IllegalStateException("illegal tree structure"));
        }

        scriptScope.putDecoration(userSymbolNode, new IRNodeDecoration(irExpressionNode));
    }

    /**
     * This handles both load and store for dot accesses as necessary. This uses buildLoadStore to
     * stub out the appropriate load and store ir nodes.
     */
    @Override
    public void visitDot(EDot userDotNode, ScriptScope scriptScope) {
        ExpressionNode irExpressionNode;

        if (scriptScope.hasDecoration(userDotNode, StaticType.class)) {
            Class<?> staticType = scriptScope.getDecoration(userDotNode, StaticType.class).getStaticType();
            StaticNode staticNode = new StaticNode(userDotNode.getLocation());
            staticNode.setExpressionType(staticType);
            irExpressionNode = staticNode;
        } else {
            boolean read = scriptScope.getCondition(userDotNode, Read.class);
            boolean write = scriptScope.getCondition(userDotNode, Write.class);
            boolean compound = scriptScope.getCondition(userDotNode, Compound.class);
            Location location = userDotNode.getLocation();
            String index = userDotNode.getIndex();
            Class<?> valueType = scriptScope.getDecoration(userDotNode, ValueType.class).getValueType();
            ValueType prefixValueType = scriptScope.getDecoration(userDotNode.getPrefixNode(), ValueType.class);

            ExpressionNode irPrefixNode = (ExpressionNode)visit(userDotNode.getPrefixNode(), scriptScope);
            ExpressionNode irIndexNode = null;
            StoreNode irStoreNode = null;
            ExpressionNode irLoadNode = null;
            int accessDepth;

            if (prefixValueType != null && prefixValueType.getValueType().isArray()) {
                LoadDotArrayLengthNode irLoadDotArrayLengthNode = new LoadDotArrayLengthNode(location);
                irLoadDotArrayLengthNode.setExpressionType(int.class);
                irLoadNode = irLoadDotArrayLengthNode;

                accessDepth = 1;
            } else if (prefixValueType != null && prefixValueType.getValueType() == def.class) {
                if (write || compound) {
                    StoreDotDefNode irStoreDotDefNode = new StoreDotDefNode(location);
                    irStoreDotDefNode.setExpressionType(read ? valueType : void.class);
                    irStoreDotDefNode.setStoreType(valueType);
                    irStoreDotDefNode.setValue(index);
                    irStoreNode = irStoreDotDefNode;
                }

                if (write == false || compound) {
                    LoadDotDefNode irLoadDotDefNode = new LoadDotDefNode(location);
                    irLoadDotDefNode.setExpressionType(valueType);
                    irLoadDotDefNode.setValue(index);
                    irLoadNode = irLoadDotDefNode;
                }

                accessDepth = 1;
            } else if (scriptScope.hasDecoration(userDotNode, StandardPainlessField.class)) {
                PainlessField painlessField =
                        scriptScope.getDecoration(userDotNode, StandardPainlessField.class).getStandardPainlessField();

                if (write || compound) {
                    StoreDotNode irStoreDotNode = new StoreDotNode(location);
                    irStoreDotNode.setExpressionType(read ? valueType : void.class);
                    irStoreDotNode.setStoreType(valueType);
                    irStoreDotNode.setField(painlessField);
                    irStoreNode = irStoreDotNode;
                }

                if (write == false || compound) {
                    LoadDotNode irLoadDotNode = new LoadDotNode(location);
                    irLoadDotNode.setExpressionType(valueType);
                    irLoadDotNode.setField(painlessField);
                    irLoadNode = irLoadDotNode;
                }

                accessDepth = 1;
            } else if (scriptScope.getCondition(userDotNode, Shortcut.class)) {
                if (write || compound) {
                    StoreDotShortcutNode irStoreDotShortcutNode = new StoreDotShortcutNode(location);
                    irStoreDotShortcutNode.setExpressionType(read ? valueType : void.class);
                    irStoreDotShortcutNode.setStoreType(valueType);
                    irStoreDotShortcutNode.setSetter(
                            scriptScope.getDecoration(userDotNode, SetterPainlessMethod.class).getSetterPainlessMethod());
                    irStoreNode = irStoreDotShortcutNode;
                }

                if (write == false || compound) {
                    LoadDotShortcutNode irLoadDotShortcutNode = new LoadDotShortcutNode(location);
                    irLoadDotShortcutNode.setExpressionType(valueType);
                    irLoadDotShortcutNode.setGetter(
                            scriptScope.getDecoration(userDotNode, GetterPainlessMethod.class).getGetterPainlessMethod());
                    irLoadNode = irLoadDotShortcutNode;
                }

                accessDepth = 1;
            } else if (scriptScope.getCondition(userDotNode, MapShortcut.class)) {
                ConstantNode irConstantNode = new ConstantNode(location);
                irConstantNode.setExpressionType(String.class);
                irConstantNode.setConstant(index);
                irIndexNode = irConstantNode;

                if (write || compound) {
                    StoreMapShortcutNode irStoreMapShortcutNode = new StoreMapShortcutNode(location);
                    irStoreMapShortcutNode.setExpressionType(read ? valueType : void.class);
                    irStoreMapShortcutNode.setStoreType(valueType);
                    irStoreMapShortcutNode.setSetter(
                            scriptScope.getDecoration(userDotNode, SetterPainlessMethod.class).getSetterPainlessMethod());
                    irStoreNode = irStoreMapShortcutNode;
                }

                if (write == false || compound) {
                    LoadMapShortcutNode irLoadMapShortcutNode = new LoadMapShortcutNode(location);
                    irLoadMapShortcutNode.setExpressionType(valueType);
                    irLoadMapShortcutNode.setGetter(
                            scriptScope.getDecoration(userDotNode, GetterPainlessMethod.class).getGetterPainlessMethod());
                    irLoadNode = irLoadMapShortcutNode;
                }

                accessDepth = 2;
            } else if (scriptScope.getCondition(userDotNode, ListShortcut.class)) {
                ConstantNode irConstantNode = new ConstantNode(location);
                irConstantNode.setExpressionType(int.class);
                irConstantNode.setConstant(scriptScope.getDecoration(userDotNode, StandardConstant.class).getStandardConstant());
                irIndexNode = irConstantNode;

                if (write || compound) {
                    StoreListShortcutNode irStoreListShortcutNode = new StoreListShortcutNode(location);
                    irStoreListShortcutNode.setExpressionType(read ? valueType : void.class);
                    irStoreListShortcutNode.setStoreType(valueType);
                    irStoreListShortcutNode.setSetter(
                            scriptScope.getDecoration(userDotNode, SetterPainlessMethod.class).getSetterPainlessMethod());
                    irStoreNode = irStoreListShortcutNode;
                }

                if (write == false || compound) {
                    LoadListShortcutNode irLoadListShortcutNode = new LoadListShortcutNode(location);
                    irLoadListShortcutNode.setExpressionType(valueType);
                    irLoadListShortcutNode.setGetter(
                            scriptScope.getDecoration(userDotNode, GetterPainlessMethod.class).getGetterPainlessMethod());
                    irLoadNode = irLoadListShortcutNode;
                }

                accessDepth = 2;
            } else {
                throw userDotNode.createError(new IllegalStateException("illegal tree structure"));
            }

            scriptScope.putDecoration(userDotNode, new AccessDepth(accessDepth));
            irExpressionNode = buildLoadStore(
                    accessDepth, location, userDotNode.isNullSafe(), irPrefixNode, irIndexNode, irLoadNode, irStoreNode);
        }

        scriptScope.putDecoration(userDotNode, new IRNodeDecoration(irExpressionNode));
    }

    /**
     * This handles both load and store for brace accesses as necessary. This uses buildLoadStore to
     * stub out the appropriate load and store ir nodes.
     */
    @Override
    public void visitBrace(EBrace userBraceNode, ScriptScope scriptScope) {
        boolean read = scriptScope.getCondition(userBraceNode, Read.class);
        boolean write = scriptScope.getCondition(userBraceNode, Write.class);
        boolean compound = scriptScope.getCondition(userBraceNode, Compound.class);
        Location location = userBraceNode.getLocation();
        Class<?> valueType = scriptScope.getDecoration(userBraceNode, ValueType.class).getValueType();
        Class<?> prefixValueType = scriptScope.getDecoration(userBraceNode.getPrefixNode(), ValueType.class).getValueType();

        ExpressionNode irPrefixNode = (ExpressionNode)visit(userBraceNode.getPrefixNode(), scriptScope);
        ExpressionNode irIndexNode = injectCast(userBraceNode.getIndexNode(), scriptScope);
        StoreNode irStoreNode = null;
        ExpressionNode irLoadNode = null;

        if (prefixValueType.isArray()) {
            FlipArrayIndexNode irFlipArrayIndexNode = new FlipArrayIndexNode(userBraceNode.getIndexNode().getLocation());
            irFlipArrayIndexNode.setExpressionType(int.class);
            irFlipArrayIndexNode.setChildNode(irIndexNode);
            irIndexNode = irFlipArrayIndexNode;

            if (write || compound) {
                StoreBraceNode irStoreBraceNode = new StoreBraceNode(location);
                irStoreBraceNode.setExpressionType(read ? valueType : void.class);
                irStoreBraceNode.setStoreType(valueType);
                irStoreNode = irStoreBraceNode;
            }

            if (write == false || compound) {
                LoadBraceNode irLoadBraceNode = new LoadBraceNode(location);
                irLoadBraceNode.setExpressionType(valueType);
                irLoadNode = irLoadBraceNode;
            }
        } else if (prefixValueType == def.class) {
            Class<?> indexType = scriptScope.getDecoration(userBraceNode.getIndexNode(), ValueType.class).getValueType();
            FlipDefIndexNode irFlipDefIndexNode = new FlipDefIndexNode(userBraceNode.getIndexNode().getLocation());
            irFlipDefIndexNode.setExpressionType(indexType);
            irFlipDefIndexNode.setChildNode(irIndexNode);
            irIndexNode = irFlipDefIndexNode;

            if (write || compound) {
                StoreBraceDefNode irStoreBraceNode = new StoreBraceDefNode(location);
                irStoreBraceNode.setExpressionType(read ? valueType : void.class);
                irStoreBraceNode.setStoreType(valueType);
                irStoreBraceNode.setIndexType(indexType);
                irStoreNode = irStoreBraceNode;
            }

            if (write == false || compound) {
                LoadBraceDefNode irLoadBraceDefNode = new LoadBraceDefNode(location);
                irLoadBraceDefNode.setExpressionType(valueType);
                irLoadBraceDefNode.setIndexType(indexType);
                irLoadNode = irLoadBraceDefNode;
            }
        } else if (scriptScope.getCondition(userBraceNode, MapShortcut.class)) {
            if (write || compound) {
                StoreMapShortcutNode irStoreMapShortcutNode = new StoreMapShortcutNode(location);
                irStoreMapShortcutNode.setExpressionType(read ? valueType : void.class);
                irStoreMapShortcutNode.setStoreType(valueType);
                irStoreMapShortcutNode.setSetter(
                        scriptScope.getDecoration(userBraceNode, SetterPainlessMethod.class).getSetterPainlessMethod());
                irStoreNode = irStoreMapShortcutNode;
            }

            if (write == false || compound) {
                LoadMapShortcutNode irLoadMapShortcutNode = new LoadMapShortcutNode(location);
                irLoadMapShortcutNode.setExpressionType(scriptScope.getDecoration(userBraceNode, ValueType.class).getValueType());
                irLoadMapShortcutNode.setGetter(
                        scriptScope.getDecoration(userBraceNode, GetterPainlessMethod.class).getGetterPainlessMethod());
                irLoadNode = irLoadMapShortcutNode;
            }
        } else if (scriptScope.getCondition(userBraceNode, ListShortcut.class)) {
            FlipCollectionIndexNode irFlipCollectionIndexNode = new FlipCollectionIndexNode(userBraceNode.getIndexNode().getLocation());
            irFlipCollectionIndexNode.setExpressionType(int.class);
            irFlipCollectionIndexNode.setChildNode(irIndexNode);
            irIndexNode = irFlipCollectionIndexNode;

            if (write || compound) {
                StoreListShortcutNode irStoreListShortcutNode = new StoreListShortcutNode(location);
                irStoreListShortcutNode.setExpressionType(read ? valueType : void.class);
                irStoreListShortcutNode.setStoreType(valueType);
                irStoreListShortcutNode.setSetter(
                        scriptScope.getDecoration(userBraceNode, SetterPainlessMethod.class).getSetterPainlessMethod());
                irStoreNode = irStoreListShortcutNode;
            }

            if (write == false || compound) {
                LoadListShortcutNode irLoadListShortcutNode = new LoadListShortcutNode(location);
                irLoadListShortcutNode.setExpressionType(scriptScope.getDecoration(userBraceNode, ValueType.class).getValueType());
                irLoadListShortcutNode.setGetter(
                        scriptScope.getDecoration(userBraceNode, GetterPainlessMethod.class).getGetterPainlessMethod());
                irLoadNode = irLoadListShortcutNode;
            }
        } else {
            throw userBraceNode.createError(new IllegalStateException("illegal tree structure"));
        }

        scriptScope.putDecoration(userBraceNode, new AccessDepth(2));

        scriptScope.putDecoration(userBraceNode, new IRNodeDecoration(
                buildLoadStore(2, location, false, irPrefixNode, irIndexNode, irLoadNode, irStoreNode)));
    }

    @Override
    public void visitCall(ECall userCallNode, ScriptScope scriptScope) {
        ExpressionNode irExpressionNode;

        ValueType prefixValueType = scriptScope.getDecoration(userCallNode.getPrefixNode(), ValueType.class);

        if (prefixValueType != null && prefixValueType.getValueType() == def.class) {
            InvokeCallDefNode irCallSubDefNode = new InvokeCallDefNode(userCallNode.getLocation());

            for (AExpression userArgumentNode : userCallNode.getArgumentNodes()) {
                irCallSubDefNode.addArgumentNode((ExpressionNode)visit(userArgumentNode, scriptScope));
            }

            irCallSubDefNode.setExpressionType(scriptScope.getDecoration(userCallNode, ValueType.class).getValueType());
            irCallSubDefNode.setName(userCallNode.getMethodName());
            irExpressionNode = irCallSubDefNode;
        } else {
            Class<?> boxType;

            if (prefixValueType != null) {
                boxType = prefixValueType.getValueType();
            } else {
                boxType = scriptScope.getDecoration(userCallNode.getPrefixNode(), StaticType.class).getStaticType();
            }

            InvokeCallNode irInvokeCallNode = new InvokeCallNode(userCallNode.getLocation());
            PainlessMethod method = scriptScope.getDecoration(userCallNode, StandardPainlessMethod.class).getStandardPainlessMethod();
            Object[] injections = PainlessLookupUtility.buildInjections(method, scriptScope.getCompilerSettings().asMap());
            Class<?>[] parameterTypes = method.javaMethod.getParameterTypes();
            int augmentedOffset = method.javaMethod.getDeclaringClass() == method.targetClass ? 0 : 1;

            for (int i = 0; i < injections.length; i++) {
                Object injection = injections[i];
                Class<?> parameterType = parameterTypes[i + augmentedOffset];

                if (parameterType != PainlessLookupUtility.typeToUnboxedType(injection.getClass())) {
                    throw new IllegalStateException("illegal tree structure");
                }

                ConstantNode constantNode = new ConstantNode(userCallNode.getLocation());
                constantNode.setExpressionType(parameterType);
                constantNode.setConstant(injection);
                irInvokeCallNode.addArgumentNode(constantNode);
            }

            for (AExpression userCallArgumentNode : userCallNode.getArgumentNodes()) {
                irInvokeCallNode.addArgumentNode(injectCast(userCallArgumentNode, scriptScope));
            }

            irInvokeCallNode.setExpressionType(scriptScope.getDecoration(userCallNode, ValueType.class).getValueType());;
            irInvokeCallNode.setMethod(scriptScope.getDecoration(userCallNode, StandardPainlessMethod.class).getStandardPainlessMethod());
            irInvokeCallNode.setBox(boxType);
            irExpressionNode = irInvokeCallNode;
        }

        if (userCallNode.isNullSafe()) {
            NullSafeSubNode irNullSafeSubNode = new NullSafeSubNode(irExpressionNode.getLocation());
            irNullSafeSubNode.setChildNode(irExpressionNode);
            irNullSafeSubNode.setExpressionType(irExpressionNode.getExpressionType());
            irExpressionNode = irNullSafeSubNode;
        }

        BinaryImplNode irBinaryImplNode = new BinaryImplNode(irExpressionNode.getLocation());
        irBinaryImplNode.setLeftNode((ExpressionNode)visit(userCallNode.getPrefixNode(), scriptScope));
        irBinaryImplNode.setRightNode(irExpressionNode);
        irBinaryImplNode.setExpressionType(irExpressionNode.getExpressionType());

        scriptScope.putDecoration(userCallNode, new IRNodeDecoration(irBinaryImplNode));
    }
}

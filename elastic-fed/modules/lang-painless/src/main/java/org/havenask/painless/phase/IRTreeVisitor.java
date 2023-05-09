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

import org.havenask.painless.ir.BinaryMathNode;
import org.havenask.painless.ir.BinaryImplNode;
import org.havenask.painless.ir.BlockNode;
import org.havenask.painless.ir.BooleanNode;
import org.havenask.painless.ir.BreakNode;
import org.havenask.painless.ir.CastNode;
import org.havenask.painless.ir.CatchNode;
import org.havenask.painless.ir.ClassNode;
import org.havenask.painless.ir.ComparisonNode;
import org.havenask.painless.ir.ConditionalNode;
import org.havenask.painless.ir.ConstantNode;
import org.havenask.painless.ir.ContinueNode;
import org.havenask.painless.ir.DeclarationBlockNode;
import org.havenask.painless.ir.DeclarationNode;
import org.havenask.painless.ir.DefInterfaceReferenceNode;
import org.havenask.painless.ir.DoWhileLoopNode;
import org.havenask.painless.ir.DupNode;
import org.havenask.painless.ir.ElvisNode;
import org.havenask.painless.ir.FieldNode;
import org.havenask.painless.ir.FlipArrayIndexNode;
import org.havenask.painless.ir.FlipCollectionIndexNode;
import org.havenask.painless.ir.FlipDefIndexNode;
import org.havenask.painless.ir.ForEachLoopNode;
import org.havenask.painless.ir.ForEachSubArrayNode;
import org.havenask.painless.ir.ForEachSubIterableNode;
import org.havenask.painless.ir.ForLoopNode;
import org.havenask.painless.ir.FunctionNode;
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
import org.havenask.painless.ir.ReturnNode;
import org.havenask.painless.ir.StatementExpressionNode;
import org.havenask.painless.ir.StaticNode;
import org.havenask.painless.ir.StoreBraceDefNode;
import org.havenask.painless.ir.StoreBraceNode;
import org.havenask.painless.ir.StoreDotDefNode;
import org.havenask.painless.ir.StoreDotNode;
import org.havenask.painless.ir.StoreDotShortcutNode;
import org.havenask.painless.ir.StoreFieldMemberNode;
import org.havenask.painless.ir.StoreListShortcutNode;
import org.havenask.painless.ir.StoreMapShortcutNode;
import org.havenask.painless.ir.StoreVariableNode;
import org.havenask.painless.ir.StringConcatenationNode;
import org.havenask.painless.ir.ThrowNode;
import org.havenask.painless.ir.TryNode;
import org.havenask.painless.ir.TypedCaptureReferenceNode;
import org.havenask.painless.ir.TypedInterfaceReferenceNode;
import org.havenask.painless.ir.UnaryMathNode;
import org.havenask.painless.ir.WhileLoopNode;

public interface IRTreeVisitor<Scope> {

    void visitClass(ClassNode irClassNode, Scope scope);
    void visitFunction(FunctionNode irFunctionNode, Scope scope);
    void visitField(FieldNode irFieldNode, Scope scope);

    void visitBlock(BlockNode irBlockNode, Scope scope);
    void visitIf(IfNode irIfNode, Scope scope);
    void visitIfElse(IfElseNode irIfElseNode, Scope scope);
    void visitWhileLoop(WhileLoopNode irWhileLoopNode, Scope scope);
    void visitDoWhileLoop(DoWhileLoopNode irDoWhileLoopNode, Scope scope);
    void visitForLoop(ForLoopNode irForLoopNode, Scope scope);
    void visitForEachLoop(ForEachLoopNode irForEachLoopNode, Scope scope);
    void visitForEachSubArrayLoop(ForEachSubArrayNode irForEachSubArrayNode, Scope scope);
    void visitForEachSubIterableLoop(ForEachSubIterableNode irForEachSubIterableNode, Scope scope);
    void visitDeclarationBlock(DeclarationBlockNode irDeclarationBlockNode, Scope scope);
    void visitDeclaration(DeclarationNode irDeclarationNode, Scope scope);
    void visitReturn(ReturnNode irReturnNode, Scope scope);
    void visitStatementExpression(StatementExpressionNode irStatementExpressionNode, Scope scope);
    void visitTry(TryNode irTryNode, Scope scope);
    void visitCatch(CatchNode irCatchNode, Scope scope);
    void visitThrow(ThrowNode irThrowNode, Scope scope);
    void visitContinue(ContinueNode irContinueNode, Scope scope);
    void visitBreak(BreakNode irBreakNode, Scope scope);

    void visitBinaryImpl(BinaryImplNode irBinaryImplNode, Scope scope);
    void visitUnaryMath(UnaryMathNode irUnaryMathNode, Scope scope);
    void visitBinaryMath(BinaryMathNode irBinaryMathNode, Scope scope);
    void visitStringConcatenation(StringConcatenationNode irStringConcatenationNode, Scope scope);
    void visitBoolean(BooleanNode irBooleanNode, Scope scope);
    void visitComparison(ComparisonNode irComparisonNode, Scope scope);
    void visitCast(CastNode irCastNode, Scope scope);
    void visitInstanceof(InstanceofNode irInstanceofNode, Scope scope);
    void visitConditional(ConditionalNode irConditionalNode, Scope scope);
    void visitElvis(ElvisNode irElvisNode, Scope scope);
    void visitListInitialization(ListInitializationNode irListInitializationNode, Scope scope);
    void visitMapInitialization(MapInitializationNode irMapInitializationNode, Scope scope);
    void visitNewArray(NewArrayNode irNewArrayNode, Scope scope);
    void visitNewObject(NewObjectNode irNewObjectNode, Scope scope);
    void visitConstant(ConstantNode irConstantNode, Scope scope);
    void visitNull(NullNode irNullNode, Scope scope);
    void visitDefInterfaceReference(DefInterfaceReferenceNode irDefInterfaceReferenceNode, Scope scope);
    void visitTypedInterfaceReference(TypedInterfaceReferenceNode irTypedInterfaceReferenceNode, Scope scope);
    void visitTypeCaptureReference(TypedCaptureReferenceNode irTypedCaptureReferenceNode, Scope scope);
    void visitStatic(StaticNode irStaticNode, Scope scope);
    void visitLoadVariable(LoadVariableNode irLoadVariableNode, Scope scope);
    void visitNullSafeSub(NullSafeSubNode irNullSafeSubNode, Scope scope);
    void visitLoadDotArrayLengthNode(LoadDotArrayLengthNode irLoadDotArrayLengthNode, Scope scope);
    void visitLoadDotDef(LoadDotDefNode irLoadDotDefNode, Scope scope);
    void visitLoadDot(LoadDotNode irLoadDotNode, Scope scope);
    void visitLoadDotShortcut(LoadDotShortcutNode irDotSubShortcutNode, Scope scope);
    void visitLoadListShortcut(LoadListShortcutNode irLoadListShortcutNode, Scope scope);
    void visitLoadMapShortcut(LoadMapShortcutNode irLoadMapShortcutNode, Scope scope);
    void visitLoadFieldMember(LoadFieldMemberNode irLoadFieldMemberNode, Scope scope);
    void visitLoadBraceDef(LoadBraceDefNode irLoadBraceDefNode, Scope scope);
    void visitLoadBrace(LoadBraceNode irLoadBraceNode, Scope scope);
    void visitStoreVariable(StoreVariableNode irStoreVariableNode, Scope scope);
    void visitStoreDotDef(StoreDotDefNode irStoreDotDefNode, Scope scope);
    void visitStoreDot(StoreDotNode irStoreDotNode, Scope scope);
    void visitStoreDotShortcut(StoreDotShortcutNode irDotSubShortcutNode, Scope scope);
    void visitStoreListShortcut(StoreListShortcutNode irStoreListShortcutNode, Scope scope);
    void visitStoreMapShortcut(StoreMapShortcutNode irStoreMapShortcutNode, Scope scope);
    void visitStoreFieldMember(StoreFieldMemberNode irStoreFieldMemberNode, Scope scope);
    void visitStoreBraceDef(StoreBraceDefNode irStoreBraceDefNode, Scope scope);
    void visitStoreBrace(StoreBraceNode irStoreBraceNode, Scope scope);
    void visitInvokeCallDef(InvokeCallDefNode irInvokeCallDefNode, Scope scope);
    void visitInvokeCall(InvokeCallNode irInvokeCallNode, Scope scope);
    void visitInvokeCallMember(InvokeCallMemberNode irInvokeCallMemberNode, Scope scope);
    void visitFlipArrayIndex(FlipArrayIndexNode irFlipArrayIndexNode, Scope scope);
    void visitFlipCollectionIndex(FlipCollectionIndexNode irFlipCollectionIndexNode, Scope scope);
    void visitFlipDefIndex(FlipDefIndexNode irFlipDefIndexNode, Scope scope);
    void visitDup(DupNode irDupNode, Scope scope);
}

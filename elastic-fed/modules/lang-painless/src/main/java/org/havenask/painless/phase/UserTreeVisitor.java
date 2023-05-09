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

public interface UserTreeVisitor<Scope> {

    void visitClass(SClass userClassNode, Scope scope);
    void visitFunction(SFunction userFunctionNode, Scope scope);

    void visitBlock(SBlock userBlockNode, Scope scope);
    void visitIf(SIf userIfNode, Scope scope);
    void visitIfElse(SIfElse userIfElseNode, Scope scope);
    void visitWhile(SWhile userWhileNode, Scope scope);
    void visitDo(SDo userDoNode, Scope scope);
    void visitFor(SFor userForNode, Scope scope);
    void visitEach(SEach userEachNode, Scope scope);
    void visitDeclBlock(SDeclBlock userDeclBlockNode, Scope scope);
    void visitDeclaration(SDeclaration userDeclarationNode, Scope scope);
    void visitReturn(SReturn userReturnNode, Scope scope);
    void visitExpression(SExpression userExpressionNode, Scope scope);
    void visitTry(STry userTryNode, Scope scope);
    void visitCatch(SCatch userCatchNode, Scope scope);
    void visitThrow(SThrow userThrowNode, Scope scope);
    void visitContinue(SContinue userContinueNode, Scope scope);
    void visitBreak(SBreak userBreakNode, Scope scope);

    void visitAssignment(EAssignment userAssignmentNode, Scope scope);
    void visitUnary(EUnary userUnaryNode, Scope scope);
    void visitBinary(EBinary userBinaryNode, Scope scope);
    void visitBooleanComp(EBooleanComp userBooleanCompNode, Scope scope);
    void visitComp(EComp userCompNode, Scope scope);
    void visitExplicit(EExplicit userExplicitNode, Scope scope);
    void visitInstanceof(EInstanceof userInstanceofNode, Scope scope);
    void visitConditional(EConditional userConditionalNode, Scope scope);
    void visitElvis(EElvis userElvisNode, Scope scope);
    void visitListInit(EListInit userListInitNode, Scope scope);
    void visitMapInit(EMapInit userMapInitNode, Scope scope);
    void visitNewArray(ENewArray userNewArrayNode, Scope scope);
    void visitNewObj(ENewObj userNewObjectNode, Scope scope);
    void visitCallLocal(ECallLocal userCallLocalNode, Scope scope);
    void visitBooleanConstant(EBooleanConstant userBooleanConstantNode, Scope scope);
    void visitNumeric(ENumeric userNumericNode, Scope scope);
    void visitDecimal(EDecimal userDecimalNode, Scope scope);
    void visitString(EString userStringNode, Scope scope);
    void visitNull(ENull userNullNode, Scope scope);
    void visitRegex(ERegex userRegexNode, Scope scope);
    void visitLambda(ELambda userLambdaNode, Scope scope);
    void visitFunctionRef(EFunctionRef userFunctionRefNode, Scope scope);
    void visitNewArrayFunctionRef(ENewArrayFunctionRef userNewArrayFunctionRefNode, Scope scope);
    void visitSymbol(ESymbol userSymbolNode, Scope scope);
    void visitDot(EDot userDotNode, Scope scope);
    void visitBrace(EBrace userBraceNode, Scope scope);
    void visitCall(ECall userCallNode, Scope scope);
}

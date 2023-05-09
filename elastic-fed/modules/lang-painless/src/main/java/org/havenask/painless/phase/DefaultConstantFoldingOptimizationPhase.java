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

import org.havenask.painless.AnalyzerCaster;
import org.havenask.painless.Operation;
import org.havenask.painless.ir.BinaryMathNode;
import org.havenask.painless.ir.BinaryImplNode;
import org.havenask.painless.ir.BooleanNode;
import org.havenask.painless.ir.CastNode;
import org.havenask.painless.ir.ComparisonNode;
import org.havenask.painless.ir.ConditionalNode;
import org.havenask.painless.ir.ConstantNode;
import org.havenask.painless.ir.DeclarationNode;
import org.havenask.painless.ir.DoWhileLoopNode;
import org.havenask.painless.ir.DupNode;
import org.havenask.painless.ir.ElvisNode;
import org.havenask.painless.ir.ExpressionNode;
import org.havenask.painless.ir.FlipArrayIndexNode;
import org.havenask.painless.ir.FlipCollectionIndexNode;
import org.havenask.painless.ir.FlipDefIndexNode;
import org.havenask.painless.ir.ForEachSubArrayNode;
import org.havenask.painless.ir.ForEachSubIterableNode;
import org.havenask.painless.ir.ForLoopNode;
import org.havenask.painless.ir.IfElseNode;
import org.havenask.painless.ir.IfNode;
import org.havenask.painless.ir.InstanceofNode;
import org.havenask.painless.ir.InvokeCallDefNode;
import org.havenask.painless.ir.InvokeCallMemberNode;
import org.havenask.painless.ir.InvokeCallNode;
import org.havenask.painless.ir.ListInitializationNode;
import org.havenask.painless.ir.MapInitializationNode;
import org.havenask.painless.ir.NewArrayNode;
import org.havenask.painless.ir.NewObjectNode;
import org.havenask.painless.ir.NullNode;
import org.havenask.painless.ir.NullSafeSubNode;
import org.havenask.painless.ir.ReturnNode;
import org.havenask.painless.ir.StatementExpressionNode;
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
import org.havenask.painless.ir.UnaryMathNode;
import org.havenask.painless.ir.WhileLoopNode;
import org.havenask.painless.lookup.PainlessLookupUtility;

import java.util.function.Consumer;

/**
 * This optimization pass will perform the specified operation on two leafs nodes if they are both
 * constants. The additional overrides for visiting ir nodes in this class are required whenever
 * there is a child node that is an expression. The structure of the tree does not have a way
 * for a child node to introspect into its parent node, so to replace itself the parent node
 * must pass the child node's particular set method as method reference.
 */
public class DefaultConstantFoldingOptimizationPhase extends IRTreeBaseVisitor<Consumer<ExpressionNode>> {

    @Override
    public void visitIf(IfNode irIfNode, Consumer<ExpressionNode> scope) {
        irIfNode.getConditionNode().visit(this, irIfNode::setConditionNode);
        irIfNode.getBlockNode().visit(this, null);
    }

    @Override
    public void visitIfElse(IfElseNode irIfElseNode, Consumer<ExpressionNode> scope) {
        irIfElseNode.getConditionNode().visit(this, irIfElseNode::setConditionNode);
        irIfElseNode.getBlockNode().visit(this, null);
        irIfElseNode.getElseBlockNode().visit(this, null);
    }

    @Override
    public void visitWhileLoop(WhileLoopNode irWhileLoopNode, Consumer<ExpressionNode> scope) {
        if (irWhileLoopNode.getConditionNode() != null) {
            irWhileLoopNode.getConditionNode().visit(this, irWhileLoopNode::setConditionNode);
        }

        if (irWhileLoopNode.getBlockNode() != null) {
            irWhileLoopNode.getBlockNode().visit(this, null);
        }
    }

    @Override
    public void visitDoWhileLoop(DoWhileLoopNode irDoWhileLoopNode, Consumer<ExpressionNode> scope) {
        irDoWhileLoopNode.getBlockNode().visit(this, null);

        if (irDoWhileLoopNode.getConditionNode() != null) {
            irDoWhileLoopNode.getConditionNode().visit(this, irDoWhileLoopNode::setConditionNode);
        }
    }

    @Override
    public void visitForLoop(ForLoopNode irForLoopNode, Consumer<ExpressionNode> scope) {
        if (irForLoopNode.getInitializerNode() != null) {
            irForLoopNode.getInitializerNode().visit(this, irForLoopNode::setInitialzerNode);
        }

        if (irForLoopNode.getConditionNode() != null) {
            irForLoopNode.getConditionNode().visit(this, irForLoopNode::setConditionNode);
        }

        if (irForLoopNode.getAfterthoughtNode() != null) {
            irForLoopNode.getAfterthoughtNode().visit(this, irForLoopNode::setAfterthoughtNode);
        }

        if (irForLoopNode.getBlockNode() != null) {
            irForLoopNode.getBlockNode().visit(this, null);
        }
    }

    @Override
    public void visitForEachSubArrayLoop(ForEachSubArrayNode irForEachSubArrayNode, Consumer<ExpressionNode> scope) {
        irForEachSubArrayNode.getConditionNode().visit(this, irForEachSubArrayNode::setConditionNode);
        irForEachSubArrayNode.getBlockNode().visit(this, null);
    }

    @Override
    public void visitForEachSubIterableLoop(ForEachSubIterableNode irForEachSubIterableNode, Consumer<ExpressionNode> scope) {
        irForEachSubIterableNode.getConditionNode().visit(this, irForEachSubIterableNode::setConditionNode);
        irForEachSubIterableNode.getBlockNode().visit(this, null);
    }

    @Override
    public void visitDeclaration(DeclarationNode irDeclarationNode, Consumer<ExpressionNode> scope) {
        if (irDeclarationNode.getExpressionNode() != null) {
            irDeclarationNode.getExpressionNode().visit(this, irDeclarationNode::setExpressionNode);
        }
    }

    @Override
    public void visitReturn(ReturnNode irReturnNode, Consumer<ExpressionNode> scope) {
        if (irReturnNode.getExpressionNode() != null) {
            irReturnNode.getExpressionNode().visit(this, irReturnNode::setExpressionNode);
        }
    }

    @Override
    public void visitStatementExpression(StatementExpressionNode irStatementExpressionNode, Consumer<ExpressionNode> scope) {
        irStatementExpressionNode.getExpressionNode().visit(this, irStatementExpressionNode::setExpressionNode);
    }

    @Override
    public void visitThrow(ThrowNode irThrowNode, Consumer<ExpressionNode> scope) {
        irThrowNode.getExpressionNode().visit(this, irThrowNode::setExpressionNode);
    }

    @Override
    public void visitBinaryImpl(BinaryImplNode irBinaryImplNode, Consumer<ExpressionNode> scope) {
        irBinaryImplNode.getLeftNode().visit(this, irBinaryImplNode::setLeftNode);
        irBinaryImplNode.getRightNode().visit(this, irBinaryImplNode::setRightNode);
    }

    @Override
    public void visitUnaryMath(UnaryMathNode irUnaryMathNode, Consumer<ExpressionNode> scope) {
        irUnaryMathNode.getChildNode().visit(this, irUnaryMathNode::setChildNode);

        if (irUnaryMathNode.getChildNode() instanceof ConstantNode) {
            ConstantNode irConstantNode = (ConstantNode)irUnaryMathNode.getChildNode();
            Operation operation = irUnaryMathNode.getOperation();
            Class<?> type = irUnaryMathNode.getExpressionType();

            if (operation == Operation.SUB) {
                if (type == int.class) {
                    irConstantNode.setConstant(-(int)irConstantNode.getConstant());
                } else if (type == long.class) {
                    irConstantNode.setConstant(-(long)irConstantNode.getConstant());
                } else if (type == float.class) {
                    irConstantNode.setConstant(-(float)irConstantNode.getConstant());
                } else if (type == double.class) {
                    irConstantNode.setConstant(-(double)irConstantNode.getConstant());
                } else {
                    throw irUnaryMathNode.getLocation().createError(new IllegalStateException("constant folding error: " +
                            "unexpected type [" + PainlessLookupUtility.typeToCanonicalTypeName(type) + "] for " +
                            "unary operation [" + operation.symbol + "] on " +
                            "constant [" + irConstantNode.getConstant() + "]"));
                }

                scope.accept(irConstantNode);
            } else if (operation == Operation.BWNOT) {
                if (type == int.class) {
                    irConstantNode.setConstant(~(int)irConstantNode.getConstant());
                } else if (type == long.class) {
                    irConstantNode.setConstant(~(long)irConstantNode.getConstant());
                } else {
                    throw irUnaryMathNode.getLocation().createError(new IllegalStateException("constant folding error: " +
                            "unexpected type [" + PainlessLookupUtility.typeToCanonicalTypeName(type) + "] for " +
                            "unary operation [" + operation.symbol + "] on " +
                            "constant [" + irConstantNode.getConstant() + "]"));
                }

                scope.accept(irConstantNode);
            } else if (operation == Operation.NOT) {
                if (type == boolean.class) {
                    irConstantNode.setConstant(!(boolean)irConstantNode.getConstant());
                } else {
                    throw irUnaryMathNode.getLocation().createError(new IllegalStateException("constant folding error: " +
                            "unexpected type [" + PainlessLookupUtility.typeToCanonicalTypeName(type) + "] for " +
                            "unary operation [" + operation.symbol + "] on " +
                            "constant [" + irConstantNode.getConstant() + "]"));
                }

                scope.accept(irConstantNode);
            } else if (operation == Operation.ADD) {
                scope.accept(irConstantNode);
            }
        }
    }

    @Override
    public void visitBinaryMath(BinaryMathNode irBinaryMathNode, Consumer<ExpressionNode> scope) {
        irBinaryMathNode.getLeftNode().visit(this, irBinaryMathNode::setLeftNode);
        irBinaryMathNode.getRightNode().visit(this, irBinaryMathNode::setRightNode);

        if (irBinaryMathNode.getLeftNode() instanceof ConstantNode && irBinaryMathNode.getRightNode() instanceof ConstantNode) {
            ConstantNode irLeftConstantNode = (ConstantNode)irBinaryMathNode.getLeftNode();
            ConstantNode irRightConstantNode = (ConstantNode)irBinaryMathNode.getRightNode();
            Operation operation = irBinaryMathNode.getOperation();
            Class<?> type = irBinaryMathNode.getExpressionType();

            if (operation == Operation.MUL) {
                if (type == int.class) {
                    irLeftConstantNode.setConstant((int)irLeftConstantNode.getConstant() * (int)irRightConstantNode.getConstant());
                } else if (type == long.class) {
                    irLeftConstantNode.setConstant((long)irLeftConstantNode.getConstant() * (long)irRightConstantNode.getConstant());
                } else if (type == float.class) {
                    irLeftConstantNode.setConstant((float)irLeftConstantNode.getConstant() * (float)irRightConstantNode.getConstant());
                } else if (type == double.class) {
                    irLeftConstantNode.setConstant((double)irLeftConstantNode.getConstant() * (double)irRightConstantNode.getConstant());
                } else {
                    throw irBinaryMathNode.getLocation().createError(new IllegalStateException("constant folding error: " +
                            "unexpected type [" + PainlessLookupUtility.typeToCanonicalTypeName(type) + "] for " +
                            "binary operation [" + operation.symbol + "] on " +
                            "constants [" + irLeftConstantNode.getConstant() + "] and [" + irRightConstantNode.getConstant() + "]"));
                }

                scope.accept(irLeftConstantNode);
            } else if (operation == Operation.DIV) {
                try {
                    if (type == int.class) {
                        irLeftConstantNode.setConstant((int)irLeftConstantNode.getConstant() / (int)irRightConstantNode.getConstant());
                    } else if (type == long.class) {
                        irLeftConstantNode.setConstant((long)irLeftConstantNode.getConstant() / (long)irRightConstantNode.getConstant());
                    } else if (type == float.class) {
                        irLeftConstantNode.setConstant((float)irLeftConstantNode.getConstant() / (float)irRightConstantNode.getConstant());
                    } else if (type == double.class) {
                        irLeftConstantNode.setConstant(
                                (double)irLeftConstantNode.getConstant() / (double)irRightConstantNode.getConstant());
                    } else {
                        throw irBinaryMathNode.getLocation().createError(new IllegalStateException("constant folding error: " +
                                "unexpected type [" + PainlessLookupUtility.typeToCanonicalTypeName(type) + "] for " +
                                "binary operation [" + operation.symbol + "] on " +
                                "constants [" + irLeftConstantNode.getConstant() + "] and [" + irRightConstantNode.getConstant() + "]"));
                    }
                } catch (ArithmeticException ae) {
                    throw irBinaryMathNode.getLocation().createError(ae);
                }

                scope.accept(irLeftConstantNode);
            } else if (operation == Operation.REM) {
                try {
                    if (type == int.class) {
                        irLeftConstantNode.setConstant((int)irLeftConstantNode.getConstant() % (int)irRightConstantNode.getConstant());
                    } else if (type == long.class) {
                        irLeftConstantNode.setConstant((long)irLeftConstantNode.getConstant() % (long)irRightConstantNode.getConstant());
                    } else if (type == float.class) {
                        irLeftConstantNode.setConstant((float)irLeftConstantNode.getConstant() % (float)irRightConstantNode.getConstant());
                    } else if (type == double.class) {
                        irLeftConstantNode.setConstant(
                                (double)irLeftConstantNode.getConstant() % (double)irRightConstantNode.getConstant());
                    } else {
                        throw irBinaryMathNode.getLocation().createError(new IllegalStateException("constant folding error: " +
                                "unexpected type [" + PainlessLookupUtility.typeToCanonicalTypeName(type) + "] for " +
                                "binary operation [" + operation.symbol + "] on " +
                                "constants [" + irLeftConstantNode.getConstant() + "] and [" + irRightConstantNode.getConstant() + "]"));
                    }
                } catch (ArithmeticException ae) {
                    throw irBinaryMathNode.getLocation().createError(ae);
                }

                scope.accept(irLeftConstantNode);
            } else if (operation == Operation.ADD) {
                if (type == int.class) {
                    irLeftConstantNode.setConstant((int)irLeftConstantNode.getConstant() + (int)irRightConstantNode.getConstant());
                } else if (type == long.class) {
                    irLeftConstantNode.setConstant((long)irLeftConstantNode.getConstant() + (long)irRightConstantNode.getConstant());
                } else if (type == float.class) {
                    irLeftConstantNode.setConstant((float)irLeftConstantNode.getConstant() + (float)irRightConstantNode.getConstant());
                } else if (type == double.class) {
                    irLeftConstantNode.setConstant((double)irLeftConstantNode.getConstant() + (double)irRightConstantNode.getConstant());
                } else {
                    throw irBinaryMathNode.getLocation().createError(new IllegalStateException("constant folding error: " +
                            "unexpected type [" + PainlessLookupUtility.typeToCanonicalTypeName(type) + "] for " +
                            "binary operation [" + operation.symbol + "] on " +
                            "constants [" + irLeftConstantNode.getConstant() + "] and [" + irRightConstantNode.getConstant() + "]"));
                }

                scope.accept(irLeftConstantNode);
            } else if (operation == Operation.SUB) {
                if (type == int.class) {
                    irLeftConstantNode.setConstant((int)irLeftConstantNode.getConstant() - (int)irRightConstantNode.getConstant());
                } else if (type == long.class) {
                    irLeftConstantNode.setConstant((long)irLeftConstantNode.getConstant() - (long)irRightConstantNode.getConstant());
                } else if (type == float.class) {
                    irLeftConstantNode.setConstant((float)irLeftConstantNode.getConstant() - (float)irRightConstantNode.getConstant());
                } else if (type == double.class) {
                    irLeftConstantNode.setConstant((double)irLeftConstantNode.getConstant() - (double)irRightConstantNode.getConstant());
                } else {
                    throw irBinaryMathNode.getLocation().createError(new IllegalStateException("constant folding error: " +
                            "unexpected type [" + PainlessLookupUtility.typeToCanonicalTypeName(type) + "] for " +
                            "binary operation [" + operation.symbol + "] on " +
                            "constants [" + irLeftConstantNode.getConstant() + "] and [" + irRightConstantNode.getConstant() + "]"));
                }

                scope.accept(irLeftConstantNode);
            } else if (operation == Operation.LSH) {
                if (type == int.class) {
                    irLeftConstantNode.setConstant((int)irLeftConstantNode.getConstant() << (int)irRightConstantNode.getConstant());
                } else if (type == long.class) {
                    irLeftConstantNode.setConstant((long)irLeftConstantNode.getConstant() << (int)irRightConstantNode.getConstant());
                } else {
                    throw irBinaryMathNode.getLocation().createError(new IllegalStateException("constant folding error: " +
                            "unexpected type [" + PainlessLookupUtility.typeToCanonicalTypeName(type) + "] for " +
                            "binary operation [" + operation.symbol + "] on " +
                            "constants [" + irLeftConstantNode.getConstant() + "] and [" + irRightConstantNode.getConstant() + "]"));
                }

                scope.accept(irLeftConstantNode);
            } else if (operation == Operation.RSH) {
                if (type == int.class) {
                    irLeftConstantNode.setConstant((int)irLeftConstantNode.getConstant() >> (int)irRightConstantNode.getConstant());
                } else if (type == long.class) {
                    irLeftConstantNode.setConstant((long)irLeftConstantNode.getConstant() >> (int)irRightConstantNode.getConstant());
                } else {
                    throw irBinaryMathNode.getLocation().createError(new IllegalStateException("constant folding error: " +
                            "unexpected type [" + PainlessLookupUtility.typeToCanonicalTypeName(type) + "] for " +
                            "binary operation [" + operation.symbol + "] on " +
                            "constants [" + irLeftConstantNode.getConstant() + "] and [" + irRightConstantNode.getConstant() + "]"));
                }

                scope.accept(irLeftConstantNode);
            } else if (operation == Operation.USH) {
                if (type == int.class) {
                    irLeftConstantNode.setConstant((int)irLeftConstantNode.getConstant() >>> (int)irRightConstantNode.getConstant());
                } else if (type == long.class) {
                    irLeftConstantNode.setConstant((long)irLeftConstantNode.getConstant() >>> (int)irRightConstantNode.getConstant());
                } else {
                    throw irBinaryMathNode.getLocation().createError(new IllegalStateException("constant folding error: " +
                            "unexpected type [" + PainlessLookupUtility.typeToCanonicalTypeName(type) + "] for " +
                            "binary operation [" + operation.symbol + "] on " +
                            "constants [" + irLeftConstantNode.getConstant() + "] and [" + irRightConstantNode.getConstant() + "]"));
                }

                scope.accept(irLeftConstantNode);
            } else if (operation == Operation.BWAND) {
                if (type == int.class) {
                    irLeftConstantNode.setConstant((int)irLeftConstantNode.getConstant() & (int)irRightConstantNode.getConstant());
                } else if (type == long.class) {
                    irLeftConstantNode.setConstant((long)irLeftConstantNode.getConstant() & (long)irRightConstantNode.getConstant());
                } else {
                    throw irBinaryMathNode.getLocation().createError(new IllegalStateException("constant folding error: " +
                            "unexpected type [" + PainlessLookupUtility.typeToCanonicalTypeName(type) + "] for " +
                            "binary operation [" + operation.symbol + "] on " +
                            "constants [" + irLeftConstantNode.getConstant() + "] and [" + irRightConstantNode.getConstant() + "]"));
                }

                scope.accept(irLeftConstantNode);
            } else if (operation == Operation.XOR) {
                if (type == boolean.class) {
                    irLeftConstantNode.setConstant((boolean)irLeftConstantNode.getConstant() ^ (boolean)irRightConstantNode.getConstant());
                } else if (type == int.class) {
                    irLeftConstantNode.setConstant((int)irLeftConstantNode.getConstant() ^ (int)irRightConstantNode.getConstant());
                } else if (type == long.class) {
                    irLeftConstantNode.setConstant((long)irLeftConstantNode.getConstant() ^ (long)irRightConstantNode.getConstant());
                } else {
                    throw irBinaryMathNode.getLocation().createError(new IllegalStateException("constant folding error: " +
                            "unexpected type [" + PainlessLookupUtility.typeToCanonicalTypeName(type) + "] for " +
                            "binary operation [" + operation.symbol + "] on " +
                            "constants [" + irLeftConstantNode.getConstant() + "] and [" + irRightConstantNode.getConstant() + "]"));
                }

                scope.accept(irLeftConstantNode);
            } else if (operation == Operation.BWOR) {
                if (type == int.class) {
                    irLeftConstantNode.setConstant((int)irLeftConstantNode.getConstant() | (int)irRightConstantNode.getConstant());
                } else if (type == long.class) {
                    irLeftConstantNode.setConstant((long)irLeftConstantNode.getConstant() | (long)irRightConstantNode.getConstant());
                } else {
                    throw irBinaryMathNode.getLocation().createError(new IllegalStateException("constant folding error: " +
                            "unexpected type [" + PainlessLookupUtility.typeToCanonicalTypeName(type) + "] for " +
                            "binary operation [" + operation.symbol + "] on " +
                            "constants [" + irLeftConstantNode.getConstant() + "] and [" + irRightConstantNode.getConstant() + "]"));
                }

                scope.accept(irLeftConstantNode);
            }
        }
    }

    @Override
    public void visitStringConcatenation(StringConcatenationNode irStringConcatenationNode, Consumer<ExpressionNode> scope) {
        irStringConcatenationNode.getArgumentNodes().get(0).visit(this, (e) -> irStringConcatenationNode.getArgumentNodes().set(0, e));

        int i = 0;

        while (i < irStringConcatenationNode.getArgumentNodes().size() - 1) {
            ExpressionNode irLeftNode = irStringConcatenationNode.getArgumentNodes().get(i);
            ExpressionNode irRightNode = irStringConcatenationNode.getArgumentNodes().get(i + 1);

            int j = i;
            irRightNode.visit(this, (e) -> irStringConcatenationNode.getArgumentNodes().set(j + 1, e));

            if (irLeftNode instanceof ConstantNode && irRightNode instanceof ConstantNode) {
                ConstantNode irConstantNode = (ConstantNode)irLeftNode;
                irConstantNode.setConstant("" + irConstantNode.getConstant() + ((ConstantNode)irRightNode).getConstant());
                irConstantNode.setExpressionType(String.class);
                irStringConcatenationNode.getArgumentNodes().remove(i + 1);
            } else if (irLeftNode instanceof NullNode && irRightNode instanceof ConstantNode) {
                ConstantNode irConstantNode = (ConstantNode)irRightNode;
                irConstantNode.setConstant("" + null + ((ConstantNode)irRightNode).getConstant());
                irConstantNode.setExpressionType(String.class);
                irStringConcatenationNode.getArgumentNodes().remove(i);
            } else if (irLeftNode instanceof ConstantNode && irRightNode instanceof NullNode) {
                ConstantNode irConstantNode = (ConstantNode)irLeftNode;
                irConstantNode.setConstant("" + ((ConstantNode)irLeftNode).getConstant() + null);
                irConstantNode.setExpressionType(String.class);
                irStringConcatenationNode.getArgumentNodes().remove(i + 1);
            } else if (irLeftNode instanceof NullNode && irRightNode instanceof NullNode) {
                ConstantNode irConstantNode = new ConstantNode(irLeftNode.getLocation());
                irConstantNode.setConstant("" + null + null);
                irConstantNode.setExpressionType(String.class);
                irStringConcatenationNode.getArgumentNodes().set(i, irConstantNode);
                irStringConcatenationNode.getArgumentNodes().remove(i + 1);
            } else {
                i++;
            }
        }

        if (irStringConcatenationNode.getArgumentNodes().size() == 1) {
            ExpressionNode irArgumentNode = irStringConcatenationNode.getArgumentNodes().get(0);

            if (irArgumentNode instanceof ConstantNode) {
                scope.accept(irArgumentNode);
            }
        }
    }

    @Override
    public void visitBoolean(BooleanNode irBooleanNode, Consumer<ExpressionNode> scope) {
        irBooleanNode.getLeftNode().visit(this, irBooleanNode::setLeftNode);
        irBooleanNode.getRightNode().visit(this, irBooleanNode::setRightNode);

        if (irBooleanNode.getLeftNode() instanceof ConstantNode && irBooleanNode.getRightNode() instanceof ConstantNode) {
            ConstantNode irLeftConstantNode = (ConstantNode)irBooleanNode.getLeftNode();
            ConstantNode irRightConstantNode = (ConstantNode)irBooleanNode.getRightNode();
            Operation operation = irBooleanNode.getOperation();
            Class<?> type = irBooleanNode.getExpressionType();

            if (operation == Operation.AND) {
                if (type == boolean.class) {
                    irLeftConstantNode.setConstant((boolean)irLeftConstantNode.getConstant() && (boolean)irRightConstantNode.getConstant());
                } else {
                    throw irBooleanNode.getLocation().createError(new IllegalStateException("constant folding error: " +
                            "unexpected type [" + PainlessLookupUtility.typeToCanonicalTypeName(type) + "] for " +
                            "binary operation [" + operation.symbol + "] on " +
                            "constants [" + irLeftConstantNode.getConstant() + "] and [" + irRightConstantNode.getConstant() + "]"));
                }

                scope.accept(irLeftConstantNode);
            } else if (operation == Operation.OR) {
                if (type == boolean.class) {
                    irLeftConstantNode.setConstant((boolean)irLeftConstantNode.getConstant() || (boolean)irRightConstantNode.getConstant());
                } else {
                    throw irBooleanNode.getLocation().createError(new IllegalStateException("constant folding error: " +
                            "unexpected type [" + PainlessLookupUtility.typeToCanonicalTypeName(type) + "] for " +
                            "boolean operation [" + operation.symbol + "] on " +
                            "constants [" + irLeftConstantNode.getConstant() + "] and [" + irRightConstantNode.getConstant() + "]"));
                }

                scope.accept(irLeftConstantNode);
            }
        }
    }

    @Override
    public void visitComparison(ComparisonNode irComparisonNode, Consumer<ExpressionNode> scope) {
        irComparisonNode.getLeftNode().visit(this, irComparisonNode::setLeftNode);
        irComparisonNode.getRightNode().visit(this, irComparisonNode::setRightNode);

        if ((irComparisonNode.getLeftNode() instanceof ConstantNode || irComparisonNode.getLeftNode() instanceof NullNode)
                && (irComparisonNode.getRightNode() instanceof ConstantNode || irComparisonNode.getRightNode() instanceof NullNode)) {

            ConstantNode irLeftConstantNode =
                    irComparisonNode.getLeftNode() instanceof NullNode ? null : (ConstantNode)irComparisonNode.getLeftNode();
            ConstantNode irRightConstantNode =
                    irComparisonNode.getRightNode() instanceof NullNode ? null : (ConstantNode)irComparisonNode.getRightNode();
            Operation operation = irComparisonNode.getOperation();
            Class<?> type = irComparisonNode.getComparisonType();

            if (operation == Operation.EQ || operation == Operation.EQR) {
                if (type == boolean.class) {
                    irLeftConstantNode.setConstant((boolean)irLeftConstantNode.getConstant() == (boolean)irRightConstantNode.getConstant());
                } else if (type == int.class) {
                    irLeftConstantNode.setConstant((int)irLeftConstantNode.getConstant() == (int)irRightConstantNode.getConstant());
                } else if (type == long.class) {
                    irLeftConstantNode.setConstant((long)irLeftConstantNode.getConstant() == (long)irRightConstantNode.getConstant());
                } else if (type == float.class) {
                    irLeftConstantNode.setConstant((float)irLeftConstantNode.getConstant() == (float)irRightConstantNode.getConstant());
                } else if (type == double.class) {
                    irLeftConstantNode.setConstant((double)irLeftConstantNode.getConstant() == (double)irRightConstantNode.getConstant());
                } else if (irLeftConstantNode == null && irRightConstantNode == null) {
                    irLeftConstantNode = new ConstantNode(irComparisonNode.getLeftNode().getLocation());
                    irLeftConstantNode.setConstant(true);
                } else if (irLeftConstantNode == null || irRightConstantNode == null) {
                    irLeftConstantNode = new ConstantNode(irComparisonNode.getLeftNode().getLocation());
                    irLeftConstantNode.setConstant(false);
                } else {
                    if (operation == Operation.EQ) {
                        irLeftConstantNode.setConstant(irLeftConstantNode.getConstant().equals(irRightConstantNode.getConstant()));
                    } else {
                        irLeftConstantNode.setConstant(irLeftConstantNode.getConstant() == irRightConstantNode.getConstant());
                    }
                }

                irLeftConstantNode.setExpressionType(boolean.class);
                scope.accept(irLeftConstantNode);
            } else if (operation == Operation.NE || operation == Operation.NER) {
                if (type == boolean.class) {
                    irLeftConstantNode.setConstant((boolean)irLeftConstantNode.getConstant() != (boolean)irRightConstantNode.getConstant());
                } else if (type == int.class) {
                    irLeftConstantNode.setConstant((int)irLeftConstantNode.getConstant() != (int)irRightConstantNode.getConstant());
                } else if (type == long.class) {
                    irLeftConstantNode.setConstant((long)irLeftConstantNode.getConstant() != (long)irRightConstantNode.getConstant());
                } else if (type == float.class) {
                    irLeftConstantNode.setConstant((float)irLeftConstantNode.getConstant() != (float)irRightConstantNode.getConstant());
                } else if (type == double.class) {
                    irLeftConstantNode.setConstant((double)irLeftConstantNode.getConstant() != (double)irRightConstantNode.getConstant());
                } else if (irLeftConstantNode == null && irRightConstantNode == null) {
                    irLeftConstantNode = new ConstantNode(irComparisonNode.getLeftNode().getLocation());
                    irLeftConstantNode.setConstant(false);
                } else if (irLeftConstantNode == null || irRightConstantNode == null) {
                    irLeftConstantNode = new ConstantNode(irComparisonNode.getLeftNode().getLocation());
                    irLeftConstantNode.setConstant(true);
                } else {
                    if (operation == Operation.NE) {
                        irLeftConstantNode.setConstant(irLeftConstantNode.getConstant().equals(irRightConstantNode.getConstant()) == false);
                    } else {
                        irLeftConstantNode.setConstant(irLeftConstantNode.getConstant() != irRightConstantNode.getConstant());
                    }
                }

                irLeftConstantNode.setExpressionType(boolean.class);
                scope.accept(irLeftConstantNode);
            } else if (irLeftConstantNode != null && irRightConstantNode != null) {
                if (operation == Operation.GT) {
                    if (type == int.class) {
                        irLeftConstantNode.setConstant((int)irLeftConstantNode.getConstant() > (int)irRightConstantNode.getConstant());
                    } else if (type == long.class) {
                        irLeftConstantNode.setConstant((long)irLeftConstantNode.getConstant() > (long)irRightConstantNode.getConstant());
                    } else if (type == float.class) {
                        irLeftConstantNode.setConstant((float)irLeftConstantNode.getConstant() > (float)irRightConstantNode.getConstant());
                    } else if (type == double.class) {
                        irLeftConstantNode.setConstant(
                                (double)irLeftConstantNode.getConstant() > (double)irRightConstantNode.getConstant());
                    } else {
                        throw irComparisonNode.getLocation().createError(new IllegalStateException("constant folding error: " +
                                "unexpected type [" + PainlessLookupUtility.typeToCanonicalTypeName(type) + "] for " +
                                "comparison operation [" + operation.symbol + "] on " +
                                "constants [" + irLeftConstantNode.getConstant() + "] and [" + irRightConstantNode.getConstant() + "]"));
                    }

                    irLeftConstantNode.setExpressionType(boolean.class);
                    scope.accept(irLeftConstantNode);
                } else if (operation == Operation.GTE) {
                    if (type == int.class) {
                        irLeftConstantNode.setConstant((int)irLeftConstantNode.getConstant() >= (int)irRightConstantNode.getConstant());
                    } else if (type == long.class) {
                        irLeftConstantNode.setConstant((long)irLeftConstantNode.getConstant() >= (long)irRightConstantNode.getConstant());
                    } else if (type == float.class) {
                        irLeftConstantNode.setConstant((float)irLeftConstantNode.getConstant() >= (float)irRightConstantNode.getConstant());
                    } else if (type == double.class) {
                        irLeftConstantNode.setConstant(
                                (double)irLeftConstantNode.getConstant() >= (double)irRightConstantNode.getConstant());
                    } else {
                        throw irComparisonNode.getLocation().createError(new IllegalStateException("constant folding error: " +
                                "unexpected type [" + PainlessLookupUtility.typeToCanonicalTypeName(type) + "] for " +
                                "comparison operation [" + operation.symbol + "] on " +
                                "constants [" + irLeftConstantNode.getConstant() + "] and [" + irRightConstantNode.getConstant() + "]"));
                    }

                    irLeftConstantNode.setExpressionType(boolean.class);
                    scope.accept(irLeftConstantNode);
                } else if (operation == Operation.LT) {
                    if (type == int.class) {
                        irLeftConstantNode.setConstant((int)irLeftConstantNode.getConstant() < (int)irRightConstantNode.getConstant());
                    } else if (type == long.class) {
                        irLeftConstantNode.setConstant((long)irLeftConstantNode.getConstant() < (long)irRightConstantNode.getConstant());
                    } else if (type == float.class) {
                        irLeftConstantNode.setConstant((float)irLeftConstantNode.getConstant() < (float)irRightConstantNode.getConstant());
                    } else if (type == double.class) {
                        irLeftConstantNode.setConstant(
                                (double)irLeftConstantNode.getConstant() < (double)irRightConstantNode.getConstant());
                    } else {
                        throw irComparisonNode.getLocation().createError(new IllegalStateException("constant folding error: " +
                                "unexpected type [" + PainlessLookupUtility.typeToCanonicalTypeName(type) + "] for " +
                                "comparison operation [" + operation.symbol + "] on " +
                                "constants [" + irLeftConstantNode.getConstant() + "] and [" + irRightConstantNode.getConstant() + "]"));
                    }

                    irLeftConstantNode.setExpressionType(boolean.class);
                    scope.accept(irLeftConstantNode);
                } else if (operation == Operation.LTE) {
                    if (type == int.class) {
                        irLeftConstantNode.setConstant((int)irLeftConstantNode.getConstant() <= (int)irRightConstantNode.getConstant());
                    } else if (type == long.class) {
                        irLeftConstantNode.setConstant((long)irLeftConstantNode.getConstant() <= (long)irRightConstantNode.getConstant());
                    } else if (type == float.class) {
                        irLeftConstantNode.setConstant((float)irLeftConstantNode.getConstant() <= (float)irRightConstantNode.getConstant());
                    } else if (type == double.class) {
                        irLeftConstantNode.setConstant(
                                (double)irLeftConstantNode.getConstant() <= (double)irRightConstantNode.getConstant());
                    } else {
                        throw irComparisonNode.getLocation().createError(new IllegalStateException("constant folding error: " +
                                "unexpected type [" + PainlessLookupUtility.typeToCanonicalTypeName(type) + "] for " +
                                "comparison operation [" + operation.symbol + "] on " +
                                "constants [" + irLeftConstantNode.getConstant() + "] and [" + irRightConstantNode.getConstant() + "]"));
                    }

                    irLeftConstantNode.setExpressionType(boolean.class);
                    scope.accept(irLeftConstantNode);
                }
            }
        }
    }

    @Override
    public void visitCast(CastNode irCastNode, Consumer<ExpressionNode> scope) {
        irCastNode.getChildNode().visit(this, irCastNode::setChildNode);

        if (irCastNode.getChildNode() instanceof ConstantNode && PainlessLookupUtility.isConstantType(irCastNode.getExpressionType())) {
            ConstantNode irConstantNode = (ConstantNode)irCastNode.getChildNode();
            irConstantNode.setConstant(
                    AnalyzerCaster.constCast(irCastNode.getLocation(), irConstantNode.getConstant(), irCastNode.getCast()));
            irConstantNode.setExpressionType(irCastNode.getExpressionType());
            scope.accept(irConstantNode);
        }
    }

    @Override
    public void visitInstanceof(InstanceofNode irInstanceofNode, Consumer<ExpressionNode> scope) {
        irInstanceofNode.getChildNode().visit(this, irInstanceofNode::setChildNode);
    }

    @Override
    public void visitConditional(ConditionalNode irConditionalNode, Consumer<ExpressionNode> scope) {
        irConditionalNode.getConditionNode().visit(this, irConditionalNode::setConditionNode);
        irConditionalNode.getLeftNode().visit(this, irConditionalNode::setLeftNode);
        irConditionalNode.getRightNode().visit(this, irConditionalNode::setRightNode);
    }

    @Override
    public void visitElvis(ElvisNode irElvisNode, Consumer<ExpressionNode> scope) {
        irElvisNode.getLeftNode().visit(this, irElvisNode::setLeftNode);
        irElvisNode.getRightNode().visit(this, irElvisNode::setRightNode);
    }

    @Override
    public void visitListInitialization(ListInitializationNode irListInitializationNode, Consumer<ExpressionNode> scope) {
        for (int i = 0; i < irListInitializationNode.getArgumentNodes().size(); i++) {
            int j = i;
            irListInitializationNode.getArgumentNodes().get(i).visit(this, (e) -> irListInitializationNode.getArgumentNodes().set(j, e));
        }
    }

    @Override
    public void visitMapInitialization(MapInitializationNode irMapInitializationNode, Consumer<ExpressionNode> scope) {
        for (int i = 0; i < irMapInitializationNode.getKeyNodes().size(); i++) {
            int j = i;
            irMapInitializationNode.getKeyNode(i).visit(this, (e) -> irMapInitializationNode.getKeyNodes().set(j, e));
        }

        for (int i = 0; i < irMapInitializationNode.getValueNodes().size(); i++) {
            int j = i;
            irMapInitializationNode.getValueNode(i).visit(this, (e) -> irMapInitializationNode.getValueNodes().set(j, e));
        }
    }

    @Override
    public void visitNewArray(NewArrayNode irNewArrayNode, Consumer<ExpressionNode> scope) {
        for (int i = 0; i < irNewArrayNode.getArgumentNodes().size(); i++) {
            int j = i;
            irNewArrayNode.getArgumentNodes().get(i).visit(this, (e) -> irNewArrayNode.getArgumentNodes().set(j, e));
        }
    }

    @Override
    public void visitNewObject(NewObjectNode irNewObjectNode, Consumer<ExpressionNode> scope) {
        for (int i = 0; i < irNewObjectNode.getArgumentNodes().size(); i++) {
            int j = i;
            irNewObjectNode.getArgumentNodes().get(i).visit(this, (e) -> irNewObjectNode.getArgumentNodes().set(j, e));
        }
    }

    @Override
    public void visitNullSafeSub(NullSafeSubNode irNullSafeSubNode, Consumer<ExpressionNode> scope) {
        irNullSafeSubNode.getChildNode().visit(this, irNullSafeSubNode::setChildNode);
    }

    @Override
    public void visitStoreVariable(StoreVariableNode irStoreVariableNode, Consumer<ExpressionNode> scope) {
        irStoreVariableNode.getChildNode().visit(this, irStoreVariableNode::setChildNode);
    }

    @Override
    public void visitStoreDotDef(StoreDotDefNode irStoreDotDefNode, Consumer<ExpressionNode> scope) {
        irStoreDotDefNode.getChildNode().visit(this, irStoreDotDefNode::setChildNode);
    }

    @Override
    public void visitStoreDot(StoreDotNode irStoreDotNode, Consumer<ExpressionNode> scope) {
        irStoreDotNode.getChildNode().visit(this, irStoreDotNode::setChildNode);
    }

    @Override
    public void visitStoreDotShortcut(StoreDotShortcutNode irDotSubShortcutNode, Consumer<ExpressionNode> scope) {
        irDotSubShortcutNode.getChildNode().visit(this, irDotSubShortcutNode::setChildNode);
    }

    @Override
    public void visitStoreListShortcut(StoreListShortcutNode irStoreListShortcutNode, Consumer<ExpressionNode> scope) {
        irStoreListShortcutNode.getChildNode().visit(this, irStoreListShortcutNode::setChildNode);
    }

    @Override
    public void visitStoreMapShortcut(StoreMapShortcutNode irStoreMapShortcutNode, Consumer<ExpressionNode> scope) {
        irStoreMapShortcutNode.getChildNode().visit(this, irStoreMapShortcutNode::setChildNode);
    }

    @Override
    public void visitStoreFieldMember(StoreFieldMemberNode irStoreFieldMemberNode, Consumer<ExpressionNode> scope) {
        irStoreFieldMemberNode.getChildNode().visit(this, irStoreFieldMemberNode::setChildNode);
    }

    @Override
    public void visitStoreBraceDef(StoreBraceDefNode irStoreBraceDefNode, Consumer<ExpressionNode> scope) {
        irStoreBraceDefNode.getChildNode().visit(this, irStoreBraceDefNode::setChildNode);
    }

    @Override
    public void visitStoreBrace(StoreBraceNode irStoreBraceNode, Consumer<ExpressionNode> scope) {
        irStoreBraceNode.getChildNode().visit(this, irStoreBraceNode::setChildNode);
    }

    @Override
    public void visitInvokeCallDef(InvokeCallDefNode irInvokeCallDefNode, Consumer<ExpressionNode> scope) {
        for (int i = 0; i < irInvokeCallDefNode.getArgumentNodes().size(); i++) {
            int j = i;
            irInvokeCallDefNode.getArgumentNodes().get(i).visit(this, (e) -> irInvokeCallDefNode.getArgumentNodes().set(j, e));
        }
    }

    @Override
    public void visitInvokeCall(InvokeCallNode irInvokeCallNode, Consumer<ExpressionNode> scope) {
        for (int i = 0; i < irInvokeCallNode.getArgumentNodes().size(); i++) {
            int j = i;
            irInvokeCallNode.getArgumentNodes().get(i).visit(this, (e) -> irInvokeCallNode.getArgumentNodes().set(j, e));
        }
    }

    @Override
    public void visitInvokeCallMember(InvokeCallMemberNode irInvokeCallMemberNode, Consumer<ExpressionNode> scope) {
        for (int i = 0; i < irInvokeCallMemberNode.getArgumentNodes().size(); i++) {
            int j = i;
            irInvokeCallMemberNode.getArgumentNodes().get(i).visit(this, (e) -> irInvokeCallMemberNode.getArgumentNodes().set(j, e));
        }
    }

    @Override
    public void visitFlipArrayIndex(FlipArrayIndexNode irFlipArrayIndexNode, Consumer<ExpressionNode> scope) {
        irFlipArrayIndexNode.getChildNode().visit(this, irFlipArrayIndexNode::setChildNode);
    }

    @Override
    public void visitFlipCollectionIndex(FlipCollectionIndexNode irFlipCollectionIndexNode, Consumer<ExpressionNode> scope) {
        irFlipCollectionIndexNode.getChildNode().visit(this, irFlipCollectionIndexNode::setChildNode);
    }

    @Override
    public void visitFlipDefIndex(FlipDefIndexNode irFlipDefIndexNode, Consumer<ExpressionNode> scope) {
        irFlipDefIndexNode.getChildNode().visit(this, irFlipDefIndexNode::setChildNode);
    }

    @Override
    public void visitDup(DupNode irDupNode, Consumer<ExpressionNode> scope) {
        irDupNode.getChildNode().visit(this, irDupNode::setChildNode);
    }
}

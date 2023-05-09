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
import org.havenask.painless.Operation;
import org.havenask.painless.WriterConstants;
import org.havenask.painless.api.Augmentation;
import org.havenask.painless.lookup.PainlessLookupUtility;
import org.havenask.painless.lookup.def;
import org.havenask.painless.phase.IRTreeVisitor;
import org.havenask.painless.symbol.WriteScope;

import java.util.regex.Matcher;

public class BinaryMathNode extends BinaryNode {

    /* ---- begin node data ---- */

    private Operation operation;
    private Class<?> binaryType;
    private Class<?> shiftType;
    private int flags;
    // TODO(stu): DefaultUserTreeToIRTree -> visitRegex should have compiler settings in script set.  set it
    private int regexLimit;

    public void setOperation(Operation operation) {
        this.operation = operation;
    }

    public Operation getOperation() {
        return operation;
    }

    public void setBinaryType(Class<?> binaryType) {
        this.binaryType = binaryType;
    }

    public Class<?> getBinaryType() {
        return binaryType;
    }

    public String getBinaryCanonicalTypeName() {
        return PainlessLookupUtility.typeToCanonicalTypeName(binaryType);
    }

    public void setShiftType(Class<?> shiftType) {
        this.shiftType = shiftType;
    }

    public Class<?> getShiftType() {
        return shiftType;
    }

    public String getShiftCanonicalTypeName() {
        return PainlessLookupUtility.typeToCanonicalTypeName(shiftType);
    }

    public void setFlags(int flags) {
        this.flags = flags;
    }

    public int getFlags() {
        return flags;
    }

    public void setRegexLimit(int regexLimit) {
        this.regexLimit = regexLimit;
    }

    public int getRegexLimit() {
        return regexLimit;
    }

    /* ---- end node data, begin visitor ---- */

    @Override
    public <Scope> void visit(IRTreeVisitor<Scope> irTreeVisitor, Scope scope) {
        irTreeVisitor.visitBinaryMath(this, scope);
    }

    @Override
    public <Scope> void visitChildren(IRTreeVisitor<Scope> irTreeVisitor, Scope scope) {
        getLeftNode().visit(irTreeVisitor, scope);
        getRightNode().visit(irTreeVisitor, scope);
    }

    /* ---- end visitor ---- */

    public BinaryMathNode(Location location) {
        super(location);
    }

    @Override
    protected void write(ClassWriter classWriter, MethodWriter methodWriter, WriteScope writeScope) {
        methodWriter.writeDebugInfo(getLocation());

        if (operation == Operation.FIND || operation == Operation.MATCH) {
            getRightNode().write(classWriter, methodWriter, writeScope);
            methodWriter.push(regexLimit);
            getLeftNode().write(classWriter, methodWriter, writeScope);
            methodWriter.invokeStatic(org.objectweb.asm.Type.getType(Augmentation.class), WriterConstants.PATTERN_MATCHER);

            if (operation == Operation.FIND) {
                methodWriter.invokeVirtual(org.objectweb.asm.Type.getType(Matcher.class), WriterConstants.MATCHER_FIND);
            } else if (operation == Operation.MATCH) {
                methodWriter.invokeVirtual(org.objectweb.asm.Type.getType(Matcher.class), WriterConstants.MATCHER_MATCHES);
            } else {
                throw new IllegalStateException("unexpected binary math operation [" + operation + "] " +
                        "for type [" + getExpressionCanonicalTypeName() + "]");
            }
        } else {
            getLeftNode().write(classWriter, methodWriter, writeScope);
            getRightNode().write(classWriter, methodWriter, writeScope);

            if (binaryType == def.class || (shiftType != null && shiftType == def.class)) {
                methodWriter.writeDynamicBinaryInstruction(getLocation(),
                        getExpressionType(), getLeftNode().getExpressionType(), getRightNode().getExpressionType(), operation, flags);
            } else {
                methodWriter.writeBinaryInstruction(getLocation(), getExpressionType(), operation);
            }
        }
    }
}

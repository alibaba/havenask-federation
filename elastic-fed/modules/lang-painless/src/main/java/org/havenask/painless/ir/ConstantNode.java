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
import org.havenask.painless.phase.IRTreeVisitor;
import org.havenask.painless.symbol.WriteScope;

public class ConstantNode extends ExpressionNode {

    /* ---- begin node data ---- */

    private Object constant;

    public void setConstant(Object constant) {
        this.constant = constant;
    }

    public Object getConstant() {
        return constant;
    }

    /* ---- end node data, begin visitor ---- */

    @Override
    public <Scope> void visit(IRTreeVisitor<Scope> irTreeVisitor, Scope scope) {
        irTreeVisitor.visitConstant(this, scope);
    }

    @Override
    public <Scope> void visitChildren(IRTreeVisitor<Scope> irTreeVisitor, Scope scope) {
        // do nothing; terminal node
    }

    /* ---- end visitor ---- */

    public ConstantNode(Location location) {
        super(location);
    }

    @Override
    protected void write(ClassWriter classWriter, MethodWriter methodWriter, WriteScope writeScope) {
        if      (constant instanceof String)    methodWriter.push((String)constant);
        else if (constant instanceof Double)    methodWriter.push((double)constant);
        else if (constant instanceof Float)     methodWriter.push((float)constant);
        else if (constant instanceof Long)      methodWriter.push((long)constant);
        else if (constant instanceof Integer)   methodWriter.push((int)constant);
        else if (constant instanceof Character) methodWriter.push((char)constant);
        else if (constant instanceof Short)     methodWriter.push((short)constant);
        else if (constant instanceof Byte)      methodWriter.push((byte)constant);
        else if (constant instanceof Boolean)   methodWriter.push((boolean)constant);
        else {
            throw new IllegalStateException("unexpected constant [" + constant + "]");
        }
    }
}

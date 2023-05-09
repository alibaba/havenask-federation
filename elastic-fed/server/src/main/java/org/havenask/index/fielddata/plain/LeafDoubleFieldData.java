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

package org.havenask.index.fielddata.plain;

import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.Accountable;
import org.havenask.index.fielddata.FieldData;
import org.havenask.index.fielddata.LeafNumericFieldData;
import org.havenask.index.fielddata.ScriptDocValues;
import org.havenask.index.fielddata.SortedBinaryDocValues;
import org.havenask.index.fielddata.SortedNumericDoubleValues;
import org.havenask.index.mapper.DocValueFetcher;
import org.havenask.search.DocValueFormat;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;


/**
 * Specialization of {@link LeafNumericFieldData} for floating-point numerics.
 */
public abstract class LeafDoubleFieldData implements LeafNumericFieldData {

    private final long ramBytesUsed;

    protected LeafDoubleFieldData(long ramBytesUsed) {
        this.ramBytesUsed = ramBytesUsed;
    }

    @Override
    public long ramBytesUsed() {
        return ramBytesUsed;
    }

    @Override
    public final ScriptDocValues<Double> getScriptValues() {
        return new ScriptDocValues.Doubles(getDoubleValues());
    }

    @Override
    public final SortedBinaryDocValues getBytesValues() {
        return FieldData.toString(getDoubleValues());
    }

    @Override
    public final SortedNumericDocValues getLongValues() {
        return FieldData.castToLong(getDoubleValues());
    }

    public static LeafNumericFieldData empty(final int maxDoc) {
        return new LeafDoubleFieldData(0) {

            @Override
            public SortedNumericDoubleValues getDoubleValues() {
                return FieldData.emptySortedNumericDoubles();
            }

            @Override
            public Collection<Accountable> getChildResources() {
                return Collections.emptyList();
            }

        };
    }

    @Override
    public DocValueFetcher.Leaf getLeafValueFetcher(DocValueFormat format) {
        SortedNumericDoubleValues values = getDoubleValues();
        return new DocValueFetcher.Leaf() {
            @Override
            public boolean advanceExact(int docId) throws IOException {
                return values.advanceExact(docId);
            }

            @Override
            public int docValueCount() throws IOException {
                return values.docValueCount();
            }

            @Override
            public Object nextValue() throws IOException {
                return format.format(values.nextValue());
            }
        };
    }

    @Override
    public void close() {
    }

}

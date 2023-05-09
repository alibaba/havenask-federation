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

package org.havenask.index.fielddata.fieldcomparator;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.comparators.LongComparator;
import org.apache.lucene.util.BitSet;
import org.havenask.common.Nullable;
import org.havenask.common.util.BigArrays;
import org.havenask.index.fielddata.LeafNumericFieldData;
import org.havenask.index.fielddata.FieldData;
import org.havenask.index.fielddata.IndexFieldData;
import org.havenask.index.fielddata.IndexNumericFieldData;
import org.havenask.index.fielddata.plain.SortedNumericIndexFieldData;
import org.havenask.search.DocValueFormat;
import org.havenask.search.MultiValueMode;
import org.havenask.search.sort.BucketedSort;
import org.havenask.search.sort.SortOrder;

import java.io.IOException;
import java.util.function.Function;

/**
 * Comparator source for long values.
 */
public class LongValuesComparatorSource extends IndexFieldData.XFieldComparatorSource {

    private final IndexNumericFieldData indexFieldData;
    private final Function<SortedNumericDocValues, SortedNumericDocValues> converter;

    public LongValuesComparatorSource(IndexNumericFieldData indexFieldData, @Nullable Object missingValue,
                                      MultiValueMode sortMode, Nested nested) {
        this(indexFieldData, missingValue, sortMode, nested, null);
    }

    public LongValuesComparatorSource(IndexNumericFieldData indexFieldData, @Nullable Object missingValue,
                                      MultiValueMode sortMode, Nested nested,
                                      Function<SortedNumericDocValues, SortedNumericDocValues> converter) {
        super(missingValue, sortMode, nested);
        this.indexFieldData = indexFieldData;
        this.converter = converter;
    }

    @Override
    public SortField.Type reducedType() {
        return SortField.Type.LONG;
    }

    private SortedNumericDocValues loadDocValues(LeafReaderContext context) {
        final LeafNumericFieldData data = indexFieldData.load(context);
        SortedNumericDocValues values;
        if (data instanceof SortedNumericIndexFieldData.NanoSecondFieldData) {
            values = ((SortedNumericIndexFieldData.NanoSecondFieldData) data).getLongValuesAsNanos();
        } else {
            values = data.getLongValues();
        }
        return converter != null ? converter.apply(values) : values;
    }

    private NumericDocValues getNumericDocValues(LeafReaderContext context, long missingValue) throws IOException {
        final SortedNumericDocValues values = loadDocValues(context);
        if (nested == null) {
            return FieldData.replaceMissing(sortMode.select(values), missingValue);
        }
        final BitSet rootDocs = nested.rootDocs(context);
        final DocIdSetIterator innerDocs = nested.innerDocs(context);
        final int maxChildren = nested.getNestedSort() != null ? nested.getNestedSort().getMaxChildren() : Integer.MAX_VALUE;
        return sortMode.select(values, missingValue, rootDocs, innerDocs, context.reader().maxDoc(), maxChildren);
    }

    @Override
    public FieldComparator<?> newComparator(String fieldname, int numHits, int sortPos, boolean reversed) {
        assert indexFieldData == null || fieldname.equals(indexFieldData.getFieldName());

        final long lMissingValue = (Long) missingObject(missingValue, reversed);
        // NOTE: it's important to pass null as a missing value in the constructor so that
        // the comparator doesn't check docsWithField since we replace missing values in select()
        return new LongComparator(numHits, null, null, reversed, sortPos) {
            @Override
            public LeafFieldComparator getLeafComparator(LeafReaderContext context) throws IOException {
                return new LongLeafComparator(context) {
                    @Override
                    protected NumericDocValues getNumericDocValues(LeafReaderContext context, String field) throws IOException {
                        return LongValuesComparatorSource.this.getNumericDocValues(context, lMissingValue);
                    }
                };
            }
        };
    }

    @Override
    public BucketedSort newBucketedSort(BigArrays bigArrays, SortOrder sortOrder, DocValueFormat format,
            int bucketSize, BucketedSort.ExtraData extra) {
        return new BucketedSort.ForLongs(bigArrays, sortOrder, format, bucketSize, extra) {
            private final long lMissingValue = (Long) missingObject(missingValue, sortOrder == SortOrder.DESC);

            @Override
            public Leaf forLeaf(LeafReaderContext ctx) throws IOException {
                return new Leaf(ctx) {
                    private final NumericDocValues docValues = getNumericDocValues(ctx, lMissingValue);
                    private long docValue;

                    @Override
                    protected boolean advanceExact(int doc) throws IOException {
                        if (docValues.advanceExact(doc)) {
                            docValue = docValues.longValue();
                            return true;
                        }
                        return false;
                    }

                    @Override
                    protected long docValue() {
                        return docValue;
                    }
                };
            }
        };
    }
}

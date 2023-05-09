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

package org.havenask.index.fielddata;

import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSelector;
import org.apache.lucene.search.SortedNumericSortField;
import org.havenask.common.Nullable;
import org.havenask.common.time.DateUtils;
import org.havenask.common.util.BigArrays;
import org.havenask.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.havenask.index.fielddata.fieldcomparator.DoubleValuesComparatorSource;
import org.havenask.index.fielddata.fieldcomparator.FloatValuesComparatorSource;
import org.havenask.index.fielddata.fieldcomparator.LongValuesComparatorSource;
import org.havenask.search.DocValueFormat;
import org.havenask.search.MultiValueMode;
import org.havenask.search.aggregations.support.CoreValuesSourceType;
import org.havenask.search.aggregations.support.ValuesSourceType;
import org.havenask.search.sort.BucketedSort;
import org.havenask.search.sort.SortOrder;

import java.io.IOException;
import java.util.function.LongUnaryOperator;

/**
 * Base class for numeric field data.
 */
public abstract class IndexNumericFieldData implements IndexFieldData<LeafNumericFieldData> {
    /**
     * The type of number.
     */
    public enum NumericType {
        BOOLEAN(false, SortField.Type.LONG, CoreValuesSourceType.BOOLEAN),
        BYTE(false, SortField.Type.LONG, CoreValuesSourceType.NUMERIC),
        SHORT(false, SortField.Type.LONG, CoreValuesSourceType.NUMERIC),
        INT(false, SortField.Type.LONG, CoreValuesSourceType.NUMERIC),
        LONG(false, SortField.Type.LONG, CoreValuesSourceType.NUMERIC),
        DATE(false, SortField.Type.LONG, CoreValuesSourceType.DATE),
        DATE_NANOSECONDS(false, SortField.Type.LONG, CoreValuesSourceType.DATE),
        HALF_FLOAT(true, SortField.Type.LONG, CoreValuesSourceType.NUMERIC),
        FLOAT(true, SortField.Type.FLOAT, CoreValuesSourceType.NUMERIC),
        DOUBLE(true, SortField.Type.DOUBLE, CoreValuesSourceType.NUMERIC);

        private final boolean floatingPoint;
        private final ValuesSourceType valuesSourceType;
        private final SortField.Type sortFieldType;

        NumericType(boolean floatingPoint, SortField.Type sortFieldType, ValuesSourceType valuesSourceType) {
            this.floatingPoint = floatingPoint;
            this.sortFieldType = sortFieldType;
            this.valuesSourceType = valuesSourceType;
        }

        public final boolean isFloatingPoint() {
            return floatingPoint;
        }
        public final ValuesSourceType getValuesSourceType() {
            return valuesSourceType;
        }
    }

    /**
     * The numeric type of this number.
     */
    public abstract NumericType getNumericType();

    /**
     * Returns the {@link SortField} to used for sorting.
     * Values are casted to the provided <code>targetNumericType</code> type if it doesn't
     * match the field's <code>numericType</code>.
     */
    public final SortField sortField(
        NumericType targetNumericType,
        Object missingValue,
        MultiValueMode sortMode,
        Nested nested,
        boolean reverse
    ) {
        XFieldComparatorSource source = comparatorSource(targetNumericType, missingValue, sortMode, nested);

        /*
         * Use a SortField with the custom comparator logic if required because
         * 1. The underlying data source needs it.
         * 2. We need to read the value from a nested field.
         * 3. We Aren't using max or min to resolve the duplicates.
         * 4. We have to cast the results to another type.
         */
        if (sortRequiresCustomComparator()
                || nested != null
                || (sortMode != MultiValueMode.MAX && sortMode != MultiValueMode.MIN)
                || targetNumericType != getNumericType()) {
            return new SortField(getFieldName(), source, reverse);
        }

        SortedNumericSelector.Type selectorType = sortMode == MultiValueMode.MAX ?
            SortedNumericSelector.Type.MAX : SortedNumericSelector.Type.MIN;
        SortField sortField = new SortedNumericSortField(getFieldName(), getNumericType().sortFieldType, reverse, selectorType);
        sortField.setMissingValue(source.missingObject(missingValue, reverse));
        return sortField;
    }

    /**
     * Does {@link #sortField} require a custom comparator because of the way
     * the data is stored in doc values ({@code true}) or are the docs values
     * stored such that they can be sorted without decoding ({@code false}).
     */
    protected abstract boolean sortRequiresCustomComparator();

    @Override
    public final SortField sortField(Object missingValue, MultiValueMode sortMode, Nested nested, boolean reverse) {
        return sortField(getNumericType(), missingValue, sortMode, nested, reverse);
    }

    /**
     * Builds a {@linkplain BucketedSort} for the {@code targetNumericType},
     * casting the values if their native type doesn't match.
     */
    public final BucketedSort newBucketedSort(NumericType targetNumericType, BigArrays bigArrays, @Nullable Object missingValue,
            MultiValueMode sortMode, Nested nested, SortOrder sortOrder, DocValueFormat format,
            int bucketSize, BucketedSort.ExtraData extra) {
        return comparatorSource(targetNumericType, missingValue, sortMode, nested)
                .newBucketedSort(bigArrays, sortOrder, format, bucketSize, extra);
    }

    @Override
    public final BucketedSort newBucketedSort(BigArrays bigArrays, @Nullable Object missingValue, MultiValueMode sortMode, Nested nested,
            SortOrder sortOrder, DocValueFormat format, int bucketSize, BucketedSort.ExtraData extra) {
        return newBucketedSort(getNumericType(), bigArrays, missingValue, sortMode, nested, sortOrder, format, bucketSize, extra);
    }

    /**
     * Build a {@link XFieldComparatorSource} matching the parameters.
     */
    private XFieldComparatorSource comparatorSource(
        NumericType targetNumericType,
        @Nullable Object missingValue,
        MultiValueMode sortMode,
        Nested nested
    ) {
        switch (targetNumericType) {
        case HALF_FLOAT:
        case FLOAT:
            return new FloatValuesComparatorSource(this, missingValue, sortMode, nested);
        case DOUBLE:
            return new DoubleValuesComparatorSource(this, missingValue, sortMode, nested);
        case DATE:
            return dateComparatorSource(missingValue, sortMode, nested);
        case DATE_NANOSECONDS:
            return dateNanosComparatorSource(missingValue, sortMode, nested);
        default:
            assert !targetNumericType.isFloatingPoint();
            return new LongValuesComparatorSource(this, missingValue, sortMode, nested);
        }
    }

    protected XFieldComparatorSource dateComparatorSource(@Nullable Object missingValue, MultiValueMode sortMode, Nested nested) {
        return new LongValuesComparatorSource(this, missingValue, sortMode, nested);
    }

    protected XFieldComparatorSource dateNanosComparatorSource(@Nullable Object missingValue, MultiValueMode sortMode, Nested nested) {
        return new LongValuesComparatorSource(this, missingValue, sortMode, nested, dvs -> convertNumeric(dvs, DateUtils::toNanoSeconds));
    }

    /**
     * Convert the values in <code>dvs</code> using the provided <code>converter</code>.
     */
    protected static SortedNumericDocValues convertNumeric(SortedNumericDocValues values, LongUnaryOperator converter) {
        return new AbstractSortedNumericDocValues() {

            @Override
            public boolean advanceExact(int target) throws IOException {
                return values.advanceExact(target);
            }

            @Override
            public long nextValue() throws IOException {
                return converter.applyAsLong(values.nextValue());
            }

            @Override
            public int docValueCount() {
                return values.docValueCount();
            }

            @Override
            public int nextDoc() throws IOException {
                return values.nextDoc();
            }
        };
    }
}

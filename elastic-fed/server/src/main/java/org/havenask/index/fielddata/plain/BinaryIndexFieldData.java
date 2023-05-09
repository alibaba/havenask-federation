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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.SortField;
import org.havenask.common.Nullable;
import org.havenask.common.util.BigArrays;
import org.havenask.index.fielddata.IndexFieldData;
import org.havenask.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.havenask.index.fielddata.IndexFieldDataCache;
import org.havenask.index.fielddata.fieldcomparator.BytesRefFieldComparatorSource;
import org.havenask.indices.breaker.CircuitBreakerService;
import org.havenask.search.DocValueFormat;
import org.havenask.search.MultiValueMode;
import org.havenask.search.aggregations.support.ValuesSourceType;
import org.havenask.search.sort.BucketedSort;
import org.havenask.search.sort.SortOrder;

public class BinaryIndexFieldData implements IndexFieldData<BinaryDVLeafFieldData> {

    public static class Builder implements IndexFieldData.Builder {
        private final String name;
        private final ValuesSourceType valuesSourceType;

        public Builder(String name, ValuesSourceType valuesSourceType) {
            this.name = name;
            this.valuesSourceType = valuesSourceType;
        }

        @Override
        public BinaryIndexFieldData build(IndexFieldDataCache cache, CircuitBreakerService breakerService) {
            return new BinaryIndexFieldData(name, valuesSourceType);
        }
    }
    protected final String fieldName;
    protected final ValuesSourceType valuesSourceType;

    public BinaryIndexFieldData(String fieldName, ValuesSourceType valuesSourceType) {
        this.fieldName = fieldName;
        this.valuesSourceType = valuesSourceType;
    }

    @Override
    public final String getFieldName() {
        return fieldName;
    }

    @Override
    public ValuesSourceType getValuesSourceType() {
        return valuesSourceType;
    }


    @Override
    public BinaryDVLeafFieldData load(LeafReaderContext context) {
        return new BinaryDVLeafFieldData(context.reader(), fieldName);
    }

    @Override
    public BinaryDVLeafFieldData loadDirect(LeafReaderContext context) throws Exception {
        return load(context);
    }

    @Override
    public SortField sortField(@Nullable Object missingValue, MultiValueMode sortMode, XFieldComparatorSource.Nested nested,
            boolean reverse) {
        XFieldComparatorSource source = new BytesRefFieldComparatorSource(this, missingValue, sortMode, nested);
        return new SortField(getFieldName(), source, reverse);
    }

    @Override
    public BucketedSort newBucketedSort(BigArrays bigArrays, Object missingValue, MultiValueMode sortMode, Nested nested,
            SortOrder sortOrder, DocValueFormat format, int bucketSize, BucketedSort.ExtraData extra) {
        throw new IllegalArgumentException("only supported on numeric fields");
    }
}

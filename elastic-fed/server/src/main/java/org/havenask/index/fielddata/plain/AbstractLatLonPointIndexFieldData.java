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

import org.apache.lucene.document.LatLonDocValuesField;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.SortField;
import org.havenask.common.Nullable;
import org.havenask.common.util.BigArrays;
import org.havenask.index.fielddata.IndexFieldData;
import org.havenask.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.havenask.index.fielddata.IndexFieldDataCache;
import org.havenask.index.fielddata.IndexGeoPointFieldData;
import org.havenask.index.fielddata.LeafGeoPointFieldData;
import org.havenask.indices.breaker.CircuitBreakerService;
import org.havenask.search.DocValueFormat;
import org.havenask.search.MultiValueMode;
import org.havenask.search.aggregations.support.ValuesSourceType;
import org.havenask.search.sort.BucketedSort;
import org.havenask.search.sort.SortOrder;

public abstract class AbstractLatLonPointIndexFieldData implements IndexGeoPointFieldData {

    protected final String fieldName;
    protected final ValuesSourceType valuesSourceType;

    AbstractLatLonPointIndexFieldData(String fieldName, ValuesSourceType valuesSourceType) {
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
    public SortField sortField(@Nullable Object missingValue, MultiValueMode sortMode, XFieldComparatorSource.Nested nested,
            boolean reverse) {
        throw new IllegalArgumentException("can't sort on geo_point field without using specific sorting feature, like geo_distance");
    }

    @Override
    public BucketedSort newBucketedSort(BigArrays bigArrays, Object missingValue, MultiValueMode sortMode, Nested nested,
            SortOrder sortOrder, DocValueFormat format, int bucketSize, BucketedSort.ExtraData extra) {
        throw new IllegalArgumentException("can't sort on geo_point field without using specific sorting feature, like geo_distance");
    }

    public static class LatLonPointIndexFieldData extends AbstractLatLonPointIndexFieldData {
        public LatLonPointIndexFieldData(String fieldName, ValuesSourceType valuesSourceType) {
            super(fieldName, valuesSourceType);
        }

        @Override
        public LeafGeoPointFieldData load(LeafReaderContext context) {
            LeafReader reader = context.reader();
            FieldInfo info = reader.getFieldInfos().fieldInfo(fieldName);
            if (info != null) {
                checkCompatible(info);
            }
            return new LatLonPointDVLeafFieldData(reader, fieldName);
        }

        @Override
        public LeafGeoPointFieldData loadDirect(LeafReaderContext context) throws Exception {
            return load(context);
        }

        /** helper: checks a fieldinfo and throws exception if its definitely not a LatLonDocValuesField */
        static void checkCompatible(FieldInfo fieldInfo) {
            // dv properties could be "unset", if you e.g. used only StoredField with this same name in the segment.
            if (fieldInfo.getDocValuesType() != DocValuesType.NONE
                && fieldInfo.getDocValuesType() != LatLonDocValuesField.TYPE.docValuesType()) {
                throw new IllegalArgumentException("field=\"" + fieldInfo.name + "\" was indexed with docValuesType="
                    + fieldInfo.getDocValuesType() + " but this type has docValuesType="
                    + LatLonDocValuesField.TYPE.docValuesType() + ", is the field really a LatLonDocValuesField?");
            }
        }
    }

    public static class Builder implements IndexFieldData.Builder {
        private final String name;
        private final ValuesSourceType valuesSourceType;

        public Builder(String name, ValuesSourceType valuesSourceType) {
            this.name = name;
            this.valuesSourceType =  valuesSourceType;
        }

        @Override
        public IndexFieldData<?> build(IndexFieldDataCache cache, CircuitBreakerService breakerService) {
            // ignore breaker
            return new LatLonPointIndexFieldData(name, valuesSourceType);
        }
    }
}

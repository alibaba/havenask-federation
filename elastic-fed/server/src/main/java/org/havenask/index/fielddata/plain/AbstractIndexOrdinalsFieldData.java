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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.OrdinalMap;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.havenask.HavenaskException;
import org.havenask.index.fielddata.IndexFieldDataCache;
import org.havenask.index.fielddata.IndexOrdinalsFieldData;
import org.havenask.index.fielddata.LeafOrdinalsFieldData;
import org.havenask.index.fielddata.RamAccountingTermsEnum;
import org.havenask.index.fielddata.ScriptDocValues;
import org.havenask.index.fielddata.ordinals.GlobalOrdinalsBuilder;
import org.havenask.index.fielddata.ordinals.GlobalOrdinalsIndexFieldData;
import org.havenask.indices.breaker.CircuitBreakerService;
import org.havenask.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.util.function.Function;

public abstract class AbstractIndexOrdinalsFieldData implements IndexOrdinalsFieldData {
    private static final Logger logger = LogManager.getLogger(AbstractBinaryDVLeafFieldData.class);

    private final String fieldName;
    private final ValuesSourceType valuesSourceType;
    private final IndexFieldDataCache cache;
    protected final CircuitBreakerService breakerService;
    protected final Function<SortedSetDocValues, ScriptDocValues<?>> scriptFunction;

    protected AbstractIndexOrdinalsFieldData(
        String fieldName,
        ValuesSourceType valuesSourceType,
        IndexFieldDataCache cache,
        CircuitBreakerService breakerService,
        Function<SortedSetDocValues, ScriptDocValues<?>> scriptFunction
    ) {
        this.fieldName = fieldName;
        this.valuesSourceType = valuesSourceType;
        this.cache = cache;
        this.breakerService = breakerService;
        this.scriptFunction = scriptFunction;
    }

    @Override
    public String getFieldName() {
        return this.fieldName;
    }

    @Override
    public ValuesSourceType getValuesSourceType() {
        return valuesSourceType;
    }

    @Override
    public OrdinalMap getOrdinalMap() {
        return null;
    }

    @Override
    public LeafOrdinalsFieldData load(LeafReaderContext context) {
        if (context.reader().getFieldInfos().fieldInfo(fieldName) == null) {
            // Some leaf readers may be wrapped and report different set of fields and use the same cache key.
            // If a field can't be found then it doesn't mean it isn't there,
            // so if a field doesn't exist then we don't cache it and just return an empty field data instance.
            // The next time the field is found, we do cache.
            return AbstractLeafOrdinalsFieldData.empty();
        }

        try {
            return cache.load(context, this);
        } catch (Exception e) {
            if (e instanceof HavenaskException) {
                throw (HavenaskException) e;
            } else {
                throw new HavenaskException(e);
            }
        }
    }

    @Override
    public IndexOrdinalsFieldData loadGlobal(DirectoryReader indexReader) {
        IndexOrdinalsFieldData fieldData = loadGlobalInternal(indexReader);
        if (fieldData instanceof GlobalOrdinalsIndexFieldData) {
            // we create a new instance of the cached value for each consumer in order
            // to avoid creating new TermsEnums for each segment in the cached instance
            return ((GlobalOrdinalsIndexFieldData) fieldData).newConsumer(indexReader);
        } else {
            return fieldData;
        }
    }

    private IndexOrdinalsFieldData loadGlobalInternal(DirectoryReader indexReader) {
        if (indexReader.leaves().size() <= 1) {
            // ordinals are already global
            return this;
        }
        boolean fieldFound = false;
        for (LeafReaderContext context : indexReader.leaves()) {
            if (context.reader().getFieldInfos().fieldInfo(getFieldName()) != null) {
                fieldFound = true;
                break;
            }
        }
        if (fieldFound == false) {
            // Some directory readers may be wrapped and report different set of fields and use the same cache key.
            // If a field can't be found then it doesn't mean it isn't there,
            // so if a field doesn't exist then we don't cache it and just return an empty field data instance.
            // The next time the field is found, we do cache.
            try {
                return GlobalOrdinalsBuilder.buildEmpty(indexReader, this);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        try {
            return cache.load(indexReader, this);
        } catch (Exception e) {
            if (e instanceof HavenaskException) {
                throw (HavenaskException) e;
            } else {
                throw new HavenaskException(e);
            }
        }
    }

    @Override
    public IndexOrdinalsFieldData loadGlobalDirect(DirectoryReader indexReader) throws Exception {
        return GlobalOrdinalsBuilder.build(
            indexReader,
            this,
            breakerService,
            logger,
            scriptFunction
        );
    }

    @Override
    public boolean supportsGlobalOrdinalsMapping() {
        return false;
    }

    /**
     * A {@code PerValueEstimator} is a sub-class that can be used to estimate
     * the memory overhead for loading the data. Each field data
     * implementation should implement its own {@code PerValueEstimator} if it
     * intends to take advantage of the CircuitBreaker.
     * <p>
     * Note that the .beforeLoad(...) and .afterLoad(...) methods must be
     * manually called.
     */
    public interface PerValueEstimator {

        /**
         * @return the number of bytes for the given term
         */
        long bytesPerValue(BytesRef term);

        /**
         * Execute any pre-loading estimations for the terms. May also
         * optionally wrap a {@link TermsEnum} in a
         * {@link RamAccountingTermsEnum}
         * which will estimate the memory on a per-term basis.
         *
         * @param terms terms to be estimated
         * @return A TermsEnum for the given terms
         */
        TermsEnum beforeLoad(Terms terms) throws IOException;

        /**
         * Possibly adjust a circuit breaker after field data has been loaded,
         * now that the actual amount of memory used by the field data is known
         *
         * @param termsEnum  terms that were loaded
         * @param actualUsed actual field data memory usage
         */
        void afterLoad(TermsEnum termsEnum, long actualUsed);
    }
}

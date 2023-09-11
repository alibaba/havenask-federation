/*
 * Copyright (c) 2021, Alibaba Group;
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.havenask.engine.index.query;

import org.apache.lucene.search.Query;

import java.util.Arrays;
import java.util.Objects;

public abstract class ProximaQuery extends Query {
    private final String field;
    private final float[] queryVector;
    private final SearchFilter searchFilter;
    private final int topN;

    public static final int DEFAULT_TOPN = 10;

    public ProximaQuery(String field, float[] queryVector, SearchFilter searchFilter, int topN) {
        this.field = field;
        this.queryVector = queryVector;
        this.searchFilter = searchFilter;
        this.topN = topN;
    }

    public ProximaQuery(String field, float[] queryVector, SearchFilter searchFilter) {
        this.field = field;
        this.queryVector = queryVector;
        this.searchFilter = searchFilter;
        this.topN = DEFAULT_TOPN;
    }

    public String getField() {
        return this.field;
    }

    public float[] getQueryVector() {
        return this.queryVector;
    }

    public SearchFilter getSearchFilter() {
        return this.searchFilter;
    }

    public int getTopN() {
        return this.topN;
    }

    @Override
    public String toString(String field) {
        return field;
    }

    @Override
    public int hashCode() {
        return field.hashCode() ^ Arrays.hashCode(queryVector) ^ Objects.hashCode(searchFilter) ^ topN;
    }

    @Override
    public boolean equals(Object other) {
        return sameClassAs(other) && equalsTo(getClass().cast(other));
    }

    private boolean equalsTo(ProximaQuery other) {
        return this.field.equals(other.getField())
            && Objects.deepEquals(queryVector, other.queryVector)
            && Objects.equals(this.searchFilter, other.getSearchFilter())
            && this.topN == other.topN;
    }
}

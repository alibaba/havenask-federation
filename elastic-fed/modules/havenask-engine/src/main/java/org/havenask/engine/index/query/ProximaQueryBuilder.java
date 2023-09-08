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

import org.havenask.common.ParseField;
import org.havenask.common.Strings;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.io.stream.StreamOutput;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.common.xcontent.XContentParser;
import org.havenask.index.query.AbstractQueryBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

public abstract class ProximaQueryBuilder<QB extends ProximaQueryBuilder<QB>> extends AbstractQueryBuilder<QB> {
    public static final ParseField VECTOR_FIELD = new ParseField("vector");
    public static final ParseField SIZE_FIELD = new ParseField("size");
    public static final ParseField SEARCH_FILTER_FIELD = new ParseField("filter");

    protected final String fieldName;
    protected final float[] vector;
    protected final int size;
    protected SearchFilter searchFilter; // TODO: final

    public ProximaQueryBuilder(String fieldName, float[] vector, int size, SearchFilter searchFilter) {
        if (Strings.isNullOrEmpty(fieldName)) {
            throw new IllegalArgumentException("[" + getName() + "] requires fieldName");
        }
        if (vector == null) {
            throw new IllegalArgumentException("[" + getName() + "] requires query vector");
        }
        if (vector.length == 0) {
            throw new IllegalArgumentException("[" + getName() + "] query vector is empty");
        }
        if (size == 0) {
            throw new IllegalArgumentException("[" + getName() + "] requires size");
        }
        if (size < 0) {
            throw new IllegalArgumentException("[" + getName() + "] query size must be positive");
        }
        this.fieldName = fieldName;
        this.vector = vector;
        this.size = size;
        this.searchFilter = searchFilter;
    }

    public ProximaQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.fieldName = in.readString();
        this.vector = in.readFloatArray();
        this.size = in.readInt();
        // TODO searchFilter;
    }

    public abstract String getName();

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(fieldName);
        out.writeFloatArray(vector);
        out.writeInt(size);
        // TODO searchFilter;
        innerDoWriteTo(out);
    }

    protected abstract void innerDoWriteTo(StreamOutput out) throws IOException;

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(getName());
        builder.startObject(fieldName);
        builder.field(VECTOR_FIELD.getPreferredName(), vector);
        builder.field(SIZE_FIELD.getPreferredName(), size);
        // TODO searchFilter;
        innerDoXContent(builder, params);
        printBoostAndQueryName(builder);
        builder.endObject();
        builder.endObject();
    }

    protected abstract void innerDoXContent(XContentBuilder builder, Params params) throws IOException;

    @Override
    protected boolean doEquals(QB other) {
        ProximaQueryBuilder<QB> proximaQueryBuilder = other;

        return Objects.equals(fieldName, proximaQueryBuilder.fieldName)
            && Arrays.equals(vector, proximaQueryBuilder.vector)
            && Objects.equals(size, proximaQueryBuilder.size)
            && Objects.equals(searchFilter, proximaQueryBuilder.searchFilter)
            && innerDoEquals(other);
    }

    protected abstract boolean innerDoEquals(QB other);

    @Override
    protected int doHashCode() {
        return Objects.hash(fieldName, Arrays.hashCode(vector), size, searchFilter, getName(), innerDoHashCode());
    }

    protected abstract int innerDoHashCode();

    @Override
    public String getWriteableName() {
        return getName();
    }

    /**
     * for test
     */
    protected int getSize() {
        return size;
    }

    protected String getFieldName() {
        return fieldName;
    }

    public float[] getVector() {
        return vector;
    }

    public SearchFilter getSearchFilter() {
        return searchFilter;
    }

    protected static Object parseVectorValue(XContentParser parser) throws IOException {
        return parser.floatValue(false);
    }

    protected void printBoostAndQueryName(XContentBuilder builder) throws IOException {
        builder.field(BOOST_FIELD.getPreferredName(), boost);
        if (queryName != null) {
            builder.field(NAME_FIELD.getPreferredName(), queryName);
        }
    }

}

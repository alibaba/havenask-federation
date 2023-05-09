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

package org.havenask.search;

import org.apache.lucene.util.BytesRef;
import org.havenask.LegacyESVersion;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.io.stream.StreamOutput;
import org.havenask.common.io.stream.Writeable;
import org.havenask.common.lucene.Lucene;
import org.havenask.common.xcontent.ToXContentFragment;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.common.xcontent.XContentParser;
import org.havenask.common.xcontent.XContentParserUtils;
import org.havenask.search.SearchHit.Fields;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

public class SearchSortValues implements ToXContentFragment, Writeable {

    private static final Object[] EMPTY_ARRAY = new Object[0];
    static final SearchSortValues EMPTY = new SearchSortValues(EMPTY_ARRAY);

    private final Object[] formattedSortValues;
    private final Object[] rawSortValues;

    SearchSortValues(Object[] sortValues) {
        this.formattedSortValues = Objects.requireNonNull(sortValues, "sort values must not be empty");
        this.rawSortValues = EMPTY_ARRAY;
    }

    public SearchSortValues(Object[] rawSortValues, DocValueFormat[] sortValueFormats) {
        Objects.requireNonNull(rawSortValues);
        Objects.requireNonNull(sortValueFormats);
        if (rawSortValues.length != sortValueFormats.length) {
            throw new IllegalArgumentException("formattedSortValues and sortValueFormats must hold the same number of items");
        }
        this.rawSortValues = rawSortValues;
        this.formattedSortValues = Arrays.copyOf(rawSortValues, rawSortValues.length);
        for (int i = 0; i < rawSortValues.length; ++i) {
            Object sortValue = rawSortValues[i];
            if (sortValue instanceof BytesRef) {
                this.formattedSortValues[i] = sortValueFormats[i].format((BytesRef) sortValue);
            } else if ((sortValue instanceof Long) && (sortValueFormats[i] == DocValueFormat.UNSIGNED_LONG_SHIFTED)) {
                this.formattedSortValues[i] = sortValueFormats[i].format((Long) sortValue);
            } else {
                this.formattedSortValues[i] = sortValue;
            }
        }
    }

    SearchSortValues(StreamInput in) throws IOException {
        this.formattedSortValues = in.readArray(Lucene::readSortValue, Object[]::new);
        if (in.getVersion().onOrAfter(LegacyESVersion.V_6_6_0)) {
            this.rawSortValues = in.readArray(Lucene::readSortValue, Object[]::new);
        } else {
            this.rawSortValues = EMPTY_ARRAY;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeArray(Lucene::writeSortValue, this.formattedSortValues);
        if (out.getVersion().onOrAfter(LegacyESVersion.V_6_6_0)) {
            out.writeArray(Lucene::writeSortValue, this.rawSortValues);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (formattedSortValues.length > 0) {
            builder.startArray(Fields.SORT);
            for (Object sortValue : formattedSortValues) {
                builder.value(sortValue);
            }
            builder.endArray();
        }
        return builder;
    }

    public static SearchSortValues fromXContent(XContentParser parser) throws IOException {
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.currentToken(), parser);
        return new SearchSortValues(parser.list().toArray());
    }

    /**
     * Returns the formatted version of the values that sorting was performed against
     */
    public Object[] getFormattedSortValues() {
        return formattedSortValues;
    }

    /**
     * Returns the raw version of the values that sorting was performed against
     */
    public Object[] getRawSortValues() {
        return rawSortValues;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SearchSortValues that = (SearchSortValues) o;
        return Arrays.equals(formattedSortValues, that.formattedSortValues) &&
            Arrays.equals(rawSortValues, that.rawSortValues);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(formattedSortValues);
        result = 31 * result + Arrays.hashCode(rawSortValues);
        return result;
    }
}

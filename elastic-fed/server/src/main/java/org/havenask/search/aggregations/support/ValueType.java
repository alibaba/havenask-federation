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

package org.havenask.search.aggregations.support;

import org.havenask.common.ParseField;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.io.stream.StreamOutput;
import org.havenask.common.io.stream.Writeable;
import org.havenask.index.mapper.DateFieldMapper;
import org.havenask.search.DocValueFormat;

import java.io.IOException;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;

public enum ValueType implements Writeable {

    STRING((byte) 1, "string", "string", CoreValuesSourceType.BYTES,
        DocValueFormat.RAW),

    LONG((byte) 2, "byte|short|integer|long", "long", CoreValuesSourceType.NUMERIC, DocValueFormat.RAW),
    DOUBLE((byte) 3, "float|double", "double", CoreValuesSourceType.NUMERIC, DocValueFormat.RAW),
    NUMBER((byte) 4, "number", "number", CoreValuesSourceType.NUMERIC, DocValueFormat.RAW),
    DATE((byte) 5, "date", "date", CoreValuesSourceType.DATE,
        new DocValueFormat.DateTime(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER, ZoneOffset.UTC,
                DateFieldMapper.Resolution.MILLISECONDS)),
    IP((byte) 6, "ip", "ip", CoreValuesSourceType.IP, DocValueFormat.IP),
    // TODO: what is the difference between "number" and "numeric"?
    NUMERIC((byte) 7, "numeric", "numeric", CoreValuesSourceType.NUMERIC, DocValueFormat.RAW),
    GEOPOINT((byte) 8, "geo_point", "geo_point", CoreValuesSourceType.GEOPOINT, DocValueFormat.GEOHASH),
    BOOLEAN((byte) 9, "boolean", "boolean", CoreValuesSourceType.BOOLEAN, DocValueFormat.BOOLEAN),
    RANGE((byte) 10, "range", "range", CoreValuesSourceType.RANGE, DocValueFormat.RAW);

    final String description;
    final ValuesSourceType valuesSourceType;
    final DocValueFormat defaultFormat;
    private final byte id;
    private String preferredName;

    public static final ParseField VALUE_TYPE = new ParseField("value_type", "valueType");

    ValueType(byte id, String description, String preferredName, ValuesSourceType valuesSourceType,
              DocValueFormat defaultFormat) {
        this.id = id;
        this.description = description;
        this.preferredName = preferredName;
        this.valuesSourceType = valuesSourceType;
        this.defaultFormat = defaultFormat;
    }

    public String getPreferredName() {
        return preferredName;
    }

    public ValuesSourceType getValuesSourceType() {
        return valuesSourceType;
    }

    private static List<ValueType> numericValueTypes = Arrays.asList(ValueType.DOUBLE, ValueType.DATE, ValueType.LONG, ValueType.NUMBER,
        ValueType.NUMERIC, ValueType.BOOLEAN);
    private static List<ValueType> stringValueTypes = Arrays.asList(ValueType.STRING, ValueType.IP);

    /**
     * This is a bit of a hack to mirror the old {@link ValueType} behavior, which would allow a rough compatibility between types.  This
     * behavior is being phased out in the aggregations framework, in favor of explicitly listing supported types, but we haven't gotten
     * to fixing composite yet.
     *
     * @param valueType The value type the user suggested
     * @return True iff the two value types are interchangeable
     */
    public boolean isA(ValueType valueType) {
        if (numericValueTypes.contains(this)) {
            return numericValueTypes.contains(valueType);
        }
        if (stringValueTypes.contains(this)) {
            return stringValueTypes.contains(valueType);
        }
        return this.equals(valueType);
    }

    public boolean isNotA(ValueType valueType) {
        return !isA(valueType);
    }

    public DocValueFormat defaultFormat() {
        return defaultFormat;
    }

    public static ValueType lenientParse(String type) {
        switch (type) {
            case "string":  return STRING;
            case "double":
            case "float":   return DOUBLE;
            case "number":
            case "numeric":
            case "long":
            case "integer":
            case "short":
            case "byte":    return LONG;
            case "date":    return DATE;
            case "ip":      return IP;
            case "boolean": return BOOLEAN;
            default:
                // TODO: do not be lenient here
                return null;
        }
    }

    @Override
    public String toString() {
        return description;
    }

    public static ValueType readFromStream(StreamInput in) throws IOException {
        byte id = in.readByte();
        for (ValueType valueType : values()) {
            if (id == valueType.id) {
                return valueType;
            }
        }
        throw new IOException("No ValueType found for id [" + id + "]");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeByte(id);
    }
}

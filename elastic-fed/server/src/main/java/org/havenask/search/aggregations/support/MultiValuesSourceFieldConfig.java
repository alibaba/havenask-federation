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

import org.havenask.LegacyESVersion;
import org.havenask.common.ParseField;
import org.havenask.common.Strings;
import org.havenask.common.TriFunction;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.io.stream.StreamOutput;
import org.havenask.common.io.stream.Writeable;
import org.havenask.common.time.DateUtils;
import org.havenask.common.xcontent.ObjectParser;
import org.havenask.common.xcontent.ToXContentObject;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.common.xcontent.XContentParser;
import org.havenask.index.query.AbstractQueryBuilder;
import org.havenask.index.query.QueryBuilder;
import org.havenask.script.Script;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Objects;

public class MultiValuesSourceFieldConfig implements Writeable, ToXContentObject {
    private final String fieldName;
    private final Object missing;
    private final Script script;
    private final ZoneId timeZone;
    private final QueryBuilder filter;

    private static final String NAME = "field_config";
    public static final ParseField FILTER = new ParseField("filter");

    public static final TriFunction<Boolean, Boolean, Boolean, ObjectParser<Builder, Void>> PARSER
        = (scriptable, timezoneAware, filtered) -> {

        ObjectParser<MultiValuesSourceFieldConfig.Builder, Void> parser
            = new ObjectParser<>(MultiValuesSourceFieldConfig.NAME, MultiValuesSourceFieldConfig.Builder::new);

        parser.declareString(MultiValuesSourceFieldConfig.Builder::setFieldName, ParseField.CommonFields.FIELD);
        parser.declareField(MultiValuesSourceFieldConfig.Builder::setMissing, XContentParser::objectText,
            ParseField.CommonFields.MISSING, ObjectParser.ValueType.VALUE);

        if (scriptable) {
            parser.declareField(MultiValuesSourceFieldConfig.Builder::setScript,
                (p, context) -> Script.parse(p),
                Script.SCRIPT_PARSE_FIELD, ObjectParser.ValueType.OBJECT_OR_STRING);
        }

        if (timezoneAware) {
            parser.declareField(MultiValuesSourceFieldConfig.Builder::setTimeZone, p -> {
                if (p.currentToken() == XContentParser.Token.VALUE_STRING) {
                    return ZoneId.of(p.text());
                } else {
                    return ZoneOffset.ofHours(p.intValue());
                }
            }, ParseField.CommonFields.TIME_ZONE, ObjectParser.ValueType.LONG);
        }

        if (filtered) {
            parser.declareField(MultiValuesSourceFieldConfig.Builder::setFilter,
                (p, context) -> AbstractQueryBuilder.parseInnerQueryBuilder(p),
                FILTER, ObjectParser.ValueType.OBJECT);
        }
        return parser;
    };

    protected MultiValuesSourceFieldConfig(String fieldName, Object missing, Script script, ZoneId timeZone, QueryBuilder filter) {
        this.fieldName = fieldName;
        this.missing = missing;
        this.script = script;
        this.timeZone = timeZone;
        this.filter = filter;
    }

    public MultiValuesSourceFieldConfig(StreamInput in) throws IOException {
        if (in.getVersion().onOrAfter(LegacyESVersion.V_7_6_0)) {
            this.fieldName = in.readOptionalString();
        } else {
            this.fieldName = in.readString();
        }
        this.missing = in.readGenericValue();
        this.script = in.readOptionalWriteable(Script::new);
        if (in.getVersion().before(LegacyESVersion.V_7_0_0)) {
            this.timeZone = DateUtils.dateTimeZoneToZoneId(in.readOptionalTimeZone());
        } else {
            this.timeZone = in.readOptionalZoneId();
        }
        if (in.getVersion().onOrAfter(LegacyESVersion.V_7_8_0)) {
            this.filter = in.readOptionalNamedWriteable(QueryBuilder.class);
        } else {
            this.filter = null;
        }
    }

    public Object getMissing() {
        return missing;
    }

    public Script getScript() {
        return script;
    }

    public ZoneId getTimeZone() {
        return timeZone;
    }

    public String getFieldName() {
        return fieldName;
    }

    public QueryBuilder getFilter() {
        return filter;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getVersion().onOrAfter(LegacyESVersion.V_7_6_0)) {
            out.writeOptionalString(fieldName);
        } else {
            out.writeString(fieldName);
        }
        out.writeGenericValue(missing);
        out.writeOptionalWriteable(script);
        if (out.getVersion().before(LegacyESVersion.V_7_0_0)) {
            out.writeOptionalTimeZone(DateUtils.zoneIdToDateTimeZone(timeZone));
        } else {
            out.writeOptionalZoneId(timeZone);
        }
        if (out.getVersion().onOrAfter(LegacyESVersion.V_7_8_0)) {
            out.writeOptionalNamedWriteable(filter);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (missing != null) {
            builder.field(ParseField.CommonFields.MISSING.getPreferredName(), missing);
        }
        if (script != null) {
            builder.field(Script.SCRIPT_PARSE_FIELD.getPreferredName(), script);
        }
        if (fieldName != null) {
            builder.field(ParseField.CommonFields.FIELD.getPreferredName(), fieldName);
        }
        if (timeZone != null) {
            builder.field(ParseField.CommonFields.TIME_ZONE.getPreferredName(), timeZone.getId());
        }
        if (filter != null) {
            builder.field(FILTER.getPreferredName());
            filter.toXContent(builder, params);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MultiValuesSourceFieldConfig that = (MultiValuesSourceFieldConfig) o;
        return Objects.equals(fieldName, that.fieldName)
            && Objects.equals(missing, that.missing)
            && Objects.equals(script, that.script)
            && Objects.equals(timeZone, that.timeZone)
            && Objects.equals(filter, that.filter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldName, missing, script, timeZone, filter);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    public static class Builder {
        private String fieldName;
        private Object missing = null;
        private Script script = null;
        private ZoneId timeZone = null;
        private QueryBuilder filter = null;

        public String getFieldName() {
            return fieldName;
        }

        public Builder setFieldName(String fieldName) {
            this.fieldName = fieldName;
            return this;
        }

        public Object getMissing() {
            return missing;
        }

        public Builder setMissing(Object missing) {
            this.missing = missing;
            return this;
        }

        public Script getScript() {
            return script;
        }

        public Builder setScript(Script script) {
            this.script = script;
            return this;
        }

        public ZoneId getTimeZone() {
            return timeZone;
        }

        public Builder setTimeZone(ZoneId timeZone) {
            this.timeZone = timeZone;
            return this;
        }

        public Builder setFilter(QueryBuilder filter) {
            this.filter = filter;
            return this;
        }

        public MultiValuesSourceFieldConfig build() {
            if (Strings.isNullOrEmpty(fieldName) && script == null) {
                throw new IllegalArgumentException("[" +  ParseField.CommonFields.FIELD.getPreferredName()
                    + "] and [" + Script.SCRIPT_PARSE_FIELD.getPreferredName() + "] cannot both be null.  " +
                    "Please specify one or the other.");
            }

            if (Strings.isNullOrEmpty(fieldName) == false && script != null) {
                throw new IllegalArgumentException("[" +  ParseField.CommonFields.FIELD.getPreferredName()
                    + "] and [" + Script.SCRIPT_PARSE_FIELD.getPreferredName() + "] cannot both be configured.  " +
                    "Please specify one or the other.");
            }

            return new MultiValuesSourceFieldConfig(fieldName, missing, script, timeZone, filter);
        }
    }
}

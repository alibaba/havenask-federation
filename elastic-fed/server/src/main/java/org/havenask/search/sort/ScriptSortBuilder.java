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

package org.havenask.search.sort;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.havenask.LegacyESVersion;
import org.havenask.common.ParseField;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.io.stream.StreamOutput;
import org.havenask.common.io.stream.Writeable;
import org.havenask.common.logging.DeprecationLogger;
import org.havenask.common.util.BigArrays;
import org.havenask.common.xcontent.ConstructingObjectParser;
import org.havenask.common.xcontent.ObjectParser.ValueType;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.common.xcontent.XContentParser;
import org.havenask.index.fielddata.AbstractBinaryDocValues;
import org.havenask.index.fielddata.FieldData;
import org.havenask.index.fielddata.IndexFieldData;
import org.havenask.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.havenask.index.fielddata.NumericDoubleValues;
import org.havenask.index.fielddata.SortedBinaryDocValues;
import org.havenask.index.fielddata.SortedNumericDoubleValues;
import org.havenask.index.fielddata.fieldcomparator.BytesRefFieldComparatorSource;
import org.havenask.index.fielddata.fieldcomparator.DoubleValuesComparatorSource;
import org.havenask.index.query.QueryBuilder;
import org.havenask.index.query.QueryRewriteContext;
import org.havenask.index.query.QueryShardContext;
import org.havenask.index.query.QueryShardException;
import org.havenask.script.NumberSortScript;
import org.havenask.script.Script;
import org.havenask.script.StringSortScript;
import org.havenask.search.DocValueFormat;
import org.havenask.search.MultiValueMode;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

import static org.havenask.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.havenask.search.sort.FieldSortBuilder.validateMaxChildrenExistOnlyInTopLevelNestedSort;
import static org.havenask.search.sort.NestedSortBuilder.NESTED_FIELD;

/**
 * Script sort builder allows to sort based on a custom script expression.
 */
public class ScriptSortBuilder extends SortBuilder<ScriptSortBuilder> {
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(ScriptSortBuilder.class);

    public static final String NAME = "_script";
    public static final ParseField TYPE_FIELD = new ParseField("type");
    public static final ParseField SCRIPT_FIELD = new ParseField("script");
    public static final ParseField SORTMODE_FIELD = new ParseField("mode");

    private final Script script;

    private final ScriptSortType type;

    private SortMode sortMode;

    private QueryBuilder nestedFilter;

    private String nestedPath;

    private NestedSortBuilder nestedSort;

    /**
     * Constructs a script sort builder with the given script.
     *
     * @param script
     *            The script to use.
     * @param type
     *            The type of the script, can be either {@link ScriptSortType#STRING} or
     *            {@link ScriptSortType#NUMBER}
     */
    public ScriptSortBuilder(Script script, ScriptSortType type) {
        Objects.requireNonNull(script, "script cannot be null");
        Objects.requireNonNull(type, "type cannot be null");
        this.script = script;
        this.type = type;
    }

    ScriptSortBuilder(ScriptSortBuilder original) {
        this.script = original.script;
        this.type = original.type;
        this.order = original.order;
        this.sortMode = original.sortMode;
        this.nestedFilter = original.nestedFilter;
        this.nestedPath = original.nestedPath;
        this.nestedSort = original.nestedSort;
    }

    /**
     * Read from a stream.
     */
    public ScriptSortBuilder(StreamInput in) throws IOException {
        script = new Script(in);
        type = ScriptSortType.readFromStream(in);
        order = SortOrder.readFromStream(in);
        sortMode = in.readOptionalWriteable(SortMode::readFromStream);
        nestedPath = in.readOptionalString();
        nestedFilter = in.readOptionalNamedWriteable(QueryBuilder.class);
        if (in.getVersion().onOrAfter(LegacyESVersion.V_6_1_0)) {
            nestedSort = in.readOptionalWriteable(NestedSortBuilder::new);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        script.writeTo(out);
        type.writeTo(out);
        order.writeTo(out);
        out.writeOptionalWriteable(sortMode);
        out.writeOptionalString(nestedPath);
        out.writeOptionalNamedWriteable(nestedFilter);
        if (out.getVersion().onOrAfter(LegacyESVersion.V_6_1_0)) {
            out.writeOptionalWriteable(nestedSort);
        }
    }

    /**
     * Get the script used in this sort.
     */
    public Script script() {
        return this.script;
    }

    /**
     * Get the type used in this sort.
     */
    public ScriptSortType type() {
        return this.type;
    }

    /**
     * Defines which distance to use for sorting in the case a document contains multiple values.<br>
     * For {@link ScriptSortType#STRING}, the set of possible values is restricted to {@link SortMode#MIN} and {@link SortMode#MAX}
     */
    public ScriptSortBuilder sortMode(SortMode sortMode) {
        Objects.requireNonNull(sortMode, "sort mode cannot be null.");
        if (ScriptSortType.STRING.equals(type) && (sortMode == SortMode.SUM || sortMode == SortMode.AVG ||
                sortMode == SortMode.MEDIAN)) {
            throw new IllegalArgumentException("script sort of type [string] doesn't support mode [" + sortMode + "]");
        }
        this.sortMode = sortMode;
        return this;
    }

    /**
     * Get the sort mode.
     */
    public SortMode sortMode() {
        return this.sortMode;
    }

    /**
     * Sets the nested filter that the nested objects should match with in order to be taken into account
     * for sorting.
     *
     * @deprecated set nested sort with {@link #setNestedSort(NestedSortBuilder)} and retrieve with {@link #getNestedSort()}
     */
    @Deprecated
    public ScriptSortBuilder setNestedFilter(QueryBuilder nestedFilter) {
        if (this.nestedSort != null) {
            throw new IllegalArgumentException("Setting both nested_path/nested_filter and nested not allowed");
        }
        this.nestedFilter = nestedFilter;
        return this;
    }

    /**
     * Gets the nested filter.
     *
     * @deprecated set nested sort with {@link #setNestedSort(NestedSortBuilder)} and retrieve with {@link #getNestedSort()}
     */
    @Deprecated
    public QueryBuilder getNestedFilter() {
        return this.nestedFilter;
    }

    /**
     * Sets the nested path if sorting occurs on a field that is inside a nested object. For sorting by script this
     * needs to be specified.
     *
     * @deprecated set nested sort with {@link #setNestedSort(NestedSortBuilder)} and retrieve with {@link #getNestedSort()}
     */
    @Deprecated
    public ScriptSortBuilder setNestedPath(String nestedPath) {
        if (this.nestedSort != null) {
            throw new IllegalArgumentException("Setting both nested_path/nested_filter and nested not allowed");
        }
        this.nestedPath = nestedPath;
        return this;
    }

    /**
     * Gets the nested path.
     *
     * @deprecated set nested sort with {@link #setNestedSort(NestedSortBuilder)} and retrieve with {@link #getNestedSort()}
     */
    @Deprecated
    public String getNestedPath() {
        return this.nestedPath;
    }

    /**
     * Returns the {@link NestedSortBuilder}
     */
    public NestedSortBuilder getNestedSort() {
        return this.nestedSort;
    }

    /**
     * Sets the {@link NestedSortBuilder} to be used for fields that are inside a nested
     * object. The {@link NestedSortBuilder} takes a `path` argument and an optional
     * nested filter that the nested objects should match with in
     * order to be taken into account for sorting.
     */
    public ScriptSortBuilder setNestedSort(final NestedSortBuilder nestedSort) {
        if (this.nestedFilter != null || this.nestedPath != null) {
            throw new IllegalArgumentException("Setting both nested_path/nested_filter and nested not allowed");
        }
        this.nestedSort = nestedSort;
        return this;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params builderParams) throws IOException {
        builder.startObject();
        builder.startObject(NAME);
        builder.field(SCRIPT_FIELD.getPreferredName(), script);
        builder.field(TYPE_FIELD.getPreferredName(), type);
        builder.field(ORDER_FIELD.getPreferredName(), order);
        if (sortMode != null) {
            builder.field(SORTMODE_FIELD.getPreferredName(), sortMode);
        }
        if (nestedPath != null) {
            builder.field(NESTED_PATH_FIELD.getPreferredName(), nestedPath);
        }
        if (nestedFilter != null) {
            builder.field(NESTED_FILTER_FIELD.getPreferredName(), nestedFilter, builderParams);
        }
        if (nestedSort != null) {
            builder.field(NESTED_FIELD.getPreferredName(), nestedSort);
        }
        builder.endObject();
        builder.endObject();
        return builder;
    }

    private static final ConstructingObjectParser<ScriptSortBuilder, Void> PARSER = new ConstructingObjectParser<>(NAME,
            a -> new ScriptSortBuilder((Script) a[0], (ScriptSortType) a[1]));

    static {
        PARSER.declareField(constructorArg(), (parser, context) -> Script.parse(parser),
                Script.SCRIPT_PARSE_FIELD, ValueType.OBJECT_OR_STRING);
        PARSER.declareField(constructorArg(), p -> ScriptSortType.fromString(p.text()), TYPE_FIELD, ValueType.STRING);
        PARSER.declareString((b, v) -> b.order(SortOrder.fromString(v)), ORDER_FIELD);
        PARSER.declareString((b, v) -> b.sortMode(SortMode.fromString(v)), SORTMODE_FIELD);
        PARSER.declareString((fieldSortBuilder, nestedPath) -> {
            deprecationLogger.deprecate("script_nested_path",
                "[nested_path] has been deprecated in favor of the [nested] parameter");
            fieldSortBuilder.setNestedPath(nestedPath);
        }, NESTED_PATH_FIELD);
        PARSER.declareObject(ScriptSortBuilder::setNestedFilter, (p, c) -> {
            deprecationLogger.deprecate("script_nested_filter",
                "[nested_filter] has been deprecated in favour for the [nested] parameter");
            return SortBuilder.parseNestedFilter(p);
        }, NESTED_FILTER_FIELD);
        PARSER.declareObject(ScriptSortBuilder::setNestedSort, (p, c) -> NestedSortBuilder.fromXContent(p), NESTED_FIELD);
    }

    /**
     * Creates a new {@link ScriptSortBuilder} from the query held by the {@link XContentParser} in
     * {@link org.havenask.common.xcontent.XContent} format.
     *
     * @param parser the input parser. The state on the parser contained in this context will be changed as a side effect of this
     *        method call
     * @param elementName in some sort syntax variations the field name precedes the xContent object that specifies further parameters, e.g.
     *        in '{ "foo": { "order" : "asc"} }'. When parsing the inner object, the field name can be passed in via this argument
     */
    public static ScriptSortBuilder fromXContent(XContentParser parser, String elementName) {
        return PARSER.apply(parser, null);
    }


    @Override
    public SortFieldAndFormat build(QueryShardContext context) throws IOException {
        return new SortFieldAndFormat(
                new SortField("_script", fieldComparatorSource(context), order == SortOrder.DESC),
                DocValueFormat.RAW);
    }

    @Override
    public BucketedSort buildBucketedSort(QueryShardContext context, int bucketSize, BucketedSort.ExtraData extra) throws IOException {
        return fieldComparatorSource(context).newBucketedSort(context.bigArrays(), order, DocValueFormat.RAW, bucketSize, extra);
    }

    private IndexFieldData.XFieldComparatorSource fieldComparatorSource(QueryShardContext context) throws IOException {
        MultiValueMode valueMode = null;
        if (sortMode != null) {
            valueMode = MultiValueMode.fromString(sortMode.toString());
        }
        if (valueMode == null) {
            valueMode = order == SortOrder.DESC ? MultiValueMode.MAX : MultiValueMode.MIN;
        }

        final Nested nested;
        if (nestedSort != null) {
            if (context.indexVersionCreated().before(LegacyESVersion.V_6_5_0) && nestedSort.getMaxChildren() != Integer.MAX_VALUE) {
                throw new QueryShardException(context,
                    "max_children is only supported on v6.5.0 or higher");
            }
            // new nested sorts takes priority
            validateMaxChildrenExistOnlyInTopLevelNestedSort(context, nestedSort);
            nested = resolveNested(context, nestedSort);
        } else {
            nested = resolveNested(context, nestedPath, nestedFilter);
        }

        switch (type) {
            case STRING:
                final StringSortScript.Factory factory = context.compile(script, StringSortScript.CONTEXT);
                final StringSortScript.LeafFactory searchScript = factory.newFactory(script.getParams(), context.lookup());
                return new BytesRefFieldComparatorSource(null, null, valueMode, nested) {
                    StringSortScript leafScript;
                    @Override
                    protected SortedBinaryDocValues getValues(LeafReaderContext context) throws IOException {
                        leafScript = searchScript.newInstance(context);
                        final BinaryDocValues values = new AbstractBinaryDocValues() {
                            final BytesRefBuilder spare = new BytesRefBuilder();
                            @Override
                            public boolean advanceExact(int doc) throws IOException {
                                leafScript.setDocument(doc);
                                return true;
                            }
                            @Override
                            public BytesRef binaryValue() {
                                spare.copyChars(leafScript.execute());
                                return spare.get();
                            }
                        };
                        return FieldData.singleton(values);
                    }
                    @Override
                    protected void setScorer(Scorable scorer) {
                        leafScript.setScorer(scorer);
                    }

                    @Override
                    public BucketedSort newBucketedSort(BigArrays bigArrays, SortOrder sortOrder, DocValueFormat format,
                            int bucketSize, BucketedSort.ExtraData extra) {
                        throw new IllegalArgumentException("error building sort for [_script]: "
                                + "script sorting only supported on [numeric] scripts but was [" + type + "]");
                    }
                };
            case NUMBER:
                final NumberSortScript.Factory numberSortFactory = context.compile(script, NumberSortScript.CONTEXT);
                final NumberSortScript.LeafFactory numberSortScript = numberSortFactory.newFactory(script.getParams(), context.lookup());
                return new DoubleValuesComparatorSource(null, Double.MAX_VALUE, valueMode, nested) {
                    NumberSortScript leafScript;
                    @Override
                    protected SortedNumericDoubleValues getValues(LeafReaderContext context) throws IOException {
                        leafScript = numberSortScript.newInstance(context);
                        final NumericDoubleValues values = new NumericDoubleValues() {
                            @Override
                            public boolean advanceExact(int doc) throws IOException {
                                leafScript.setDocument(doc);
                                return true;
                            }
                            @Override
                            public double doubleValue() {
                                return leafScript.execute();
                            }
                        };
                        return FieldData.singleton(values);
                    }
                    @Override
                    protected void setScorer(Scorable scorer) {
                        leafScript.setScorer(scorer);
                    }
                };
            default:
            throw new QueryShardException(context, "custom script sort type [" + type + "] not supported");
        }
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }
        ScriptSortBuilder other = (ScriptSortBuilder) object;
        return Objects.equals(script, other.script) &&
                Objects.equals(type, other.type) &&
                Objects.equals(order, other.order) &&
                Objects.equals(sortMode, other.sortMode) &&
                Objects.equals(nestedFilter, other.nestedFilter) &&
                Objects.equals(nestedPath, other.nestedPath) &&
                Objects.equals(nestedSort, other.nestedSort);
    }

    @Override
    public int hashCode() {
        return Objects.hash(script, type, order, sortMode, nestedFilter, nestedPath, nestedSort);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    public enum ScriptSortType implements Writeable {
        /** script sort for a string value **/
        STRING,
        /** script sort for a numeric value **/
        NUMBER;

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            out.writeEnum(this);
        }

        /**
         * Read from a stream.
         */
        static ScriptSortType readFromStream(final StreamInput in) throws IOException {
            return in.readEnum(ScriptSortType.class);
        }

        public static ScriptSortType fromString(final String str) {
            Objects.requireNonNull(str, "input string is null");
            switch (str.toLowerCase(Locale.ROOT)) {
                case ("string"):
                    return ScriptSortType.STRING;
                case ("number"):
                    return ScriptSortType.NUMBER;
                default:
                    throw new IllegalArgumentException("Unknown ScriptSortType [" + str + "]");
            }
        }

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    @Override
    public ScriptSortBuilder rewrite(QueryRewriteContext ctx) throws IOException {
        if (nestedFilter == null && nestedSort == null) {
            return this;
        }
        if (nestedFilter != null) {
            QueryBuilder rewrite = nestedFilter.rewrite(ctx);
            if (nestedFilter == rewrite) {
                return this;
            }
            return new ScriptSortBuilder(this).setNestedFilter(rewrite);
        } else {
            NestedSortBuilder rewrite = nestedSort.rewrite(ctx);
            if (nestedSort == rewrite) {
                return this;
            }
            return new ScriptSortBuilder(this).setNestedSort(rewrite);
        }
    }
}

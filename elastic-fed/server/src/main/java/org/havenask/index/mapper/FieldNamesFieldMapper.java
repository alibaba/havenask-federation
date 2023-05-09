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

package org.havenask.index.mapper;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.Query;
import org.havenask.LegacyESVersion;
import org.havenask.Version;
import org.havenask.common.Explicit;
import org.havenask.common.logging.DeprecationLogger;
import org.havenask.index.query.QueryShardContext;
import org.havenask.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * A mapper that indexes the field names of a document under <code>_field_names</code>. This mapper is typically useful in order
 * to have fast <code>exists</code> and <code>missing</code> queries/filters.
 */
public class FieldNamesFieldMapper extends MetadataFieldMapper {

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(FieldNamesFieldMapper.class);

    public static final String NAME = "_field_names";

    public static final String CONTENT_TYPE = "_field_names";

    @Override
    public ParametrizedFieldMapper.Builder getMergeBuilder() {
        return new Builder(indexVersionCreated).init(this);
    }

    public static class Defaults {
        public static final String NAME = FieldNamesFieldMapper.NAME;

        public static final Explicit<Boolean> ENABLED = new Explicit<>(true, false);
        public static final FieldType FIELD_TYPE = new FieldType();

        static {
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setStored(false);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.freeze();
        }
    }

    private static FieldNamesFieldMapper toType(FieldMapper in) {
        return (FieldNamesFieldMapper) in;
    }

    public static final String ENABLED_DEPRECATION_MESSAGE =
        "Disabling _field_names is not necessary because it no longer carries a large index overhead. Support for the `enabled` " +
        "setting will be removed in a future major version. Please remove it from your mappings and templates.";


    static class Builder extends MetadataFieldMapper.Builder {

        private final Parameter<Explicit<Boolean>> enabled
            = updateableBoolParam("enabled", m -> toType(m).enabled, Defaults.ENABLED.value());

        private final Version indexVersionCreated;

        Builder(Version indexVersionCreated) {
            super(Defaults.NAME);
            this.indexVersionCreated = indexVersionCreated;
        }

        @Override
        protected List<Parameter<?>> getParameters() {
            return Collections.singletonList(enabled);
        }

        @Override
        public FieldNamesFieldMapper build(BuilderContext context) {
            if (enabled.getValue().explicit()) {
                deprecationLogger.deprecate("field_names_enabled_parameter", ENABLED_DEPRECATION_MESSAGE);
            }
            FieldNamesFieldType fieldNamesFieldType = new FieldNamesFieldType(enabled.getValue().value());
            return new FieldNamesFieldMapper(enabled.getValue(), indexVersionCreated, fieldNamesFieldType);
        }
    }

    public static final TypeParser PARSER = new ConfigurableTypeParser(
        c -> new FieldNamesFieldMapper(Defaults.ENABLED, c.indexVersionCreated(), new FieldNamesFieldType(Defaults.ENABLED.value())),
        c -> new Builder(c.indexVersionCreated())
    );

    public static final class FieldNamesFieldType extends TermBasedFieldType {

        private final boolean enabled;

        public FieldNamesFieldType(boolean enabled) {
            super(Defaults.NAME, true, false, false, TextSearchInfo.SIMPLE_MATCH_ONLY, Collections.emptyMap());
            this.enabled = enabled;
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        public boolean isEnabled() {
            return enabled;
        }

        @Override
        public ValueFetcher valueFetcher(MapperService mapperService, SearchLookup lookup, String format) {
            throw new UnsupportedOperationException("Cannot fetch values for internal field [" + name() + "].");
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            throw new UnsupportedOperationException("Cannot run exists query on _field_names");
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            if (isEnabled() == false) {
                throw new IllegalStateException("Cannot run [exists] queries if the [_field_names] field is disabled");
            }
            deprecationLogger.deprecate("terms_query_on_field_names",
                "terms query on the _field_names field is deprecated and will be removed, use exists query instead");
            return super.termQuery(value, context);
        }
    }

    private final Explicit<Boolean> enabled;
    private final Version indexVersionCreated;

    private FieldNamesFieldMapper(Explicit<Boolean> enabled, Version indexVersionCreated, FieldNamesFieldType mappedFieldType) {
        super(mappedFieldType);
        this.enabled = enabled;
        this.indexVersionCreated = indexVersionCreated;
    }

    @Override
    public FieldNamesFieldType fieldType() {
        return (FieldNamesFieldType) super.fieldType();
    }

    @Override
    public void postParse(ParseContext context) throws IOException {
        if (context.indexSettings().getIndexVersionCreated().before(LegacyESVersion.V_6_1_0)) {
            if (fieldType().isEnabled() == false) {
                return;
            }
            for (ParseContext.Document document : context) {
                final List<String> paths = new ArrayList<>(document.getFields().size());
                String previousPath = ""; // used as a sentinel - field names can't be empty
                for (IndexableField field : document.getFields()) {
                    final String path = field.name();
                    if (path.equals(previousPath)) {
                        // Sometimes mappers create multiple Lucene fields, eg. one for indexing,
                        // one for doc values and one for storing. Deduplicating is not required
                        // for correctness but this simple check helps save utf-8 conversions and
                        // gives Lucene fewer values to deal with.
                        continue;
                    }
                    paths.add(path);
                    previousPath = path;
                }
                for (String path : paths) {
                    for (String fieldName : extractFieldNames(path)) {
                        document.add(new Field(fieldType().name(), fieldName, Defaults.FIELD_TYPE));
                    }
                }
            }
        }
    }

    static Iterable<String> extractFieldNames(final String fullPath) {
        return new Iterable<String>() {
            @Override
            public Iterator<String> iterator() {
                return new Iterator<String>() {
                    int endIndex = nextEndIndex(0);

                    private int nextEndIndex(int index) {
                        while (index < fullPath.length() && fullPath.charAt(index) != '.') {
                            index += 1;
                        }
                        return index;
                    }

                    @Override
                    public boolean hasNext() {
                        return endIndex <= fullPath.length();
                    }

                    @Override
                    public String next() {
                        final String result = fullPath.substring(0, endIndex);
                        endIndex = nextEndIndex(endIndex + 1);
                        return result;
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }

                };
            }
        };
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

}

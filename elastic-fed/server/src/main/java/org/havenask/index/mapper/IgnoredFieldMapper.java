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
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermRangeQuery;
import org.havenask.index.query.QueryShardContext;
import org.havenask.search.lookup.SearchLookup;

import java.util.Collections;

/**
 * A field mapper that records fields that have been ignored because they were malformed.
 */
public final class IgnoredFieldMapper extends MetadataFieldMapper {

    public static final String NAME = "_ignored";

    public static final String CONTENT_TYPE = "_ignored";

    public static class Defaults {
        public static final String NAME = IgnoredFieldMapper.NAME;

        public static final FieldType FIELD_TYPE = new FieldType();

        static {
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setStored(true);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.freeze();
        }
    }

    public static final TypeParser PARSER = new FixedTypeParser(c -> new IgnoredFieldMapper());

    public static final class IgnoredFieldType extends StringFieldType {

        public static final IgnoredFieldType INSTANCE = new IgnoredFieldType();

        private IgnoredFieldType() {
            super(NAME, true, true, false, TextSearchInfo.SIMPLE_MATCH_ONLY, Collections.emptyMap());
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            // This query is not performance sensitive, it only helps assess
            // quality of the data, so we may use a slow query. It shouldn't
            // be too slow in practice since the number of unique terms in this
            // field is bounded by the number of fields in the mappings.
            return new TermRangeQuery(name(), null, null, true, true);
        }

        @Override
        public ValueFetcher valueFetcher(MapperService mapperService, SearchLookup lookup, String format) {
            throw new UnsupportedOperationException("Cannot fetch values for internal field [" + name() + "].");
        }
    }

    private IgnoredFieldMapper() {
        super(IgnoredFieldType.INSTANCE);
    }

    @Override
    public void postParse(ParseContext context) {
        for (String field : context.getIgnoredFields()) {
            context.doc().add(new Field(NAME, field, Defaults.FIELD_TYPE));
        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

}

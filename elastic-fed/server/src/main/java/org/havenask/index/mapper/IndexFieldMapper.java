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

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.havenask.common.Strings;
import org.havenask.index.fielddata.IndexFieldData;
import org.havenask.index.fielddata.plain.ConstantIndexFieldData;
import org.havenask.index.query.QueryShardContext;
import org.havenask.search.aggregations.support.CoreValuesSourceType;
import org.havenask.search.lookup.SearchLookup;

import java.util.Collections;
import java.util.function.Supplier;

public class IndexFieldMapper extends MetadataFieldMapper {

    public static final String NAME = "_index";

    public static final String CONTENT_TYPE = "_index";

    public static final TypeParser PARSER = new FixedTypeParser(c -> new IndexFieldMapper());

    static final class IndexFieldType extends ConstantFieldType {

        static final IndexFieldType INSTANCE = new IndexFieldType();

        private IndexFieldType() {
            super(NAME, Collections.emptyMap());
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        protected boolean matches(String pattern, boolean caseInsensitive, QueryShardContext context) {
            if (caseInsensitive) {
                // Thankfully, all index names are lower-cased so we don't have to pass a case_insensitive mode flag
                // down to all the index name-matching logic. We just lower-case the search string
                pattern = Strings.toLowercaseAscii(pattern);
            }
            return context.indexMatches(pattern);
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            return new MatchAllDocsQuery();
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName, Supplier<SearchLookup> searchLookup) {
            return new ConstantIndexFieldData.Builder(fullyQualifiedIndexName, name(), CoreValuesSourceType.BYTES);
        }

        @Override
        public ValueFetcher valueFetcher(MapperService mapperService, SearchLookup searchLookup, String format) {
            throw new UnsupportedOperationException("Cannot fetch values for internal field [" + name() + "].");
        }
    }

    public IndexFieldMapper() {
        super(IndexFieldType.INSTANCE);
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }
}

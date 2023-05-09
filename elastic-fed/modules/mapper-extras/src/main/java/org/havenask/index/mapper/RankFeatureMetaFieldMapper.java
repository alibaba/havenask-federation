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

import org.apache.lucene.search.Query;
import org.havenask.index.query.QueryShardContext;
import org.havenask.search.lookup.SearchLookup;

import java.util.Collections;

/**
 * This meta field only exists because rank feature fields index everything into a
 * common _feature field and Havenask has a custom codec that complains
 * when fields exist in the index and not in mappings.
 */
public class RankFeatureMetaFieldMapper extends MetadataFieldMapper {

    public static final String NAME = "_feature";

    public static final String CONTENT_TYPE = "_feature";

    public static final TypeParser PARSER = new FixedTypeParser(c -> new RankFeatureMetaFieldMapper());

    public static final class RankFeatureMetaFieldType extends MappedFieldType {

        public static final RankFeatureMetaFieldType INSTANCE = new RankFeatureMetaFieldType();

        private RankFeatureMetaFieldType() {
            super(NAME, false, false, false, TextSearchInfo.NONE, Collections.emptyMap());
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public ValueFetcher valueFetcher(MapperService mapperService, SearchLookup searchLookup, String format) {
            throw new UnsupportedOperationException("Cannot fetch values for internal field [" + typeName() + "].");
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            throw new UnsupportedOperationException("Cannot run exists query on [_feature]");
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            throw new UnsupportedOperationException("The [_feature] field may not be queried directly");
        }
    }

    private RankFeatureMetaFieldMapper() {
        super(RankFeatureMetaFieldType.INSTANCE);
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

}

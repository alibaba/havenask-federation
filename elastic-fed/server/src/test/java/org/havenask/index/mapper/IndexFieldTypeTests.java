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
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.lucene.search.MatchNoDocsQuery;
import org.havenask.Version;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.common.regex.Regex;
import org.havenask.common.settings.Settings;
import org.havenask.index.IndexSettings;
import org.havenask.index.query.QueryShardContext;
import org.havenask.index.query.QueryShardException;
import org.havenask.test.HavenaskTestCase;

import java.util.function.Predicate;

import static org.hamcrest.Matchers.containsString;

public class IndexFieldTypeTests extends HavenaskTestCase {

    public void testPrefixQuery() {
        MappedFieldType ft = IndexFieldMapper.IndexFieldType.INSTANCE;

        assertEquals(new MatchAllDocsQuery(), ft.prefixQuery("ind", null, createContext()));
        assertEquals(new MatchNoDocsQuery(), ft.prefixQuery("other_ind", null, createContext()));
    }

    public void testWildcardQuery() {
        MappedFieldType ft = IndexFieldMapper.IndexFieldType.INSTANCE;

        assertEquals(new MatchAllDocsQuery(), ft.wildcardQuery("ind*x", null, createContext()));
        assertEquals(new MatchAllDocsQuery(), ft.wildcardQuery("iNd*x", null, true, createContext()));
        assertEquals(new MatchNoDocsQuery(), ft.wildcardQuery("other_ind*x", null, createContext()));
        assertEquals(new MatchNoDocsQuery(), ft.wildcardQuery("Other_ind*x", null, true, createContext()));
    }

    public void testRegexpQuery() {
        MappedFieldType ft = IndexFieldMapper.IndexFieldType.INSTANCE;

        QueryShardException e = expectThrows(QueryShardException.class, () ->
            assertEquals(new MatchAllDocsQuery(), ft.regexpQuery("ind.x", 0, 0, 10, null, createContext())));
        assertThat(e.getMessage(), containsString("Can only use regexp queries on keyword and text fields"));
    }

    private QueryShardContext createContext() {
        IndexMetadata indexMetadata = IndexMetadata.builder("index")
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        IndexSettings indexSettings = new IndexSettings(indexMetadata, Settings.EMPTY);

        Predicate<String> indexNameMatcher = pattern -> Regex.simpleMatch(pattern, "index");
        return new QueryShardContext(0, indexSettings, null, null, null, null, null, null, xContentRegistry(), writableRegistry(),
            null, null, System::currentTimeMillis, null, indexNameMatcher, () -> true, null);
    }
}

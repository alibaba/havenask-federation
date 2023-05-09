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

import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.havenask.HavenaskException;
import org.havenask.common.lucene.Lucene;
import org.havenask.index.mapper.SearchAsYouTypeFieldMapper.Defaults;
import org.havenask.index.mapper.SearchAsYouTypeFieldMapper.PrefixFieldType;
import org.havenask.index.mapper.SearchAsYouTypeFieldMapper.SearchAsYouTypeFieldType;
import org.havenask.index.mapper.SearchAsYouTypeFieldMapper.ShingleFieldType;

import java.io.IOException;
import java.util.Collections;

import static java.util.Arrays.asList;
import static org.apache.lucene.search.MultiTermQuery.CONSTANT_SCORE_REWRITE;
import static org.hamcrest.Matchers.equalTo;

public class SearchAsYouTypeFieldTypeTests extends FieldTypeTestCase {

    private static final String NAME = "a_field";
    private static final FieldType UNSEARCHABLE = new FieldType();
    static {
        UNSEARCHABLE.setIndexOptions(IndexOptions.NONE);
        UNSEARCHABLE.freeze();
    }

    private static final FieldType SEARCHABLE = new FieldType();
    static {
        SEARCHABLE.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
        SEARCHABLE.freeze();
    }

    private static SearchAsYouTypeFieldType createFieldType() {
        final SearchAsYouTypeFieldType fieldType = new SearchAsYouTypeFieldType(NAME, SEARCHABLE, null,
            Lucene.STANDARD_ANALYZER, Lucene.STANDARD_ANALYZER, Collections.emptyMap());
        fieldType.setPrefixField(new PrefixFieldType(NAME, TextSearchInfo.SIMPLE_MATCH_ONLY, Defaults.MIN_GRAM, Defaults.MAX_GRAM));
        fieldType.setShingleFields(new ShingleFieldType[] {
            new ShingleFieldType(fieldType.name(), 2, TextSearchInfo.SIMPLE_MATCH_ONLY)
        });
        return fieldType;
    }

    public void testTermQuery() {
        final MappedFieldType fieldType = createFieldType();

        assertThat(fieldType.termQuery("foo", null), equalTo(new TermQuery(new Term(NAME, "foo"))));

        SearchAsYouTypeFieldType unsearchable = new SearchAsYouTypeFieldType(NAME, UNSEARCHABLE, null,
            Lucene.STANDARD_ANALYZER, Lucene.STANDARD_ANALYZER, Collections.emptyMap());
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> unsearchable.termQuery("foo", null));
        assertThat(e.getMessage(), equalTo("Cannot search on field [" + NAME + "] since it is not indexed."));
    }

    public void testTermsQuery() {
        final MappedFieldType fieldType = createFieldType();

        assertThat(fieldType.termsQuery(asList("foo", "bar"), null),
            equalTo(new TermInSetQuery(NAME, asList(new BytesRef("foo"), new BytesRef("bar")))));

        SearchAsYouTypeFieldType unsearchable = new SearchAsYouTypeFieldType(NAME, UNSEARCHABLE, null,
            Lucene.STANDARD_ANALYZER, Lucene.STANDARD_ANALYZER, Collections.emptyMap());
        final IllegalArgumentException e =
            expectThrows(IllegalArgumentException.class, () -> unsearchable.termsQuery(asList("foo", "bar"), null));
        assertThat(e.getMessage(), equalTo("Cannot search on field [" + NAME + "] since it is not indexed."));
    }

    public void testPrefixQuery() {
        final SearchAsYouTypeFieldType fieldType = createFieldType();

        // this term should be a length that can be rewriteable to a term query on the prefix field
        final String withinBoundsTerm = "foo";
        assertThat(fieldType.prefixQuery(withinBoundsTerm, CONSTANT_SCORE_REWRITE, randomMockShardContext()),
            equalTo(new ConstantScoreQuery(new TermQuery(new Term(NAME + "._index_prefix", withinBoundsTerm)))));

        // our defaults don't allow a situation where a term can be too small

        // this term should be too long to be rewriteable to a term query on the prefix field
        final String longTerm = "toolongforourprefixfieldthistermis";
        assertThat(fieldType.prefixQuery(longTerm, CONSTANT_SCORE_REWRITE, MOCK_QSC),
            equalTo(new PrefixQuery(new Term(NAME, longTerm))));

        HavenaskException ee = expectThrows(HavenaskException.class,
                () -> fieldType.prefixQuery(longTerm, CONSTANT_SCORE_REWRITE, MOCK_QSC_DISALLOW_EXPENSIVE));
        assertEquals("[prefix] queries cannot be executed when 'search.allow_expensive_queries' is set to false. " +
                "For optimised prefix queries on text fields please enable [index_prefixes].", ee.getMessage());
    }

    public void testFetchSourceValue() throws IOException {
        SearchAsYouTypeFieldType fieldType = createFieldType();
        fieldType.setIndexAnalyzer(Lucene.STANDARD_ANALYZER);

        assertEquals(org.havenask.common.collect.List.of("value"), fetchSourceValue(fieldType, "value"));
        assertEquals(org.havenask.common.collect.List.of("42"), fetchSourceValue(fieldType, 42L));
        assertEquals(org.havenask.common.collect.List.of("true"), fetchSourceValue(fieldType, true));

        SearchAsYouTypeFieldMapper.PrefixFieldType prefixFieldType = new SearchAsYouTypeFieldMapper.PrefixFieldType(
            fieldType.name(), fieldType.getTextSearchInfo(), 2, 10);
        assertEquals(org.havenask.common.collect.List.of("value"), fetchSourceValue(prefixFieldType, "value"));
        assertEquals(org.havenask.common.collect.List.of("42"), fetchSourceValue(prefixFieldType, 42L));
        assertEquals(org.havenask.common.collect.List.of("true"), fetchSourceValue(prefixFieldType, true));

        SearchAsYouTypeFieldMapper.ShingleFieldType shingleFieldType = new SearchAsYouTypeFieldMapper.ShingleFieldType(
            fieldType.name(), 5, fieldType.getTextSearchInfo());
        assertEquals(org.havenask.common.collect.List.of("value"), fetchSourceValue(shingleFieldType, "value"));
        assertEquals(org.havenask.common.collect.List.of("42"), fetchSourceValue(shingleFieldType, 42L));
        assertEquals(org.havenask.common.collect.List.of("true"), fetchSourceValue(shingleFieldType, true));
    }
}

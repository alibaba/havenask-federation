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

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.havenask.index.IndexSettings;
import org.havenask.index.analysis.AnalysisRegistry;
import org.havenask.index.analysis.AnalyzerScope;
import org.havenask.index.analysis.IndexAnalyzers;
import org.havenask.index.analysis.NamedAnalyzer;
import org.havenask.index.mapper.MapperServiceTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class DefaultAnalyzersTests extends MapperServiceTestCase {

    private boolean setDefaultSearchAnalyzer;
    private boolean setDefaultSearchQuoteAnalyzer;

    @Override
    protected IndexAnalyzers createIndexAnalyzers(IndexSettings indexSettings) {
        Map<String, NamedAnalyzer> analyzers = new HashMap<>();
        analyzers.put(AnalysisRegistry.DEFAULT_ANALYZER_NAME, new NamedAnalyzer("default", AnalyzerScope.INDEX, new StandardAnalyzer()));
        if (setDefaultSearchAnalyzer) {
            analyzers.put(AnalysisRegistry.DEFAULT_SEARCH_ANALYZER_NAME,
                new NamedAnalyzer("default_search", AnalyzerScope.INDEX, new StandardAnalyzer()));
        }
        if (setDefaultSearchQuoteAnalyzer) {
            analyzers.put(AnalysisRegistry.DEFAULT_SEARCH_QUOTED_ANALYZER_NAME,
                new NamedAnalyzer("default_search_quote", AnalyzerScope.INDEX, new StandardAnalyzer()));
        }
        analyzers.put("configured", new NamedAnalyzer("configured", AnalyzerScope.INDEX, new StandardAnalyzer()));
        return new IndexAnalyzers(
            analyzers,
            Collections.emptyMap(),
            Collections.emptyMap()
        );
    }

    public void testDefaultSearchAnalyzer() throws IOException {
        {
            setDefaultSearchAnalyzer = false;
            MapperService ms = createMapperService(fieldMapping(b -> b.field("type", "text")));
            MappedFieldType ft = ms.fieldType("field");
            assertEquals("default", ft.getTextSearchInfo().getSearchAnalyzer().name());
        }
        {
            setDefaultSearchAnalyzer = false;
            MapperService ms = createMapperService(fieldMapping(b -> b.field("type", "text").field("search_analyzer", "configured")));
            MappedFieldType ft = ms.fieldType("field");
            assertEquals("configured", ft.getTextSearchInfo().getSearchAnalyzer().name());
        }
        {
            setDefaultSearchAnalyzer = true;
            MapperService ms = createMapperService(fieldMapping(b -> b.field("type", "text")));
            MappedFieldType ft = ms.fieldType("field");
            assertEquals("default_search", ft.getTextSearchInfo().getSearchAnalyzer().name());
        }
        {
            setDefaultSearchAnalyzer = true;
            MapperService ms = createMapperService(fieldMapping(b -> b.field("type", "text").field("search_analyzer", "configured")));
            MappedFieldType ft = ms.fieldType("field");
            assertEquals("configured", ft.getTextSearchInfo().getSearchAnalyzer().name());
        }

    }

    public void testDefaultSearchQuoteAnalyzer() throws IOException {
        {
            setDefaultSearchQuoteAnalyzer = false;
            setDefaultSearchAnalyzer = false;
            MapperService ms = createMapperService(fieldMapping(b -> b.field("type", "text")));
            MappedFieldType ft = ms.fieldType("field");
            assertEquals("default", ft.getTextSearchInfo().getSearchQuoteAnalyzer().name());
        }
        {
            setDefaultSearchQuoteAnalyzer = false;
            setDefaultSearchAnalyzer = false;
            MapperService ms
                = createMapperService(fieldMapping(b -> b.field("type", "text").field("search_quote_analyzer", "configured")));
            MappedFieldType ft = ms.fieldType("field");
            assertEquals("configured", ft.getTextSearchInfo().getSearchQuoteAnalyzer().name());
        }
        {
            setDefaultSearchQuoteAnalyzer = true;
            setDefaultSearchAnalyzer = false;
            MapperService ms = createMapperService(fieldMapping(b -> b.field("type", "text")));
            MappedFieldType ft = ms.fieldType("field");
            assertEquals("default_search_quote", ft.getTextSearchInfo().getSearchQuoteAnalyzer().name());
        }
        {
            setDefaultSearchQuoteAnalyzer = true;
            setDefaultSearchAnalyzer = false;
            MapperService ms
                = createMapperService(fieldMapping(b -> b.field("type", "text").field("search_quote_analyzer", "configured")));
            MappedFieldType ft = ms.fieldType("field");
            assertEquals("configured", ft.getTextSearchInfo().getSearchQuoteAnalyzer().name());
        }
        {
            setDefaultSearchQuoteAnalyzer = false;
            setDefaultSearchAnalyzer = true;
            MapperService ms = createMapperService(fieldMapping(b -> b.field("type", "text")));
            MappedFieldType ft = ms.fieldType("field");
            assertEquals("default_search", ft.getTextSearchInfo().getSearchQuoteAnalyzer().name());
        }
        {
            setDefaultSearchQuoteAnalyzer = false;
            setDefaultSearchAnalyzer = true;
            MapperService ms
                = createMapperService(fieldMapping(b -> b.field("type", "text").field("search_quote_analyzer", "configured")));
            MappedFieldType ft = ms.fieldType("field");
            assertEquals("configured", ft.getTextSearchInfo().getSearchQuoteAnalyzer().name());
        }
        {
            setDefaultSearchQuoteAnalyzer = true;
            setDefaultSearchAnalyzer = true;
            MapperService ms = createMapperService(fieldMapping(b -> b.field("type", "text")));
            MappedFieldType ft = ms.fieldType("field");
            assertEquals("default_search_quote", ft.getTextSearchInfo().getSearchQuoteAnalyzer().name());
        }
        {
            setDefaultSearchQuoteAnalyzer = true;
            setDefaultSearchAnalyzer = true;
            MapperService ms
                = createMapperService(fieldMapping(b -> b.field("type", "text").field("search_quote_analyzer", "configured")));
            MappedFieldType ft = ms.fieldType("field");
            assertEquals("configured", ft.getTextSearchInfo().getSearchQuoteAnalyzer().name());
        }
    }

}

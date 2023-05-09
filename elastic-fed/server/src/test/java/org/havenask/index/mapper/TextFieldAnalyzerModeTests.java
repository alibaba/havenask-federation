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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.havenask.Version;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.common.settings.Settings;
import org.havenask.index.IndexSettings;
import org.havenask.index.analysis.AbstractTokenFilterFactory;
import org.havenask.index.analysis.AnalysisMode;
import org.havenask.index.analysis.AnalyzerScope;
import org.havenask.index.analysis.CharFilterFactory;
import org.havenask.index.analysis.CustomAnalyzer;
import org.havenask.index.analysis.IndexAnalyzers;
import org.havenask.index.analysis.NamedAnalyzer;
import org.havenask.index.analysis.TokenFilterFactory;
import org.havenask.test.HavenaskTestCase;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.havenask.index.analysis.AnalysisRegistry.DEFAULT_ANALYZER_NAME;
import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TextFieldAnalyzerModeTests extends HavenaskTestCase {

    private static Map<String, NamedAnalyzer> defaultAnalyzers() {
        Map<String, NamedAnalyzer> analyzers = new HashMap<>();
        analyzers.put(DEFAULT_ANALYZER_NAME, new NamedAnalyzer("default", AnalyzerScope.INDEX, null));
        return analyzers;
    }

    private static final IndexMetadata EMPTY_INDEX_METADATA = IndexMetadata.builder("")
        .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
        .numberOfShards(1).numberOfReplicas(0).build();
    private static final IndexSettings indexSettings = new IndexSettings(EMPTY_INDEX_METADATA, Settings.EMPTY);


    private Analyzer createAnalyzerWithMode(AnalysisMode mode) {
        TokenFilterFactory tokenFilter = new AbstractTokenFilterFactory(indexSettings, "my_analyzer", Settings.EMPTY) {
            @Override
            public AnalysisMode getAnalysisMode() {
                return mode;
            }

            @Override
            public TokenStream create(TokenStream tokenStream) {
                return null;
            }
        };
        return new CustomAnalyzer(null, new CharFilterFactory[0],
            new TokenFilterFactory[] { tokenFilter  });
    }

    public void testParseTextFieldCheckAnalyzerAnalysisMode() {

        Map<String, Object> fieldNode = new HashMap<>();
        fieldNode.put("analyzer", "my_analyzer");
        Mapper.TypeParser.ParserContext parserContext = mock(Mapper.TypeParser.ParserContext.class);

        // check AnalysisMode.ALL works
        Map<String, NamedAnalyzer> analyzers = defaultAnalyzers();
        analyzers.put("my_analyzer",
            new NamedAnalyzer("my_named_analyzer", AnalyzerScope.INDEX, createAnalyzerWithMode(AnalysisMode.ALL)));

        IndexAnalyzers indexAnalyzers = new IndexAnalyzers(analyzers, Collections.emptyMap(), Collections.emptyMap());
        when(parserContext.getIndexAnalyzers()).thenReturn(indexAnalyzers);

        TextFieldMapper.PARSER.parse("field", fieldNode, parserContext);

        // check that "analyzer" set to something that only supports AnalysisMode.SEARCH_TIME or AnalysisMode.INDEX_TIME is blocked
        AnalysisMode mode = randomFrom(AnalysisMode.SEARCH_TIME, AnalysisMode.INDEX_TIME);
        analyzers = defaultAnalyzers();
        analyzers.put("my_analyzer", new NamedAnalyzer("my_named_analyzer", AnalyzerScope.INDEX,
            createAnalyzerWithMode(mode)));
        indexAnalyzers = new IndexAnalyzers(analyzers, Collections.emptyMap(), Collections.emptyMap());
        when(parserContext.getIndexAnalyzers()).thenReturn(indexAnalyzers);
        fieldNode.put("analyzer", "my_analyzer");
        MapperException ex = expectThrows(MapperException.class, () -> {
            TextFieldMapper.PARSER.parse("name", fieldNode, parserContext);
        });
        assertThat(ex.getMessage(),
            containsString("analyzer [my_named_analyzer] contains filters [my_analyzer] that are not allowed to run"));
    }

    public void testParseTextFieldCheckSearchAnalyzerAnalysisMode() {

        for (String settingToTest : new String[] { "search_analyzer", "search_quote_analyzer" }) {
            Map<String, Object> fieldNode = new HashMap<>();
            fieldNode.put(settingToTest, "my_analyzer");
            fieldNode.put("analyzer", "standard");
            if (settingToTest.equals("search_quote_analyzer")) {
                fieldNode.put("search_analyzer", "standard");
            }
            Mapper.TypeParser.ParserContext parserContext = mock(Mapper.TypeParser.ParserContext.class);

            // check AnalysisMode.ALL and AnalysisMode.SEARCH_TIME works
            Map<String, NamedAnalyzer> analyzers = defaultAnalyzers();
            AnalysisMode mode = randomFrom(AnalysisMode.ALL, AnalysisMode.SEARCH_TIME);
            analyzers.put("my_analyzer",
                new NamedAnalyzer("my_named_analyzer", AnalyzerScope.INDEX, createAnalyzerWithMode(mode)));
            analyzers.put("standard", new NamedAnalyzer("standard", AnalyzerScope.INDEX, new StandardAnalyzer()));

            IndexAnalyzers indexAnalyzers = new IndexAnalyzers(analyzers, Collections.emptyMap(), Collections.emptyMap());
            when(parserContext.getIndexAnalyzers()).thenReturn(indexAnalyzers);
            TextFieldMapper.PARSER.parse("textField", fieldNode, parserContext);

            // check that "analyzer" set to AnalysisMode.INDEX_TIME is blocked
            mode = AnalysisMode.INDEX_TIME;
            analyzers = defaultAnalyzers();
            analyzers.put("my_analyzer",
                new NamedAnalyzer("my_named_analyzer", AnalyzerScope.INDEX, createAnalyzerWithMode(mode)));
            analyzers.put("standard", new NamedAnalyzer("standard", AnalyzerScope.INDEX, new StandardAnalyzer()));
            indexAnalyzers = new IndexAnalyzers(analyzers, Collections.emptyMap(), Collections.emptyMap());
            when(parserContext.getIndexAnalyzers()).thenReturn(indexAnalyzers);
            fieldNode.clear();
            fieldNode.put(settingToTest, "my_analyzer");
            fieldNode.put("analyzer", "standard");
            if (settingToTest.equals("search_quote_analyzer")) {
                fieldNode.put("search_analyzer", "standard");
            }
            MapperException ex = expectThrows(MapperException.class, () -> {
                TextFieldMapper.PARSER.parse("field", fieldNode, parserContext);
            });
            assertEquals("analyzer [my_named_analyzer] contains filters [my_analyzer] that are not allowed to run in search time mode.",
                ex.getMessage());
        }
    }

    public void testParseTextFieldCheckAnalyzerWithSearchAnalyzerAnalysisMode() {

        Map<String, Object> fieldNode = new HashMap<>();
        fieldNode.put("analyzer", "my_analyzer");
        Mapper.TypeParser.ParserContext parserContext = mock(Mapper.TypeParser.ParserContext.class);

        // check that "analyzer" set to AnalysisMode.INDEX_TIME is blocked if there is no search analyzer
        AnalysisMode mode = AnalysisMode.INDEX_TIME;
        Map<String, NamedAnalyzer> analyzers = defaultAnalyzers();
        analyzers.put("my_analyzer",
            new NamedAnalyzer("my_named_analyzer", AnalyzerScope.INDEX, createAnalyzerWithMode(mode)));
        IndexAnalyzers indexAnalyzers = new IndexAnalyzers(analyzers, Collections.emptyMap(), Collections.emptyMap());
        when(parserContext.getIndexAnalyzers()).thenReturn(indexAnalyzers);
        MapperException ex = expectThrows(MapperException.class, () -> {
            TextFieldMapper.PARSER.parse("field", fieldNode, parserContext);
        });
        assertThat(ex.getMessage(),
            containsString("analyzer [my_named_analyzer] contains filters [my_analyzer] that are not allowed to run"));

        // check AnalysisMode.INDEX_TIME is okay if search analyzer is also set
        fieldNode.put("analyzer", "my_analyzer");
        fieldNode.put("search_analyzer", "standard");
        analyzers = defaultAnalyzers();
        mode = randomFrom(AnalysisMode.ALL, AnalysisMode.INDEX_TIME);
        analyzers.put("my_analyzer",
            new NamedAnalyzer("my_named_analyzer", AnalyzerScope.INDEX, createAnalyzerWithMode(mode)));
        analyzers.put("standard", new NamedAnalyzer("standard", AnalyzerScope.INDEX, new StandardAnalyzer()));

        indexAnalyzers = new IndexAnalyzers(analyzers, Collections.emptyMap(), Collections.emptyMap());
        when(parserContext.getIndexAnalyzers()).thenReturn(indexAnalyzers);
        TextFieldMapper.PARSER.parse("field", fieldNode, parserContext);
    }

}

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

package org.havenask.analysis.common;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.havenask.LegacyESVersion;
import org.havenask.Version;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.common.settings.Settings;
import org.havenask.env.Environment;
import org.havenask.env.TestEnvironment;
import org.havenask.index.IndexSettings;
import org.havenask.index.analysis.AnalysisTestsHelper;
import org.havenask.index.analysis.IndexAnalyzers;
import org.havenask.index.analysis.NamedAnalyzer;
import org.havenask.index.analysis.TokenFilterFactory;
import org.havenask.indices.analysis.AnalysisModule;
import org.havenask.test.HavenaskTestCase;
import org.havenask.test.IndexSettingsModule;
import org.havenask.test.VersionUtils;

import java.io.IOException;
import java.io.StringReader;
import java.util.Collections;

public class WordDelimiterGraphTokenFilterFactoryTests
        extends BaseWordDelimiterTokenFilterFactoryTestCase {
    public WordDelimiterGraphTokenFilterFactoryTests() {
        super("word_delimiter_graph");
    }

    public void testMultiTerms() throws IOException {
        HavenaskTestCase.TestAnalysis analysis = AnalysisTestsHelper.createTestAnalysisFromSettings(
                Settings.builder()
                    .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                    .put("index.analysis.filter.my_word_delimiter.type", type)
                    .put("index.analysis.filter.my_word_delimiter.catenate_all", "true")
                    .put("index.analysis.filter.my_word_delimiter.preserve_original", "true")
                    .build(),
                new CommonAnalysisPlugin());

        TokenFilterFactory tokenFilter = analysis.tokenFilter.get("my_word_delimiter");
        String source = "PowerShot 500-42 wi-fi wi-fi-4000 j2se O'Neil's";
        String[] expected = new String[] { "PowerShot", "PowerShot", "Power", "Shot", "500-42",
                "50042", "500", "42", "wi-fi", "wifi", "wi", "fi", "wi-fi-4000", "wifi4000", "wi",
                "fi", "4000", "j2se", "j2se", "j", "2", "se", "O'Neil's", "ONeil", "O", "Neil" };
        Tokenizer tokenizer = new WhitespaceTokenizer();
        tokenizer.setReader(new StringReader(source));
        int[] expectedIncr = new int[] { 1, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 0, 0,
                1, 1, 1, 0, 0, 1 };
        int[] expectedPosLen = new int[] { 2, 2, 1, 1, 2, 2, 1, 1, 2, 2, 1, 1, 3, 3, 1, 1, 1, 3, 3,
                1, 1, 1, 2, 2, 1, 1 };
        assertTokenStreamContents(tokenFilter.create(tokenizer), expected, null, null, null,
                expectedIncr, expectedPosLen, null);
    }

    /**
     * Correct offset order when doing both parts and concatenation: PowerShot is a synonym of Power
     */
    public void testPartsAndCatenate() throws IOException {
        HavenaskTestCase.TestAnalysis analysis = AnalysisTestsHelper.createTestAnalysisFromSettings(
                Settings.builder()
                    .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                    .put("index.analysis.filter.my_word_delimiter.type", type)
                    .put("index.analysis.filter.my_word_delimiter.catenate_words", "true")
                    .put("index.analysis.filter.my_word_delimiter.generate_word_parts", "true")
                    .build(),
                new CommonAnalysisPlugin());
        TokenFilterFactory tokenFilter = analysis.tokenFilter.get("my_word_delimiter");
        String source = "PowerShot";
        int[] expectedIncr = new int[]{1, 0, 1};
        int[] expectedPosLen = new int[]{2, 1, 1};
        int[] expectedStartOffsets = new int[]{0, 0, 5};
        int[] expectedEndOffsets = new int[]{9, 5, 9};
        String[] expected = new String[]{"PowerShot", "Power", "Shot" };
        Tokenizer tokenizer = new WhitespaceTokenizer();
        tokenizer.setReader(new StringReader(source));
        assertTokenStreamContents(tokenFilter.create(tokenizer), expected, expectedStartOffsets, expectedEndOffsets, null,
            expectedIncr, expectedPosLen, null);
    }

    public void testAdjustingOffsets() throws IOException {
        HavenaskTestCase.TestAnalysis analysis = AnalysisTestsHelper.createTestAnalysisFromSettings(
            Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .put("index.analysis.filter.my_word_delimiter.type", type)
                .put("index.analysis.filter.my_word_delimiter.catenate_words", "true")
                .put("index.analysis.filter.my_word_delimiter.generate_word_parts", "true")
                .put("index.analysis.filter.my_word_delimiter.adjust_offsets", "false")
                .build(),
            new CommonAnalysisPlugin());
        TokenFilterFactory tokenFilter = analysis.tokenFilter.get("my_word_delimiter");
        String source = "PowerShot";
        int[] expectedIncr = new int[]{1, 0, 1};
        int[] expectedPosLen = new int[]{2, 1, 1};
        int[] expectedStartOffsets = new int[]{0, 0, 0};
        int[] expectedEndOffsets = new int[]{9, 9, 9};
        String[] expected = new String[]{"PowerShot", "Power", "Shot" };
        Tokenizer tokenizer = new WhitespaceTokenizer();
        tokenizer.setReader(new StringReader(source));
        assertTokenStreamContents(tokenFilter.create(tokenizer), expected, expectedStartOffsets, expectedEndOffsets, null,
            expectedIncr, expectedPosLen, null);
    }

    public void testIgnoreKeywords() throws IOException {
        //test with keywords but ignore is false (default behavior)
        Settings settings = Settings.builder()
                .put("index.analysis.filter.my_word_delimiter.type", type)
                .put("index.analysis.filter.my_word_delimiter.generate_word_parts", "true")
                .put("index.analysis.filter.my_keyword.type", "keyword_marker")
                .put("index.analysis.filter.my_keyword.keywords", "PowerHungry")
                .put("index.analysis.analyzer.my_analyzer.type", "custom")
                .put("index.analysis.analyzer.my_analyzer.tokenizer", "whitespace")
                .put("index.analysis.analyzer.my_analyzer.filter", "my_keyword, my_word_delimiter")
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .build();
        HavenaskTestCase.TestAnalysis analysis = AnalysisTestsHelper.createTestAnalysisFromSettings(settings, new CommonAnalysisPlugin());
        String source = "PowerShot PowerHungry";
        int[] expectedStartOffsets = new int[]{0, 5, 10, 15};
        int[] expectedEndOffsets = new int[]{5, 9, 15, 21};
        String[] expected = new String[]{"Power", "Shot", "Power", "Hungry"};
        NamedAnalyzer analyzer = analysis.indexAnalyzers.get("my_analyzer");
        assertAnalyzesTo(analyzer, source, expected, expectedStartOffsets, expectedEndOffsets);

        //test with keywords but ignore_keywords is set as true
        settings = Settings.builder().put(settings)
                .put("index.analysis.filter.my_word_delimiter.ignore_keywords", "true")
                .build();
        analysis = AnalysisTestsHelper.createTestAnalysisFromSettings(settings, new CommonAnalysisPlugin());
        analyzer = analysis.indexAnalyzers.get("my_analyzer");
        expectedStartOffsets = new int[]{0, 5, 10};
        expectedEndOffsets = new int[]{5, 9, 21};
        expected = new String[]{"Power", "Shot", "PowerHungry"};
        assertAnalyzesTo(analyzer, source, expected, expectedStartOffsets, expectedEndOffsets);
    }

    public void testPreconfiguredFilter() throws IOException {
        // Before 7.3 we don't adjust offsets
        {
            Settings settings = Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .build();
            Settings indexSettings = Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED,
                    VersionUtils.randomVersionBetween(
                        random(), LegacyESVersion.V_7_0_0, VersionUtils.getPreviousVersion(LegacyESVersion.V_7_3_0)))
                .put("index.analysis.analyzer.my_analyzer.tokenizer", "standard")
                .putList("index.analysis.analyzer.my_analyzer.filter", "word_delimiter_graph")
                .build();
            IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", indexSettings);

            try (IndexAnalyzers indexAnalyzers = new AnalysisModule(TestEnvironment.newEnvironment(settings),
                Collections.singletonList(new CommonAnalysisPlugin())).getAnalysisRegistry().build(idxSettings)) {

                NamedAnalyzer analyzer = indexAnalyzers.get("my_analyzer");
                assertNotNull(analyzer);
                assertAnalyzesTo(analyzer, "h100", new String[]{"h", "100"}, new int[]{ 0, 0 }, new int[]{ 4, 4 });

            }
        }

        // Afger 7.3 we do adjust offsets
        {
            Settings settings = Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .build();
            Settings indexSettings = Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put("index.analysis.analyzer.my_analyzer.tokenizer", "standard")
                .putList("index.analysis.analyzer.my_analyzer.filter", "word_delimiter_graph")
                .build();
            IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", indexSettings);

            try (IndexAnalyzers indexAnalyzers = new AnalysisModule(TestEnvironment.newEnvironment(settings),
                Collections.singletonList(new CommonAnalysisPlugin())).getAnalysisRegistry().build(idxSettings)) {

                NamedAnalyzer analyzer = indexAnalyzers.get("my_analyzer");
                assertNotNull(analyzer);
                assertAnalyzesTo(analyzer, "h100", new String[]{"h", "100"}, new int[]{ 0, 1 }, new int[]{ 1, 4 });

            }
        }
    }
}

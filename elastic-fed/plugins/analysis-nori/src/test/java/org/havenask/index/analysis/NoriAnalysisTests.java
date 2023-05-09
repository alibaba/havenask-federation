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

package org.havenask.index.analysis;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.ko.KoreanAnalyzer;
import org.apache.lucene.analysis.ko.KoreanTokenizer;
import org.havenask.Version;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.common.settings.Settings;
import org.havenask.env.Environment;
import org.havenask.plugin.analysis.nori.AnalysisNoriPlugin;
import org.havenask.test.HavenaskTestCase.TestAnalysis;
import org.havenask.test.HavenaskTokenStreamTestCase;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

public class NoriAnalysisTests extends HavenaskTokenStreamTestCase {
    public void testDefaultsNoriAnalysis() throws IOException {
        TestAnalysis analysis = createTestAnalysis(Settings.EMPTY);

        TokenizerFactory tokenizerFactory = analysis.tokenizer.get("nori_tokenizer");
        assertThat(tokenizerFactory, instanceOf(NoriTokenizerFactory.class));

        TokenFilterFactory filterFactory = analysis.tokenFilter.get("nori_part_of_speech");
        assertThat(filterFactory, instanceOf(NoriPartOfSpeechStopFilterFactory.class));

        filterFactory = analysis.tokenFilter.get("nori_readingform");
        assertThat(filterFactory, instanceOf(NoriReadingFormFilterFactory.class));

        filterFactory = analysis.tokenFilter.get("nori_number");
        assertThat(filterFactory, instanceOf(NoriNumberFilterFactory.class));

        IndexAnalyzers indexAnalyzers = analysis.indexAnalyzers;
        NamedAnalyzer analyzer = indexAnalyzers.get("nori");
        assertThat(analyzer.analyzer(), instanceOf(KoreanAnalyzer.class));
    }

    public void testNoriAnalyzer() throws Exception {
        Settings settings = Settings.builder()
            .put("index.analysis.analyzer.my_analyzer.type", "nori")
            .put("index.analysis.analyzer.my_analyzer.stoptags", "NR, SP")
            .put("index.analysis.analyzer.my_analyzer.decompound_mode", "mixed")
            .build();
        TestAnalysis analysis = createTestAnalysis(settings);
        Analyzer analyzer = analysis.indexAnalyzers.get("my_analyzer");
        try (TokenStream stream = analyzer.tokenStream("", "여섯 용이" )) {
            assertTokenStreamContents(stream, new String[] {"용", "이"});
        }

        try (TokenStream stream = analyzer.tokenStream("", "가늠표")) {
            assertTokenStreamContents(stream, new String[] {"가늠표", "가늠", "표"});
        }
    }

    public void testNoriAnalyzerUserDict() throws Exception {
        Settings settings = Settings.builder()
            .put("index.analysis.analyzer.my_analyzer.type", "nori")
            .putList("index.analysis.analyzer.my_analyzer.user_dictionary_rules", "c++", "C샤프", "세종", "세종시 세종 시")
            .build();
        TestAnalysis analysis = createTestAnalysis(settings);
        Analyzer analyzer = analysis.indexAnalyzers.get("my_analyzer");
        try (TokenStream stream = analyzer.tokenStream("", "세종시")) {
            assertTokenStreamContents(stream, new String[]{"세종", "시"});
        }

        try (TokenStream stream = analyzer.tokenStream("", "c++world")) {
            assertTokenStreamContents(stream, new String[]{"c++", "world"});
        }
    }

    public void testNoriAnalyzerUserDictPath() throws Exception {
        Settings settings = Settings.builder()
            .put("index.analysis.analyzer.my_analyzer.type", "nori")
            .put("index.analysis.analyzer.my_analyzer.user_dictionary", "user_dict.txt")
            .build();
        TestAnalysis analysis = createTestAnalysis(settings);
        Analyzer analyzer = analysis.indexAnalyzers.get("my_analyzer");
        try (TokenStream stream = analyzer.tokenStream("", "세종시" )) {
            assertTokenStreamContents(stream, new String[] {"세종", "시"});
        }

        try (TokenStream stream = analyzer.tokenStream("", "c++world")) {
            assertTokenStreamContents(stream, new String[] {"c++", "world"});
        }
    }

    public void testNoriAnalyzerInvalidUserDictOption() throws Exception {
        Settings settings = Settings.builder()
            .put("index.analysis.analyzer.my_analyzer.type", "nori")
            .put("index.analysis.analyzer.my_analyzer.user_dictionary", "user_dict.txt")
            .putList("index.analysis.analyzer.my_analyzer.user_dictionary_rules", "c++", "C샤프", "세종", "세종시 세종 시")
            .build();
        IllegalArgumentException exc = expectThrows(IllegalArgumentException.class, () -> createTestAnalysis(settings));
        assertThat(exc.getMessage(), containsString("It is not allowed to use [user_dictionary] in conjunction " +
            "with [user_dictionary_rules]"));
    }

    public void testNoriTokenizer() throws Exception {
        Settings settings = Settings.builder()
            .put("index.analysis.tokenizer.my_tokenizer.type", "nori_tokenizer")
            .put("index.analysis.tokenizer.my_tokenizer.decompound_mode", "mixed")
            .build();
        TestAnalysis analysis = createTestAnalysis(settings);
        Tokenizer tokenizer = analysis.tokenizer.get("my_tokenizer").create();
        tokenizer.setReader(new StringReader("뿌리가 깊은 나무"));
        assertTokenStreamContents(tokenizer, new String[] {"뿌리", "가", "깊", "은", "나무"});
        tokenizer.setReader(new StringReader("가늠표"));
        assertTokenStreamContents(tokenizer, new String[] {"가늠표", "가늠", "표"});
        // discard_punctuation default(true)
        tokenizer.setReader(new StringReader("3.2개"));
        assertTokenStreamContents(tokenizer, new String[] {"3", "2", "개"});
    }

    public void testNoriTokenizerDiscardPunctuationOptionTrue() throws Exception {
        Settings settings = createDiscardPunctuationOption("true");
        TestAnalysis analysis = createTestAnalysis(settings);
        Tokenizer tokenizer = analysis.tokenizer.get("my_tokenizer").create();
        tokenizer.setReader(new StringReader("3.2개"));
        assertTokenStreamContents(tokenizer, new String[] {"3", "2", "개"});
    }

    public void testNoriTokenizerDiscardPunctuationOptionFalse() throws Exception {
        Settings settings = createDiscardPunctuationOption("false");
        TestAnalysis analysis = createTestAnalysis(settings);
        Tokenizer tokenizer = analysis.tokenizer.get("my_tokenizer").create();
        tokenizer.setReader(new StringReader("3.2개"));
        assertTokenStreamContents(tokenizer, new String[] {"3", ".", "2", "개"});
    }

    public void testNoriTokenizerInvalidDiscardPunctuationOption() {
        String wrongOption = "wrong";
        Settings settings = createDiscardPunctuationOption(wrongOption);
        IllegalArgumentException exc = expectThrows(IllegalArgumentException.class, () -> createTestAnalysis(settings));
        assertThat(exc.getMessage(), containsString("Failed to parse value [" + wrongOption
            + "] as only [true] or [false] are allowed."));
    }

    public void testNoriPartOfSpeech() throws IOException {
        Settings settings = Settings.builder()
            .put("index.analysis.filter.my_filter.type", "nori_part_of_speech")
            .put("index.analysis.filter.my_filter.stoptags", "NR, SP")
            .build();
        TestAnalysis analysis = createTestAnalysis(settings);
        TokenFilterFactory factory = analysis.tokenFilter.get("my_filter");
        Tokenizer tokenizer = new KoreanTokenizer();
        tokenizer.setReader(new StringReader("여섯 용이"));
        TokenStream stream = factory.create(tokenizer);
        assertTokenStreamContents(stream, new String[] {"용", "이"});
    }

    public void testNoriReadingForm() throws IOException {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .put("index.analysis.filter.my_filter.type", "nori_readingform")
            .build();
        TestAnalysis analysis = AnalysisTestsHelper.createTestAnalysisFromSettings(settings, new AnalysisNoriPlugin());
        TokenFilterFactory factory = analysis.tokenFilter.get("my_filter");
        Tokenizer tokenizer = new KoreanTokenizer();
        tokenizer.setReader(new StringReader("鄕歌"));
        TokenStream stream = factory.create(tokenizer);
        assertTokenStreamContents(stream, new String[] {"향가"});
    }

    public void testNoriNumber() throws IOException {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .put("index.analysis.filter.my_filter.type", "nori_number")
            .build();
        TestAnalysis analysis = AnalysisTestsHelper.createTestAnalysisFromSettings(settings, new AnalysisNoriPlugin());
        TokenFilterFactory factory = analysis.tokenFilter.get("my_filter");
        Tokenizer tokenizer = new KoreanTokenizer();
        tokenizer.setReader(new StringReader("오늘 십만이천오백원짜리 와인 구입"));
        TokenStream stream = factory.create(tokenizer);
        assertTokenStreamContents(stream, new String[] {"오늘", "102500", "원", "짜리", "와인", "구입"});
    }

    private Settings createDiscardPunctuationOption(String option) {
        return Settings.builder()
            .put("index.analysis.tokenizer.my_tokenizer.type", "nori_tokenizer")
            .put("index.analysis.tokenizer.my_tokenizer.discard_punctuation", option)
            .build();
    }

    private TestAnalysis createTestAnalysis(Settings analysisSettings) throws IOException {
        InputStream dict = NoriAnalysisTests.class.getResourceAsStream("user_dict.txt");
        Path home = createTempDir();
        Path config = home.resolve("config");
        Files.createDirectory(config);
        Files.copy(dict, config.resolve("user_dict.txt"));
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(Environment.PATH_HOME_SETTING.getKey(), home)
            .put(analysisSettings)
            .build();
        return AnalysisTestsHelper.createTestAnalysisFromSettings(settings, new AnalysisNoriPlugin());
    }
}

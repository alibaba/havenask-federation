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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.havenask.LegacyESVersion;
import org.havenask.Version;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.common.settings.Settings;
import org.havenask.env.Environment;
import org.havenask.index.IndexSettings;
import org.havenask.index.analysis.IndexAnalyzers;
import org.havenask.index.analysis.PreConfiguredTokenFilter;
import org.havenask.index.analysis.TokenFilterFactory;
import org.havenask.index.analysis.TokenizerFactory;
import org.havenask.test.HavenaskTestCase;
import org.havenask.test.IndexSettingsModule;
import org.havenask.test.VersionUtils;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;

public class SynonymsAnalysisTests extends HavenaskTestCase {
    private IndexAnalyzers indexAnalyzers;

    public void testSynonymsAnalysis() throws IOException {
        InputStream synonyms = getClass().getResourceAsStream("synonyms.txt");
        InputStream synonymsWordnet = getClass().getResourceAsStream("synonyms_wordnet.txt");
        Path home = createTempDir();
        Path config = home.resolve("config");
        Files.createDirectory(config);
        Files.copy(synonyms, config.resolve("synonyms.txt"));
        Files.copy(synonymsWordnet, config.resolve("synonyms_wordnet.txt"));

        String json = "/org/havenask/analysis/common/synonyms.json";
        Settings settings = Settings.builder().
            loadFromStream(json, getClass().getResourceAsStream(json), false)
                .put(Environment.PATH_HOME_SETTING.getKey(), home)
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build();

        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", settings);
        indexAnalyzers = createTestAnalysis(idxSettings, settings, new CommonAnalysisPlugin()).indexAnalyzers;

        match("synonymAnalyzer", "foobar is the dude abides", "fred is the havenask man!");
        match("synonymAnalyzer_file", "foobar is the dude abides", "fred is the havenask man!");
        match("synonymAnalyzerWordnet", "abstain", "abstain refrain desist");
        match("synonymAnalyzerWordnet_file", "abstain", "abstain refrain desist");
        match("synonymAnalyzerWithsettings", "foobar", "fre red");
        match("synonymAnalyzerWithStopAfterSynonym", "foobar is the dude abides , stop", "fred is the havenask man! ,");
        match("synonymAnalyzerWithStopBeforeSynonym", "foobar is the dude abides , stop", "fred is the havenask man! ,");
        match("synonymAnalyzerWithStopSynonymAfterSynonym", "foobar is the dude abides", "fred is the man!");
        match("synonymAnalyzerExpand", "foobar is the dude abides", "foobar fred is the dude havenask abides man!");
        match("synonymAnalyzerExpandWithStopAfterSynonym", "foobar is the dude abides", "fred is the dude abides man!");

    }

    public void testSynonymWordDeleteByAnalyzer() throws IOException {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put("path.home", createTempDir().toString())
            .put("index.analysis.filter.synonym.type", "synonym")
            .putList("index.analysis.filter.synonym.synonyms", "foobar => fred", "dude => havenask", "abides => man!")
            .put("index.analysis.filter.stop_within_synonym.type", "stop")
            .putList("index.analysis.filter.stop_within_synonym.stopwords", "foobar", "havenask")
            .put("index.analysis.analyzer.synonymAnalyzerWithStopSynonymBeforeSynonym.tokenizer", "whitespace")
            .putList("index.analysis.analyzer.synonymAnalyzerWithStopSynonymBeforeSynonym.filter", "stop_within_synonym","synonym")
            .build();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", settings);
        try {
            indexAnalyzers = createTestAnalysis(idxSettings, settings, new CommonAnalysisPlugin()).indexAnalyzers;
            fail("fail! due to synonym word deleted by analyzer");
        } catch (Exception e) {
            assertThat(e, instanceOf(IllegalArgumentException.class));
            assertThat(e.getMessage(), startsWith("failed to build synonyms"));
        }
    }

    public void testExpandSynonymWordDeleteByAnalyzer() throws IOException {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put("path.home", createTempDir().toString())
            .put("index.analysis.filter.synonym_expand.type", "synonym")
            .putList("index.analysis.filter.synonym_expand.synonyms", "foobar, fred", "dude, havenask", "abides, man!")
            .put("index.analysis.filter.stop_within_synonym.type", "stop")
            .putList("index.analysis.filter.stop_within_synonym.stopwords", "foobar", "havenask")
            .put("index.analysis.analyzer.synonymAnalyzerExpandWithStopBeforeSynonym.tokenizer", "whitespace")
            .putList("index.analysis.analyzer.synonymAnalyzerExpandWithStopBeforeSynonym.filter", "stop_within_synonym","synonym_expand")
            .build();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", settings);
        try {
            indexAnalyzers = createTestAnalysis(idxSettings, settings, new CommonAnalysisPlugin()).indexAnalyzers;
            fail("fail! due to synonym word deleted by analyzer");
        } catch (Exception e) {
            assertThat(e, instanceOf(IllegalArgumentException.class));
            assertThat(e.getMessage(), startsWith("failed to build synonyms"));
        }
    }

    public void testSynonymsWrappedByMultiplexer() throws IOException {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put("path.home", createTempDir().toString())
            .put("index.analysis.filter.synonyms.type", "synonym")
            .putList("index.analysis.filter.synonyms.synonyms", "programmer, developer")
            .put("index.analysis.filter.my_english.type", "stemmer")
            .put("index.analysis.filter.my_english.language", "porter2")
            .put("index.analysis.filter.stem_repeat.type", "multiplexer")
            .putList("index.analysis.filter.stem_repeat.filters", "my_english, synonyms")
            .put("index.analysis.analyzer.synonymAnalyzer.tokenizer", "standard")
            .putList("index.analysis.analyzer.synonymAnalyzer.filter", "lowercase", "stem_repeat")
            .build();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", settings);
        indexAnalyzers = createTestAnalysis(idxSettings, settings, new CommonAnalysisPlugin()).indexAnalyzers;

        BaseTokenStreamTestCase.assertAnalyzesTo(indexAnalyzers.get("synonymAnalyzer"), "Some developers are odd",
            new String[]{ "some", "developers", "develop", "programm", "are", "odd" },
            new int[]{ 1, 1, 0, 0, 1, 1 });
    }

    public void testAsciiFoldingFilterForSynonyms() throws IOException {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put("path.home", createTempDir().toString())
            .put("index.analysis.filter.synonyms.type", "synonym")
            .putList("index.analysis.filter.synonyms.synonyms", "hoj, height")
            .put("index.analysis.analyzer.synonymAnalyzer.tokenizer", "standard")
            .putList("index.analysis.analyzer.synonymAnalyzer.filter", "lowercase", "asciifolding", "synonyms")
            .build();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", settings);
        indexAnalyzers = createTestAnalysis(idxSettings, settings, new CommonAnalysisPlugin()).indexAnalyzers;

        BaseTokenStreamTestCase.assertAnalyzesTo(indexAnalyzers.get("synonymAnalyzer"), "høj",
            new String[]{ "hoj", "height" },
            new int[]{ 1, 0 });
    }

    public void testPreconfigured() throws IOException {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put("path.home", createTempDir().toString())
            .put("index.analysis.filter.synonyms.type", "synonym")
            .putList("index.analysis.filter.synonyms.synonyms", "würst, sausage")
            .put("index.analysis.analyzer.my_analyzer.tokenizer", "standard")
            .putList("index.analysis.analyzer.my_analyzer.filter", "lowercase", "asciifolding", "synonyms")
            .build();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", settings);
        indexAnalyzers = createTestAnalysis(idxSettings, settings, new CommonAnalysisPlugin()).indexAnalyzers;

        BaseTokenStreamTestCase.assertAnalyzesTo(indexAnalyzers.get("my_analyzer"), "würst",
            new String[]{ "wurst", "sausage"},
            new int[]{ 1, 0 });
    }

    public void testChainedSynonymFilters() throws IOException {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put("path.home", createTempDir().toString())
            .put("index.analysis.filter.synonyms1.type", "synonym")
            .putList("index.analysis.filter.synonyms1.synonyms", "term1, term2")
            .put("index.analysis.filter.synonyms2.type", "synonym")
            .putList("index.analysis.filter.synonyms2.synonyms", "term1, term3")
            .put("index.analysis.analyzer.syn.tokenizer", "standard")
            .putList("index.analysis.analyzer.syn.filter", "lowercase", "synonyms1", "synonyms2")
            .build();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", settings);
        indexAnalyzers = createTestAnalysis(idxSettings, settings, new CommonAnalysisPlugin()).indexAnalyzers;

        BaseTokenStreamTestCase.assertAnalyzesTo(indexAnalyzers.get("syn"), "term1",
            new String[]{ "term1", "term3", "term2" }, new int[]{ 1, 0, 0 });
    }

    public void testShingleFilters() {

        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED,
                VersionUtils.randomVersionBetween(random(), LegacyESVersion.V_7_0_0, Version.CURRENT))
            .put("path.home", createTempDir().toString())
            .put("index.analysis.filter.synonyms.type", "synonym")
            .putList("index.analysis.filter.synonyms.synonyms", "programmer, developer")
            .put("index.analysis.filter.my_shingle.type", "shingle")
            .put("index.analysis.analyzer.my_analyzer.tokenizer", "standard")
            .putList("index.analysis.analyzer.my_analyzer.filter", "my_shingle", "synonyms")
            .build();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", settings);

        expectThrows(IllegalArgumentException.class, () -> {
            indexAnalyzers = createTestAnalysis(idxSettings, settings, new CommonAnalysisPlugin()).indexAnalyzers;
        });

    }

    public void testTokenFiltersBypassSynonymAnalysis() throws IOException {

        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put("path.home", createTempDir().toString())
            .putList("word_list", "a")
            .put("hyphenation_patterns_path", "foo")
            .build();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", settings);

        String[] bypassingFactories = new String[]{
            "dictionary_decompounder"
        };

        CommonAnalysisPlugin plugin = new CommonAnalysisPlugin();
        for (String factory : bypassingFactories) {
            TokenFilterFactory tff = plugin.getTokenFilters().get(factory).get(idxSettings, null, factory, settings);
            TokenizerFactory tok = new KeywordTokenizerFactory(idxSettings, null, "keyword", settings);
            SynonymTokenFilterFactory stff = new SynonymTokenFilterFactory(idxSettings, null, "synonym", settings);
            Analyzer analyzer = stff.buildSynonymAnalyzer(tok, Collections.emptyList(), Collections.singletonList(tff), null);

            try (TokenStream ts = analyzer.tokenStream("field", "text")) {
                assertThat(ts, instanceOf(KeywordTokenizer.class));
            }
        }

    }

    public void testPreconfiguredTokenFilters() throws IOException {
        Set<String> disallowedFilters = new HashSet<>(Arrays.asList(
            "common_grams", "edge_ngram", "edgeNGram", "keyword_repeat", "ngram", "nGram",
            "shingle", "word_delimiter", "word_delimiter_graph"
        ));

        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED,
                VersionUtils.randomVersionBetween(random(), LegacyESVersion.V_7_0_0, Version.CURRENT))
            .put("path.home", createTempDir().toString())
            .build();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", settings);

        CommonAnalysisPlugin plugin = new CommonAnalysisPlugin();

        for (PreConfiguredTokenFilter tf : plugin.getPreConfiguredTokenFilters()) {
            if (disallowedFilters.contains(tf.getName())) {
                IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                    "Expected exception for factory " + tf.getName(), () -> {
                        tf.get(idxSettings, null, tf.getName(), settings).getSynonymFilter();
                    });
                assertEquals(tf.getName(), "Token filter [" + tf.getName()
                        + "] cannot be used to parse synonyms",
                    e.getMessage());
            }
            else {
                tf.get(idxSettings, null, tf.getName(), settings).getSynonymFilter();
            }
        }

        Settings settings2 = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED,
                VersionUtils.randomVersionBetween(
                    random(), LegacyESVersion.V_6_0_0, VersionUtils.getPreviousVersion(LegacyESVersion.V_7_0_0)))
            .put("path.home", createTempDir().toString())
            .putList("common_words", "a", "b")
            .put("output_unigrams", "true")
            .build();
        IndexSettings idxSettings2 = IndexSettingsModule.newIndexSettings("index", settings2);

        List<String> expectedWarnings = new ArrayList<>();
        for (PreConfiguredTokenFilter tf : plugin.getPreConfiguredTokenFilters()) {
            if (disallowedFilters.contains(tf.getName())) {
                tf.get(idxSettings2, null, tf.getName(), settings2).getSynonymFilter();
                expectedWarnings.add("Token filter [" + tf.getName() + "] will not be usable to parse synonyms after v7.0");
            }
            else {
                tf.get(idxSettings2, null, tf.getName(), settings2).getSynonymFilter();
            }
        }
        assertWarnings(expectedWarnings.toArray(new String[0]));
    }

    public void testDisallowedTokenFilters() throws IOException {

        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED,
                VersionUtils.randomVersionBetween(random(), LegacyESVersion.V_7_0_0, Version.CURRENT))
            .put("path.home", createTempDir().toString())
            .putList("common_words", "a", "b")
            .put("output_unigrams", "true")
            .build();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", settings);
        CommonAnalysisPlugin plugin = new CommonAnalysisPlugin();

        String[] disallowedFactories = new String[]{
            "multiplexer", "cjk_bigram", "common_grams", "ngram", "edge_ngram",
            "word_delimiter", "word_delimiter_graph", "fingerprint"
        };

        for (String factory : disallowedFactories) {
            TokenFilterFactory tff = plugin.getTokenFilters().get(factory).get(idxSettings, null, factory, settings);
            TokenizerFactory tok = new KeywordTokenizerFactory(idxSettings, null, "keyword", settings);
            SynonymTokenFilterFactory stff = new SynonymTokenFilterFactory(idxSettings, null, "synonym", settings);

            IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                "Expected IllegalArgumentException for factory " + factory,
                () -> stff.buildSynonymAnalyzer(tok, Collections.emptyList(), Collections.singletonList(tff), null));

            assertEquals(factory, "Token filter [" + factory
                    + "] cannot be used to parse synonyms",
                e.getMessage());
        }

        settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED,
                VersionUtils.randomVersionBetween(
                    random(), LegacyESVersion.V_6_0_0, VersionUtils.getPreviousVersion(LegacyESVersion.V_7_0_0)))
            .put("path.home", createTempDir().toString())
            .putList("common_words", "a", "b")
            .put("output_unigrams", "true")
            .build();
        idxSettings = IndexSettingsModule.newIndexSettings("index", settings);

        List<String> expectedWarnings = new ArrayList<>();
        for (String factory : disallowedFactories) {
            TokenFilterFactory tff = plugin.getTokenFilters().get(factory).get(idxSettings, null, factory, settings);
            TokenizerFactory tok = new KeywordTokenizerFactory(idxSettings, null, "keyword", settings);
            SynonymTokenFilterFactory stff = new SynonymTokenFilterFactory(idxSettings, null, "synonym", settings);

            stff.buildSynonymAnalyzer(tok, Collections.emptyList(), Collections.singletonList(tff), null);
            expectedWarnings.add("Token filter [" + factory
                + "] will not be usable to parse synonyms after v7.0");
        }

        assertWarnings(expectedWarnings.toArray(new String[0]));

        settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED,
                VersionUtils.randomVersionBetween(
                    random(), LegacyESVersion.V_6_0_0, VersionUtils.getPreviousVersion(LegacyESVersion.V_7_0_0)))
            .put("path.home", createTempDir().toString())
            .put("preserve_original", "false")
            .build();
        idxSettings = IndexSettingsModule.newIndexSettings("index", settings);
        TokenFilterFactory tff = plugin.getTokenFilters().get("multiplexer").get(idxSettings, null, "multiplexer", settings);
        TokenizerFactory tok = new KeywordTokenizerFactory(idxSettings, null, "keyword", settings);
        SynonymTokenFilterFactory stff = new SynonymTokenFilterFactory(idxSettings, null, "synonym", settings);

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> stff.buildSynonymAnalyzer(tok, Collections.emptyList(), Collections.singletonList(tff), null));

        assertEquals("Token filter [multiplexer] cannot be used to parse synonyms unless [preserve_original] is [true]",
            e.getMessage());

    }

    private void match(String analyzerName, String source, String target) throws IOException {
        Analyzer analyzer = indexAnalyzers.get(analyzerName).analyzer();

        TokenStream stream = analyzer.tokenStream("", source);
        stream.reset();
        CharTermAttribute termAtt = stream.addAttribute(CharTermAttribute.class);

        StringBuilder sb = new StringBuilder();
        while (stream.incrementToken()) {
            sb.append(termAtt.toString()).append(" ");
        }

        MatcherAssert.assertThat(target, equalTo(sb.toString().trim()));
    }

}

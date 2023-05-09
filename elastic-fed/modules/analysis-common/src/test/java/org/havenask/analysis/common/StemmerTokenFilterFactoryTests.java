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

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.en.PorterStemFilter;
import org.apache.lucene.analysis.snowball.SnowballFilter;
import org.havenask.Version;
import org.havenask.common.settings.Settings;
import org.havenask.env.Environment;
import org.havenask.index.analysis.AnalysisTestsHelper;
import org.havenask.index.analysis.IndexAnalyzers;
import org.havenask.index.analysis.NamedAnalyzer;
import org.havenask.index.analysis.TokenFilterFactory;
import org.havenask.test.HavenaskTestCase;
import org.havenask.test.HavenaskTokenStreamTestCase;
import org.havenask.test.VersionUtils;

import java.io.IOException;
import java.io.StringReader;

import static com.carrotsearch.randomizedtesting.RandomizedTest.scaledRandomIntBetween;
import static org.havenask.cluster.metadata.IndexMetadata.SETTING_VERSION_CREATED;
import static org.hamcrest.Matchers.instanceOf;

public class StemmerTokenFilterFactoryTests extends HavenaskTokenStreamTestCase {

    private static final CommonAnalysisPlugin PLUGIN = new CommonAnalysisPlugin();

    public void testEnglishFilterFactory() throws IOException {
        int iters = scaledRandomIntBetween(20, 100);
        for (int i = 0; i < iters; i++) {
            Version v = VersionUtils.randomVersion(random());
            Settings settings = Settings.builder()
                    .put("index.analysis.filter.my_english.type", "stemmer")
                    .put("index.analysis.filter.my_english.language", "english")
                    .put("index.analysis.analyzer.my_english.tokenizer","whitespace")
                    .put("index.analysis.analyzer.my_english.filter","my_english")
                    .put(SETTING_VERSION_CREATED,v)
                    .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                    .build();

            HavenaskTestCase.TestAnalysis analysis = AnalysisTestsHelper.createTestAnalysisFromSettings(settings, PLUGIN);
            TokenFilterFactory tokenFilter = analysis.tokenFilter.get("my_english");
            assertThat(tokenFilter, instanceOf(StemmerTokenFilterFactory.class));
            Tokenizer tokenizer = new WhitespaceTokenizer();
            tokenizer.setReader(new StringReader("foo bar"));
            TokenStream create = tokenFilter.create(tokenizer);
            IndexAnalyzers indexAnalyzers = analysis.indexAnalyzers;
            NamedAnalyzer analyzer = indexAnalyzers.get("my_english");
            assertThat(create, instanceOf(PorterStemFilter.class));
            assertAnalyzesTo(analyzer, "consolingly", new String[]{"consolingli"});
        }
    }

    public void testPorter2FilterFactory() throws IOException {
        int iters = scaledRandomIntBetween(20, 100);
        for (int i = 0; i < iters; i++) {

            Version v = VersionUtils.randomVersion(random());
            Settings settings = Settings.builder()
                    .put("index.analysis.filter.my_porter2.type", "stemmer")
                    .put("index.analysis.filter.my_porter2.language", "porter2")
                    .put("index.analysis.analyzer.my_porter2.tokenizer","whitespace")
                    .put("index.analysis.analyzer.my_porter2.filter","my_porter2")
                    .put(SETTING_VERSION_CREATED,v)
                    .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                    .build();

            HavenaskTestCase.TestAnalysis analysis = AnalysisTestsHelper.createTestAnalysisFromSettings(settings, PLUGIN);
            TokenFilterFactory tokenFilter = analysis.tokenFilter.get("my_porter2");
            assertThat(tokenFilter, instanceOf(StemmerTokenFilterFactory.class));
            Tokenizer tokenizer = new WhitespaceTokenizer();
            tokenizer.setReader(new StringReader("foo bar"));
            TokenStream create = tokenFilter.create(tokenizer);
            IndexAnalyzers indexAnalyzers = analysis.indexAnalyzers;
            NamedAnalyzer analyzer = indexAnalyzers.get("my_porter2");
            assertThat(create, instanceOf(SnowballFilter.class));
            assertAnalyzesTo(analyzer, "possibly", new String[]{"possibl"});
        }
    }

    public void testMultipleLanguagesThrowsException() throws IOException {
        Version v = VersionUtils.randomVersion(random());
        Settings settings = Settings.builder().put("index.analysis.filter.my_english.type", "stemmer")
                .putList("index.analysis.filter.my_english.language", "english", "light_english").put(SETTING_VERSION_CREATED, v)
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build();

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> AnalysisTestsHelper.createTestAnalysisFromSettings(settings, PLUGIN));
        assertEquals("Invalid stemmer class specified: [english, light_english]", e.getMessage());
    }
}

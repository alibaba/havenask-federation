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
import org.havenask.common.settings.Settings;
import org.havenask.env.Environment;
import org.havenask.index.analysis.AnalysisTestsHelper;
import org.havenask.index.analysis.TokenFilterFactory;
import org.havenask.test.HavenaskTestCase;
import org.havenask.test.HavenaskTokenStreamTestCase;
import org.junit.Assert;

import java.io.IOException;
import java.io.StringReader;

import static org.hamcrest.Matchers.instanceOf;

public class KeepFilterFactoryTests extends HavenaskTokenStreamTestCase {
    private static final String RESOURCE = "/org/havenask/analysis/common/keep_analysis.json";

    public void testLoadWithoutSettings() throws IOException {
        HavenaskTestCase.TestAnalysis analysis = AnalysisTestsHelper.createTestAnalysisFromClassPath(
                createTempDir(), RESOURCE, new CommonAnalysisPlugin());
        TokenFilterFactory tokenFilter = analysis.tokenFilter.get("keep");
        Assert.assertNull(tokenFilter);
    }

    public void testLoadOverConfiguredSettings() {
        Settings settings = Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .put("index.analysis.filter.broken_keep_filter.type", "keep")
                .put("index.analysis.filter.broken_keep_filter.keep_words_path", "does/not/exists.txt")
                .put("index.analysis.filter.broken_keep_filter.keep_words", "[\"Hello\", \"worlD\"]")
                .build();
        try {
            AnalysisTestsHelper.createTestAnalysisFromSettings(settings, new CommonAnalysisPlugin());
            Assert.fail("path and array are configured");
        } catch (IllegalArgumentException e) {
        } catch (IOException e) {
            fail("expected IAE");
        }
    }

    public void testKeepWordsPathSettings() {
        Settings settings = Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .put("index.analysis.filter.non_broken_keep_filter.type", "keep")
                .put("index.analysis.filter.non_broken_keep_filter.keep_words_path", "does/not/exists.txt")
                .build();
        try {
            // test our none existing setup is picked up
            AnalysisTestsHelper.createTestAnalysisFromSettings(settings, new CommonAnalysisPlugin());
            fail("expected an exception due to non existent keep_words_path");
        } catch (IllegalArgumentException e) {
        } catch (IOException e) {
            fail("expected IAE");
        }

        settings = Settings.builder().put(settings)
                .putList("index.analysis.filter.non_broken_keep_filter.keep_words", "test")
                .build();
        try {
            // test our none existing setup is picked up
            AnalysisTestsHelper.createTestAnalysisFromSettings(settings, new CommonAnalysisPlugin());
            fail("expected an exception indicating that you can't use [keep_words_path] with [keep_words] ");
        } catch (IllegalArgumentException e) {
        } catch (IOException e) {
            fail("expected IAE");
        }

    }

    public void testCaseInsensitiveMapping() throws IOException {
        HavenaskTestCase.TestAnalysis analysis = AnalysisTestsHelper.createTestAnalysisFromClassPath(
                createTempDir(), RESOURCE, new CommonAnalysisPlugin());
        TokenFilterFactory tokenFilter = analysis.tokenFilter.get("my_keep_filter");
        assertThat(tokenFilter, instanceOf(KeepWordFilterFactory.class));
        String source = "hello small world";
        String[] expected = new String[]{"hello", "world"};
        Tokenizer tokenizer = new WhitespaceTokenizer();
        tokenizer.setReader(new StringReader(source));
        assertTokenStreamContents(tokenFilter.create(tokenizer), expected, new int[]{1, 2});
    }

    public void testCaseSensitiveMapping() throws IOException {
        HavenaskTestCase.TestAnalysis analysis = AnalysisTestsHelper.createTestAnalysisFromClassPath(
                createTempDir(), RESOURCE, new CommonAnalysisPlugin());
        TokenFilterFactory tokenFilter = analysis.tokenFilter.get("my_case_sensitive_keep_filter");
        assertThat(tokenFilter, instanceOf(KeepWordFilterFactory.class));
        String source = "Hello small world";
        String[] expected = new String[]{"Hello"};
        Tokenizer tokenizer = new WhitespaceTokenizer();
        tokenizer.setReader(new StringReader(source));
        assertTokenStreamContents(tokenFilter.create(tokenizer), expected, new int[]{1});
    }
}

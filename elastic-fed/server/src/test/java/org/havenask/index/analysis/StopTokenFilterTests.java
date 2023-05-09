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

package org.havenask.index.analysis;

import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.search.suggest.analyzing.SuggestStopFilter;
import org.apache.lucene.util.Version;
import org.havenask.common.settings.Settings.Builder;
import org.havenask.common.settings.Settings;
import org.havenask.env.Environment;
import org.havenask.test.HavenaskTestCase;
import org.havenask.test.HavenaskTokenStreamTestCase;
import org.havenask.index.analysis.AnalysisTestsHelper;

import java.io.IOException;
import java.io.StringReader;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;


public class StopTokenFilterTests extends HavenaskTokenStreamTestCase {
    public void testPositionIncrementSetting() throws IOException {
        Builder builder = Settings.builder().put("index.analysis.filter.my_stop.type", "stop")
                .put("index.analysis.filter.my_stop.enable_position_increments", false);
        if (random().nextBoolean()) {
            builder.put("index.analysis.filter.my_stop.version", "5.0");
        }
        builder.put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString());
        Settings settings = builder.build();
        try {
            AnalysisTestsHelper.createTestAnalysisFromSettings(settings);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("enable_position_increments is not supported anymore"));
        }
    }

    public void testCorrectPositionIncrementSetting() throws IOException {
        Builder builder = Settings.builder().put("index.analysis.filter.my_stop.type", "stop");
        if (random().nextBoolean()) {
            builder.put("index.analysis.filter.my_stop.version", Version.LATEST);
        } else {
            // don't specify
        }
        builder.put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString());
        HavenaskTestCase.TestAnalysis analysis = AnalysisTestsHelper.createTestAnalysisFromSettings(builder.build());
        TokenFilterFactory tokenFilter = analysis.tokenFilter.get("my_stop");
        assertThat(tokenFilter, instanceOf(StopTokenFilterFactory.class));
        Tokenizer tokenizer = new WhitespaceTokenizer();
        tokenizer.setReader(new StringReader("foo bar"));
        TokenStream create = tokenFilter.create(tokenizer);
        assertThat(create, instanceOf(StopFilter.class));
    }

    public void testThatSuggestStopFilterWorks() throws Exception {
        Settings settings = Settings.builder()
                .put("index.analysis.filter.my_stop.type", "stop")
                .put("index.analysis.filter.my_stop.remove_trailing", false)
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .build();
        HavenaskTestCase.TestAnalysis analysis = AnalysisTestsHelper.createTestAnalysisFromSettings(settings);
        TokenFilterFactory tokenFilter = analysis.tokenFilter.get("my_stop");
        assertThat(tokenFilter, instanceOf(StopTokenFilterFactory.class));
        Tokenizer tokenizer = new WhitespaceTokenizer();
        tokenizer.setReader(new StringReader("foo an"));
        TokenStream create = tokenFilter.create(tokenizer);
        assertThat(create, instanceOf(SuggestStopFilter.class));
    }
}

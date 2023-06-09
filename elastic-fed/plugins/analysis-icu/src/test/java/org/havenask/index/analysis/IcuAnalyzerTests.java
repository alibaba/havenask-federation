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
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.havenask.Version;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.common.settings.Settings;
import org.havenask.index.IndexSettings;
import org.havenask.plugin.analysis.icu.AnalysisICUPlugin;
import org.havenask.test.IndexSettingsModule;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;

public class IcuAnalyzerTests extends BaseTokenStreamTestCase {

    public void testMixedAlphabetTokenization() throws IOException {

        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .build();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", settings);

        String input = "안녕은하철도999극장판2.1981년8월8일.일본개봉작1999년재더빙video판";

        AnalysisICUPlugin plugin = new AnalysisICUPlugin();
        Analyzer analyzer = plugin.getAnalyzers().get("icu_analyzer").get(idxSettings, null, "icu", settings).get();
        assertAnalyzesTo(analyzer, input,
            new String[]{"안녕은하철도", "999", "극장판", "2.1981", "년", "8", "월", "8", "일", "일본개봉작", "1999", "년재더빙", "video", "판"});

    }

    public void testMiddleDots() throws IOException {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .build();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", settings);

        String input = "경승지·산악·협곡·해협·곶·심연·폭포·호수·급류";

        Analyzer analyzer = new IcuAnalyzerProvider(idxSettings, null, "icu", settings).get();
        assertAnalyzesTo(analyzer, input,
            new String[]{"경승지", "산악", "협곡", "해협", "곶", "심연", "폭포", "호수", "급류"});
    }

    public void testUnicodeNumericCharacters() throws IOException {

        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .build();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", settings);

        String input = "① ② ③ ⑴ ⑵ ⑶ ¼ ⅓ ⅜ ¹ ² ³ ₁ ₂ ₃";

        Analyzer analyzer = new IcuAnalyzerProvider(idxSettings, null, "icu", settings).get();
        assertAnalyzesTo(analyzer, input,
            new String[]{"1", "2", "3", "1", "2", "3", "1/4", "1/3", "3/8", "1", "2", "3", "1", "2", "3"});
    }

    public void testBadSettings() {

        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put("mode", "wrong")
            .build();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", settings);

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
            new IcuAnalyzerProvider(idxSettings, null, "icu", settings);
        });

        assertThat(e.getMessage(), containsString("Unknown mode [wrong] in analyzer [icu], expected one of [compose, decompose]"));

    }

}

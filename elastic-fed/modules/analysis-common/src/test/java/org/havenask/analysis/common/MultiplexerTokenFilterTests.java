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

import org.havenask.Version;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.common.settings.Settings;
import org.havenask.env.Environment;
import org.havenask.env.TestEnvironment;
import org.havenask.index.IndexSettings;
import org.havenask.index.analysis.IndexAnalyzers;
import org.havenask.index.analysis.NamedAnalyzer;
import org.havenask.indices.analysis.AnalysisModule;
import org.havenask.test.HavenaskTokenStreamTestCase;
import org.havenask.test.IndexSettingsModule;

import java.io.IOException;
import java.util.Collections;

public class MultiplexerTokenFilterTests extends HavenaskTokenStreamTestCase {

    public void testMultiplexingFilter() throws IOException {
        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .build();
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put("index.analysis.filter.t.type", "truncate")
            .put("index.analysis.filter.t.length", "2")
            .put("index.analysis.filter.multiplexFilter.type", "multiplexer")
            .putList("index.analysis.filter.multiplexFilter.filters", "lowercase, t", "uppercase")
            .put("index.analysis.analyzer.myAnalyzer.type", "custom")
            .put("index.analysis.analyzer.myAnalyzer.tokenizer", "standard")
            .putList("index.analysis.analyzer.myAnalyzer.filter", "multiplexFilter")
            .build();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", indexSettings);

        IndexAnalyzers indexAnalyzers = new AnalysisModule(TestEnvironment.newEnvironment(settings),
            Collections.singletonList(new CommonAnalysisPlugin())).getAnalysisRegistry().build(idxSettings);

        try (NamedAnalyzer analyzer = indexAnalyzers.get("myAnalyzer")) {
            assertNotNull(analyzer);
            assertAnalyzesTo(analyzer, "ONe tHree", new String[]{
                "ONe", "on", "ONE", "tHree", "th", "THREE"
            }, new int[]{
                1,      0,      0,      1,      0,      0
            });
            // Duplicates are removed
            assertAnalyzesTo(analyzer, "ONe THREE", new String[]{
                "ONe", "on", "ONE", "THREE", "th"
            }, new int[]{
                1,      0,      0,      1,      0,      0
            });
        }
    }

    public void testMultiplexingNoOriginal() throws IOException {

        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .build();
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put("index.analysis.filter.t.type", "truncate")
            .put("index.analysis.filter.t.length", "2")
            .put("index.analysis.filter.multiplexFilter.type", "multiplexer")
            .put("index.analysis.filter.multiplexFilter.preserve_original", "false")
            .putList("index.analysis.filter.multiplexFilter.filters", "lowercase, t", "uppercase")
            .put("index.analysis.analyzer.myAnalyzer.type", "custom")
            .put("index.analysis.analyzer.myAnalyzer.tokenizer", "standard")
            .putList("index.analysis.analyzer.myAnalyzer.filter", "multiplexFilter")
            .build();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", indexSettings);

        IndexAnalyzers indexAnalyzers = new AnalysisModule(TestEnvironment.newEnvironment(settings),
            Collections.singletonList(new CommonAnalysisPlugin())).getAnalysisRegistry().build(idxSettings);

        try (NamedAnalyzer analyzer = indexAnalyzers.get("myAnalyzer")) {
            assertNotNull(analyzer);
            assertAnalyzesTo(analyzer, "ONe tHree", new String[]{
                "on", "ONE", "th", "THREE"
            }, new int[]{
                1,      0,      1,      0,
            });
        }

    }

}

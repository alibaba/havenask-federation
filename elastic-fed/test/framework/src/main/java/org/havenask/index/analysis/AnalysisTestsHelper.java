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

import org.havenask.Version;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.common.settings.Settings;
import org.havenask.env.Environment;
import org.havenask.index.IndexSettings;
import org.havenask.indices.analysis.AnalysisModule;
import org.havenask.plugins.AnalysisPlugin;
import org.havenask.test.HavenaskTestCase;
import org.havenask.test.IndexSettingsModule;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;

public class AnalysisTestsHelper {

    public static HavenaskTestCase.TestAnalysis createTestAnalysisFromClassPath(final Path baseDir,
                                                                                  final String resource,
                                                                                  final AnalysisPlugin... plugins) throws IOException {
        final Settings settings = Settings.builder()
                .loadFromStream(resource, AnalysisTestsHelper.class.getResourceAsStream(resource), false)
                .put(Environment.PATH_HOME_SETTING.getKey(), baseDir.toString())
                .build();

        return createTestAnalysisFromSettings(settings, plugins);
    }

    public static HavenaskTestCase.TestAnalysis createTestAnalysisFromSettings(
            final Settings settings, final AnalysisPlugin... plugins) throws IOException {
        return createTestAnalysisFromSettings(settings, null, plugins);
    }

    public static HavenaskTestCase.TestAnalysis createTestAnalysisFromSettings(
            final Settings settings,
            final Path configPath,
            final AnalysisPlugin... plugins) throws IOException {
        final Settings actualSettings;
        if (settings.get(IndexMetadata.SETTING_VERSION_CREATED) == null) {
            actualSettings = Settings.builder().put(settings).put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build();
        } else {
            actualSettings = settings;
        }
        final IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test", actualSettings);
        final AnalysisRegistry analysisRegistry =
                new AnalysisModule(new Environment(actualSettings, configPath), Arrays.asList(plugins)).getAnalysisRegistry();
        return new HavenaskTestCase.TestAnalysis(analysisRegistry.build(indexSettings),
                analysisRegistry.buildTokenFilterFactories(indexSettings),
                analysisRegistry.buildTokenizerFactories(indexSettings),
                analysisRegistry.buildCharFilterFactories(indexSettings));
    }

}

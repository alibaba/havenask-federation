/*
 * Copyright (c) 2021, Alibaba Group;
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.havenask.engine.index.analysis;

import org.havenask.common.settings.Settings;
import org.havenask.env.Environment;
import org.havenask.index.IndexSettings;
import org.havenask.index.analysis.AbstractIndexAnalyzerProvider;

public class HavenaskJiebaAnalyzerProvider extends AbstractIndexAnalyzerProvider<HavenaskJiebaAnalyzer> {
    public static final String NAME = "jieba_analyzer";
    private HavenaskJiebaAnalyzer analyzer;

    public HavenaskJiebaAnalyzerProvider(IndexSettings indexSettings, Environment env, String name, Settings settings) {
        super(indexSettings, name, settings);
        this.analyzer = new HavenaskJiebaAnalyzer();
    }

    @Override
    public HavenaskJiebaAnalyzer get() {
        return this.analyzer;
    }
}

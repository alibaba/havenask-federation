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

import org.havenask.common.settings.Settings;
import org.havenask.index.Index;
import org.havenask.plugin.analysis.icu.AnalysisICUPlugin;
import org.havenask.test.HavenaskTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.instanceOf;

public class SimpleIcuAnalysisTests extends HavenaskTestCase {
    public void testDefaultsIcuAnalysis() throws IOException {
        TestAnalysis analysis = createTestAnalysis(new Index("test", "_na_"), Settings.EMPTY, new AnalysisICUPlugin());

        TokenizerFactory tokenizerFactory = analysis.tokenizer.get("icu_tokenizer");
        assertThat(tokenizerFactory, instanceOf(IcuTokenizerFactory.class));

        TokenFilterFactory filterFactory = analysis.tokenFilter.get("icu_normalizer");
        assertThat(filterFactory, instanceOf(IcuNormalizerTokenFilterFactory.class));

        filterFactory = analysis.tokenFilter.get("icu_folding");
        assertThat(filterFactory, instanceOf(IcuFoldingTokenFilterFactory.class));

        filterFactory = analysis.tokenFilter.get("icu_collation");
        assertThat(filterFactory, instanceOf(IcuCollationTokenFilterFactory.class));

        filterFactory = analysis.tokenFilter.get("icu_transform");
        assertThat(filterFactory, instanceOf(IcuTransformTokenFilterFactory.class));

        CharFilterFactory charFilterFactory = analysis.charFilter.get("icu_normalizer");
        assertThat(charFilterFactory, instanceOf(IcuNormalizerCharFilterFactory.class));
    }
}

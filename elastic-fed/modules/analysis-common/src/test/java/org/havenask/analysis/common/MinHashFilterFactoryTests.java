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

import java.io.IOException;
import java.io.StringReader;

public class MinHashFilterFactoryTests extends HavenaskTokenStreamTestCase {
    public void testDefault() throws IOException {
        int default_hash_count = 1;
        int default_bucket_size = 512;
        int default_hash_set_size = 1;
        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .build();
        HavenaskTestCase.TestAnalysis analysis = AnalysisTestsHelper.createTestAnalysisFromSettings(settings, new CommonAnalysisPlugin());
        TokenFilterFactory tokenFilter = analysis.tokenFilter.get("min_hash");
        String source = "the quick brown fox";
        Tokenizer tokenizer = new WhitespaceTokenizer();
        tokenizer.setReader(new StringReader(source));

        // with_rotation is true by default, and hash_set_size is 1, so even though the source doesn't
        // have enough tokens to fill all the buckets, we still expect 512 tokens.
        assertStreamHasNumberOfTokens(tokenFilter.create(tokenizer),
            default_hash_count * default_bucket_size * default_hash_set_size);
    }

    public void testSettings() throws IOException {
        Settings settings = Settings.builder()
            .put("index.analysis.filter.test_min_hash.type", "min_hash")
            .put("index.analysis.filter.test_min_hash.hash_count", "1")
            .put("index.analysis.filter.test_min_hash.bucket_count", "2")
            .put("index.analysis.filter.test_min_hash.hash_set_size", "1")
            .put("index.analysis.filter.test_min_hash.with_rotation", false)
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .build();
        HavenaskTestCase.TestAnalysis analysis = AnalysisTestsHelper.createTestAnalysisFromSettings(settings, new CommonAnalysisPlugin());
        TokenFilterFactory tokenFilter = analysis.tokenFilter.get("test_min_hash");
        String source = "sushi";
        Tokenizer tokenizer = new WhitespaceTokenizer();
        tokenizer.setReader(new StringReader(source));

        // despite the fact that bucket_count is 2 and hash_set_size is 1,
        // because with_rotation is false, we only expect 1 token here.
        assertStreamHasNumberOfTokens(tokenFilter.create(tokenizer), 1);
    }
}

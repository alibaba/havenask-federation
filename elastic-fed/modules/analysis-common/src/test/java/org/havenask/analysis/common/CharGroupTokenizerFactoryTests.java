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

import com.carrotsearch.randomizedtesting.generators.RandomStrings;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.util.CharTokenizer;
import org.havenask.common.settings.Settings;
import org.havenask.index.Index;
import org.havenask.index.IndexSettings;
import org.havenask.test.HavenaskTokenStreamTestCase;
import org.havenask.test.IndexSettingsModule;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Arrays;

public class CharGroupTokenizerFactoryTests extends HavenaskTokenStreamTestCase {

    public void testParseTokenChars() {
        final Index index = new Index("test", "_na_");
        final Settings indexSettings = newAnalysisSettingsBuilder().build();
        IndexSettings indexProperties = IndexSettingsModule.newIndexSettings(index, indexSettings);
        final String name = "cg";
        for (String[] conf : Arrays.asList(
                new String[] { "\\v" },
                new String[] { "\\u00245" },
                new String[] { "commas" },
                new String[] { "a", "b", "c", "\\$" })) {
            final Settings settings = newAnalysisSettingsBuilder().putList("tokenize_on_chars", conf).build();
            expectThrows(RuntimeException.class, () -> new CharGroupTokenizerFactory(indexProperties, null, name, settings).create());
        }

        for (String[] conf : Arrays.asList(
                new String[0],
                new String[] { "\\n" },
                new String[] { "\\u0024" },
                new String[] { "whitespace" },
                new String[] { "a", "b", "c" },
                new String[] { "a", "b", "c", "\\r" },
                new String[] { "\\r" },
                new String[] { "f", "o", "o", "symbol" })) {
            final Settings settings = newAnalysisSettingsBuilder().putList("tokenize_on_chars", Arrays.asList(conf)).build();
            new CharGroupTokenizerFactory(indexProperties, null, name, settings).create();
            // no exception
        }
    }

    public void testMaxTokenLength() throws IOException {
        final Index index = new Index("test", "_na_");
        final Settings indexSettings = newAnalysisSettingsBuilder().build();
        IndexSettings indexProperties = IndexSettingsModule.newIndexSettings(index, indexSettings);
        final String name = "cg";

        String[] conf = new String[] {"-"};

        final Settings defaultLengthSettings = newAnalysisSettingsBuilder()
            .putList("tokenize_on_chars", conf)
            .build();
        CharTokenizer tokenizer = (CharTokenizer) new CharGroupTokenizerFactory(indexProperties, null, name, defaultLengthSettings)
            .create();
        String textWithVeryLongToken = RandomStrings.randomAsciiAlphanumOfLength(random(), 256).concat("-trailing");
        try (Reader reader = new StringReader(textWithVeryLongToken)) {
            tokenizer.setReader(reader);
            assertTokenStreamContents(tokenizer, new String[] { textWithVeryLongToken.substring(0, 255),
                textWithVeryLongToken.substring(255, 256), "trailing"});
        }

        final Settings analysisSettings = newAnalysisSettingsBuilder()
            .putList("tokenize_on_chars", conf)
            .put("max_token_length", 2)
            .build();
        tokenizer = (CharTokenizer) new CharGroupTokenizerFactory(indexProperties, null, name, analysisSettings).create();
        try (Reader reader = new StringReader("one-two-three")) {
            tokenizer.setReader(reader);
            assertTokenStreamContents(tokenizer, new String[] { "on", "e", "tw", "o", "th", "re", "e" });
        }

        final Settings tooLongLengthSettings = newAnalysisSettingsBuilder()
            .putList("tokenize_on_chars", conf)
            .put("max_token_length", 1024 * 1024 + 1)
            .build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> new CharGroupTokenizerFactory(indexProperties, null, name, tooLongLengthSettings).create());
        assertEquals("maxTokenLen must be greater than 0 and less than 1048576 passed: 1048577", e.getMessage());

        final Settings negativeLengthSettings = newAnalysisSettingsBuilder()
            .putList("tokenize_on_chars", conf)
            .put("max_token_length", -1)
            .build();
        e = expectThrows(IllegalArgumentException.class,
            () -> new CharGroupTokenizerFactory(indexProperties, null, name, negativeLengthSettings).create());
        assertEquals("maxTokenLen must be greater than 0 and less than 1048576 passed: -1", e.getMessage());
    }

    public void testTokenization() throws IOException {
        final Index index = new Index("test", "_na_");
        final String name = "cg";
        final Settings indexSettings = newAnalysisSettingsBuilder().build();
        final Settings settings = newAnalysisSettingsBuilder().putList("tokenize_on_chars", "whitespace", ":", "\\u0024").build();
        Tokenizer tokenizer = new CharGroupTokenizerFactory(IndexSettingsModule.newIndexSettings(index, indexSettings),
                null, name, settings).create();
        tokenizer.setReader(new StringReader("foo bar $34 test:test2"));
        assertTokenStreamContents(tokenizer, new String[] {"foo", "bar", "34", "test", "test2"});
    }
}

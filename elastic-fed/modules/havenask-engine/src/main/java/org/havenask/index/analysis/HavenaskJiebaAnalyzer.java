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

package org.havenask.index.analysis;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordTokenizer;

/**
 * The Jieba analyzer is an integrated analyzer within Havenask,
 * we define this tokenizer as a plugin solely for the purpose of passing the analyzer verification during index creation.
 * Consequently, within this analyzer, we have encapsulated only a keyword tokenizer
 * and have refrained from performing any additional operations.
 */
public class HavenaskJiebaAnalyzer extends Analyzer {
    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
        return new TokenStreamComponents(new KeywordTokenizer());
    }
}

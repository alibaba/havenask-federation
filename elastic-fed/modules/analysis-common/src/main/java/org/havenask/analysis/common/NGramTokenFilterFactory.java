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

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.ngram.NGramTokenFilter;
import org.havenask.LegacyESVersion;
import org.havenask.common.logging.DeprecationLogger;
import org.havenask.common.settings.Settings;
import org.havenask.env.Environment;
import org.havenask.index.IndexSettings;
import org.havenask.index.analysis.AbstractTokenFilterFactory;
import org.havenask.index.analysis.TokenFilterFactory;


public class NGramTokenFilterFactory extends AbstractTokenFilterFactory {

    private static final DeprecationLogger DEPRECATION_LOGGER =  DeprecationLogger.getLogger(NGramTokenFilterFactory.class);

    private final int minGram;
    private final int maxGram;
    private final boolean preserveOriginal;
    private static final String PRESERVE_ORIG_KEY = "preserve_original";

    NGramTokenFilterFactory(IndexSettings indexSettings, Environment environment, String name, Settings settings) {
        super(indexSettings, name, settings);
        int maxAllowedNgramDiff = indexSettings.getMaxNgramDiff();
        this.minGram = settings.getAsInt("min_gram", 1);
        this.maxGram = settings.getAsInt("max_gram", 2);
        int ngramDiff = maxGram - minGram;
        if (ngramDiff > maxAllowedNgramDiff) {
            if (indexSettings.getIndexVersionCreated().onOrAfter(LegacyESVersion.V_7_0_0)) {
                throw new IllegalArgumentException(
                    "The difference between max_gram and min_gram in NGram Tokenizer must be less than or equal to: ["
                        + maxAllowedNgramDiff + "] but was [" + ngramDiff + "]. This limit can be set by changing the ["
                        + IndexSettings.MAX_NGRAM_DIFF_SETTING.getKey() + "] index level setting.");
            } else {
                deprecationLogger.deprecate("ngram_big_difference",
                    "Deprecated big difference between max_gram and min_gram in NGram Tokenizer,"
                    + "expected difference must be less than or equal to: [" + maxAllowedNgramDiff + "]");
            }
        }
        preserveOriginal = settings.getAsBoolean(PRESERVE_ORIG_KEY, false);
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        return new NGramTokenFilter(tokenStream, minGram, maxGram, preserveOriginal);
    }

    @Override
    public TokenFilterFactory getSynonymFilter() {
        if (indexSettings.getIndexVersionCreated().onOrAfter(LegacyESVersion.V_7_0_0)) {
            throw new IllegalArgumentException("Token filter [" + name() + "] cannot be used to parse synonyms");
        }
        else {
            DEPRECATION_LOGGER.deprecate("synonym_tokenfilters", "Token filter [" + name()
                + "] will not be usable to parse synonyms after v7.0");
            return this;
        }
    }
}

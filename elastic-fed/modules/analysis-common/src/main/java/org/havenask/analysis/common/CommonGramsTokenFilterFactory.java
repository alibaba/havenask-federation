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

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.commongrams.CommonGramsFilter;
import org.apache.lucene.analysis.commongrams.CommonGramsQueryFilter;
import org.havenask.LegacyESVersion;
import org.havenask.common.logging.DeprecationLogger;
import org.havenask.common.settings.Settings;
import org.havenask.env.Environment;
import org.havenask.index.IndexSettings;
import org.havenask.index.analysis.AbstractTokenFilterFactory;
import org.havenask.index.analysis.Analysis;
import org.havenask.index.analysis.TokenFilterFactory;

public class CommonGramsTokenFilterFactory extends AbstractTokenFilterFactory {

    private static final DeprecationLogger DEPRECATION_LOGGER =  DeprecationLogger.getLogger(CommonGramsTokenFilterFactory.class);

    private final CharArraySet words;

    private final boolean ignoreCase;

    private final boolean queryMode;

    CommonGramsTokenFilterFactory(IndexSettings indexSettings, Environment env, String name, Settings settings) {
        super(indexSettings, name, settings);
        this.ignoreCase = settings.getAsBoolean("ignore_case", false);
        this.queryMode = settings.getAsBoolean("query_mode", false);
        this.words = Analysis.parseCommonWords(env, settings, null, ignoreCase);

        if (this.words == null) {
            throw new IllegalArgumentException(
                    "missing or empty [common_words] or [common_words_path] configuration for common_grams token filter");
        }
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        CommonGramsFilter filter = new CommonGramsFilter(tokenStream, words);
        if (queryMode) {
            return new CommonGramsQueryFilter(filter);
        } else {
            return filter;
        }
    }

    @Override
    public TokenFilterFactory getSynonymFilter() {
        if (indexSettings.getIndexVersionCreated().onOrAfter(LegacyESVersion.V_7_0_0)) {
            throw new IllegalArgumentException("Token filter [" + name() + "] cannot be used to parse synonyms");
        } else {
            DEPRECATION_LOGGER.deprecate("synonym_tokenfilters", "Token filter [" + name()
                + "] will not be usable to parse synonyms after v7.0");
        }

        return this;
    }
}


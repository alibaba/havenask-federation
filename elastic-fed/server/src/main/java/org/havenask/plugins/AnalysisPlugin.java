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

package org.havenask.plugins;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharFilter;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.Tokenizer;
import org.havenask.common.settings.Settings;
import org.havenask.env.Environment;
import org.havenask.index.IndexSettings;
import org.havenask.index.analysis.AnalyzerProvider;
import org.havenask.index.analysis.CharFilterFactory;
import org.havenask.index.analysis.PreBuiltAnalyzerProviderFactory;
import org.havenask.index.analysis.PreConfiguredCharFilter;
import org.havenask.index.analysis.PreConfiguredTokenFilter;
import org.havenask.index.analysis.PreConfiguredTokenizer;
import org.havenask.index.analysis.TokenFilterFactory;
import org.havenask.index.analysis.TokenizerFactory;
import org.havenask.indices.analysis.AnalysisModule.AnalysisProvider;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

/**
 * An additional extension point for {@link Plugin}s that extends Havenask's analysis functionality. To add an additional
 * {@link TokenFilter} just implement the interface and implement the {@link #getTokenFilters()} method:
 *
 * <pre>
 * public class AnalysisPhoneticPlugin extends Plugin implements AnalysisPlugin {
 *     &#64;Override
 *     public Map&#60;String, AnalysisProvider&#60;TokenFilterFactory&#62;&#62; getTokenFilters() {
 *         return singletonMap("phonetic", PhoneticTokenFilterFactory::new);
 *     }
 * }
 * </pre>
 *
 * Havenask doesn't have any automatic mechanism to share these components between indexes. If any component is heavy enough to warrant
 * such sharing then it is the Plugin's responsibility to do it in their {@link AnalysisProvider} implementation. We recommend against doing
 * this unless absolutely necessary because it can be difficult to get the caching right given things like behavior changes across versions.
 */
public interface AnalysisPlugin {
    /**
     * Override to add additional {@link CharFilter}s. See {@link #requiresAnalysisSettings(AnalysisProvider)}
     * how to on get the configuration from the index.
     */
    default Map<String, AnalysisProvider<CharFilterFactory>> getCharFilters() {
        return emptyMap();
    }

    /**
     * Override to add additional {@link TokenFilter}s. See {@link #requiresAnalysisSettings(AnalysisProvider)}
     * how to on get the configuration from the index.
     */
    default Map<String, AnalysisProvider<TokenFilterFactory>> getTokenFilters() {
        return emptyMap();
    }

    /**
     * Override to add additional {@link Tokenizer}s. See {@link #requiresAnalysisSettings(AnalysisProvider)}
     * how to on get the configuration from the index.
     */
    default Map<String, AnalysisProvider<TokenizerFactory>> getTokenizers() {
        return emptyMap();
    }

    /**
     * Override to add additional {@link Analyzer}s. See {@link #requiresAnalysisSettings(AnalysisProvider)}
     * how to on get the configuration from the index.
     */
    default Map<String, AnalysisProvider<AnalyzerProvider<? extends Analyzer>>> getAnalyzers() {
        return emptyMap();
    }

    /**
     * Override to add additional pre-configured {@link Analyzer}s.
     */
    default List<PreBuiltAnalyzerProviderFactory> getPreBuiltAnalyzerProviderFactories() {
        return emptyList();
    }

    /**
     * Override to add additional pre-configured {@link CharFilter}s.
     */
    default List<PreConfiguredCharFilter> getPreConfiguredCharFilters() {
        return emptyList();
    }

    /**
     * Override to add additional pre-configured {@link TokenFilter}s.
     */
    default List<PreConfiguredTokenFilter> getPreConfiguredTokenFilters() {
        return emptyList();
    }

    /**
     * Override to add additional pre-configured {@link Tokenizer}.
     */
    default List<PreConfiguredTokenizer> getPreConfiguredTokenizers() {
        return emptyList();
    }

    /**
     * Override to add additional hunspell {@link org.apache.lucene.analysis.hunspell.Dictionary}s.
     */
    default Map<String, org.apache.lucene.analysis.hunspell.Dictionary> getHunspellDictionaries() {
        return emptyMap();
    }

    /**
     * Mark an {@link AnalysisProvider} as requiring the index's settings.
     */
    static <T> AnalysisProvider<T> requiresAnalysisSettings(AnalysisProvider<T> provider) {
        return new AnalysisProvider<T>() {
            @Override
            public T get(IndexSettings indexSettings, Environment environment, String name, Settings settings) throws IOException {
                return provider.get(indexSettings, environment, name, settings);
            }

            @Override
            public boolean requiresAnalysisSettings() {
                return true;
            }
        };
    }
}

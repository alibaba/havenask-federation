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

import org.apache.lucene.analysis.CharFilter;
import org.apache.lucene.analysis.TokenFilter;
import org.havenask.Version;
import org.havenask.indices.analysis.PreBuiltCacheFactory.CachingStrategy;

import java.io.Reader;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Provides pre-configured, shared {@link CharFilter}s.
 */
public class PreConfiguredCharFilter extends PreConfiguredAnalysisComponent<CharFilterFactory> {
    /**
     * Create a pre-configured char filter that may not vary at all.
     */
    public static PreConfiguredCharFilter singleton(String name, boolean useFilterForMultitermQueries, Function<Reader, Reader> create) {
        return new PreConfiguredCharFilter(name, CachingStrategy.ONE, useFilterForMultitermQueries,
                (reader, version) -> create.apply(reader));
    }

    /**
     * Create a pre-configured char filter that may not vary at all, provide access to the openearch version
     */
    public static PreConfiguredCharFilter singletonWithVersion(String name, boolean useFilterForMultitermQueries,
            BiFunction<Reader, org.havenask.Version, Reader> create) {
        return new PreConfiguredCharFilter(name, CachingStrategy.ONE, useFilterForMultitermQueries,
                (reader, version) -> create.apply(reader, version));
    }

    /**
     * Create a pre-configured token filter that may vary based on the Lucene version.
     */
    public static PreConfiguredCharFilter luceneVersion(String name, boolean useFilterForMultitermQueries,
            BiFunction<Reader, org.apache.lucene.util.Version, Reader> create) {
        return new PreConfiguredCharFilter(name, CachingStrategy.LUCENE, useFilterForMultitermQueries,
                (reader, version) -> create.apply(reader, version.luceneVersion));
    }

    /**
     * Create a pre-configured token filter that may vary based on the Havenask version.
     */
    public static PreConfiguredCharFilter havenaskVersion(String name, boolean useFilterForMultitermQueries,
            BiFunction<Reader, org.havenask.Version, Reader> create) {
        return new PreConfiguredCharFilter(name, CachingStrategy.HAVENASK, useFilterForMultitermQueries, create);
    }

    private final boolean useFilterForMultitermQueries;
    private final BiFunction<Reader, Version, Reader> create;

    protected PreConfiguredCharFilter(String name, CachingStrategy cache, boolean useFilterForMultitermQueries,
            BiFunction<Reader, org.havenask.Version, Reader> create) {
        super(name, cache);
        this.useFilterForMultitermQueries = useFilterForMultitermQueries;
        this.create = create;
    }

    /**
     * Can this {@link TokenFilter} be used in multi-term queries?
     */
    public boolean shouldUseFilterForMultitermQueries() {
        return useFilterForMultitermQueries;
    }

    @Override
    protected CharFilterFactory create(Version version) {
        if (useFilterForMultitermQueries) {
            return new NormalizingCharFilterFactory() {
                @Override
                public String name() {
                    return getName();
                }

                @Override
                public Reader create(Reader reader) {
                    return create.apply(reader, version);
                }
            };
        }
        return new CharFilterFactory() {
            @Override
            public Reader create(Reader reader) {
                return create.apply(reader, version);
            }

            @Override
            public String name() {
                return getName();
            }
        };
    }

}

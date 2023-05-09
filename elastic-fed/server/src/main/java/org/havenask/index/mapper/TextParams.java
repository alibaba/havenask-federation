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

package org.havenask.index.mapper;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.havenask.index.analysis.AnalysisMode;
import org.havenask.index.analysis.AnalysisRegistry;
import org.havenask.index.analysis.IndexAnalyzers;
import org.havenask.index.analysis.NamedAnalyzer;
import org.havenask.index.mapper.ParametrizedFieldMapper.Parameter;
import org.havenask.index.similarity.SimilarityProvider;

import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Utility functions for text mapper parameters
 */
public final class TextParams {

    private TextParams() {}

    public static final class Analyzers {
        public final Parameter<NamedAnalyzer> indexAnalyzer;
        public final Parameter<NamedAnalyzer> searchAnalyzer;
        public final Parameter<NamedAnalyzer> searchQuoteAnalyzer;

        public final IndexAnalyzers indexAnalyzers;

        public Analyzers(IndexAnalyzers indexAnalyzers) {
            this.indexAnalyzer = Parameter.analyzerParam("analyzer", false,
                m -> m.fieldType().indexAnalyzer(), indexAnalyzers::getDefaultIndexAnalyzer)
                .setSerializerCheck((id, ic, a) -> id || ic ||
                    Objects.equals(a, getSearchAnalyzer()) == false || Objects.equals(a, getSearchQuoteAnalyzer()) == false)
                .setValidator(a -> a.checkAllowedInMode(AnalysisMode.INDEX_TIME));
            this.searchAnalyzer
                = Parameter.analyzerParam("search_analyzer", true,
                m -> m.fieldType().getTextSearchInfo().getSearchAnalyzer(), () -> {
                    NamedAnalyzer defaultAnalyzer = indexAnalyzers.get(AnalysisRegistry.DEFAULT_SEARCH_ANALYZER_NAME);
                    if (defaultAnalyzer != null) {
                        return defaultAnalyzer;
                    }
                    return indexAnalyzer.get();
                })
                .setSerializerCheck((id, ic, a) -> id || ic || Objects.equals(a, getSearchQuoteAnalyzer()) == false)
                .setValidator(a -> a.checkAllowedInMode(AnalysisMode.SEARCH_TIME));
            this.searchQuoteAnalyzer
                = Parameter.analyzerParam("search_quote_analyzer", true,
                m -> m.fieldType().getTextSearchInfo().getSearchQuoteAnalyzer(), () -> {
                    NamedAnalyzer defaultAnalyzer = indexAnalyzers.get(AnalysisRegistry.DEFAULT_SEARCH_QUOTED_ANALYZER_NAME);
                    if (defaultAnalyzer != null) {
                        return defaultAnalyzer;
                    }
                    return searchAnalyzer.get();
                })
                .setValidator(a -> a.checkAllowedInMode(AnalysisMode.SEARCH_TIME));

            this.indexAnalyzers = indexAnalyzers;
        }

        public NamedAnalyzer getIndexAnalyzer() {
            return indexAnalyzer.getValue();
        }

        public NamedAnalyzer getSearchAnalyzer() {
            return searchAnalyzer.getValue();
        }

        public NamedAnalyzer getSearchQuoteAnalyzer() {
            return searchQuoteAnalyzer.getValue();
        }
    }

    public static Parameter<Boolean> norms(boolean defaultValue, Function<FieldMapper, Boolean> initializer) {
        return Parameter.boolParam("norms", true, initializer, defaultValue)
            .setMergeValidator((o, n) -> o == n || (o && n == false));  // norms can be updated from 'true' to 'false' but not vv
    }

    public static Parameter<SimilarityProvider> similarity(Function<FieldMapper, SimilarityProvider> init) {
        return new Parameter<>("similarity", false, () -> null,
            (n, c, o) -> TypeParsers.resolveSimilarity(c, n, o), init)
            .setSerializer((b, f, v) -> b.field(f, v == null ? null : v.name()), v -> v == null ? null : v.name())
            .acceptsNull();
    }

    public static Parameter<String> indexOptions(Function<FieldMapper, String> initializer) {
        return Parameter.restrictedStringParam("index_options", false, initializer,
            "positions", "docs", "freqs", "offsets");
    }

    public static FieldType buildFieldType(Supplier<Boolean> indexed,
                                           Supplier<Boolean> stored,
                                           Supplier<String> indexOptions,
                                           Supplier<Boolean> norms,
                                           Supplier<String> termVectors) {
        FieldType ft = new FieldType();
        ft.setStored(stored.get());
        ft.setTokenized(true);
        ft.setIndexOptions(toIndexOptions(indexed.get(), indexOptions.get()));
        ft.setOmitNorms(norms.get() == false);
        setTermVectorParams(termVectors.get(), ft);
        return ft;
    }

    public static IndexOptions toIndexOptions(boolean indexed, String indexOptions) {
        if (indexed == false) {
            return IndexOptions.NONE;
        }
        switch (indexOptions) {
            case "docs":
                return IndexOptions.DOCS;
            case "freqs":
                return IndexOptions.DOCS_AND_FREQS;
            case "positions":
                return IndexOptions.DOCS_AND_FREQS_AND_POSITIONS;
            case "offsets":
                return IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS;
        }
        throw new IllegalArgumentException("Unknown [index_options] value: [" + indexOptions + "]");
    }

    public static Parameter<String> termVectors(Function<FieldMapper, String> initializer) {
        return Parameter.restrictedStringParam("term_vector", false, initializer,
            "no",
            "yes",
            "with_positions",
            "with_offsets",
            "with_positions_offsets",
            "with_positions_payloads",
            "with_positions_offsets_payloads");
    }

    public static void setTermVectorParams(String configuration, FieldType fieldType) {
        switch (configuration) {
            case "no":
                fieldType.setStoreTermVectors(false);
                return;
            case "yes":
                fieldType.setStoreTermVectors(true);
                return;
            case "with_positions":
                fieldType.setStoreTermVectors(true);
                fieldType.setStoreTermVectorPositions(true);
                return;
            case "with_offsets":
                fieldType.setStoreTermVectors(true);
                fieldType.setStoreTermVectorOffsets(true);
                return;
            case "with_positions_offsets":
                fieldType.setStoreTermVectors(true);
                fieldType.setStoreTermVectorPositions(true);
                fieldType.setStoreTermVectorOffsets(true);
                return;
            case "with_positions_payloads":
                fieldType.setStoreTermVectors(true);
                fieldType.setStoreTermVectorPositions(true);
                fieldType.setStoreTermVectorPayloads(true);
                return;
            case "with_positions_offsets_payloads":
                fieldType.setStoreTermVectors(true);
                fieldType.setStoreTermVectorPositions(true);
                fieldType.setStoreTermVectorOffsets(true);
                fieldType.setStoreTermVectorPayloads(true);
                return;
        }
        throw new IllegalArgumentException("Unknown [term_vector] setting: [" + configuration + "]");
    }

}

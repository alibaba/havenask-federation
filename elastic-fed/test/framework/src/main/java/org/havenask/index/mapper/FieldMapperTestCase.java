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

import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.havenask.Version;
import org.havenask.common.Strings;
import org.havenask.common.compress.CompressedXContent;
import org.havenask.common.settings.Settings;
import org.havenask.common.xcontent.ToXContent;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.common.xcontent.json.JsonXContent;
import org.havenask.index.IndexService;
import org.havenask.index.analysis.AnalyzerScope;
import org.havenask.index.analysis.NamedAnalyzer;
import org.havenask.test.HavenaskSingleNodeTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * Base class for testing {@link FieldMapper}s.
 * @param <T> builder for the mapper to test
 * @deprecated prefer {@link FieldMapperTestCase2}
 */
@Deprecated
public abstract class FieldMapperTestCase<T extends FieldMapper.Builder<?>> extends HavenaskSingleNodeTestCase {

    protected final Settings SETTINGS = Settings.builder()
        .put("index.version.created", Version.CURRENT)
        .build();

    private final class Modifier {
        final String property;
        final boolean updateable;
        final BiConsumer<T, T> modifier;

        Modifier(String property, boolean updateable, BiConsumer<T, T> modifier) {
            this.property = property;
            this.updateable = updateable;
            this.modifier = modifier;
        }

        void apply(T first, T second) {
            modifier.accept(first, second);
        }
    }

    private Modifier booleanModifier(String name, boolean updateable, BiConsumer<T, Boolean> method) {
        return new Modifier(name, updateable, (a, b) -> {
            method.accept(a, true);
            method.accept(b, false);
        });
    }

    protected Set<String> unsupportedProperties() {
        return Collections.emptySet();
    }

    private final List<Modifier> modifiers = new ArrayList<>(Arrays.asList(
        new Modifier("analyzer", false, (a, b) -> {
            a.indexAnalyzer(new NamedAnalyzer("standard", AnalyzerScope.INDEX, new StandardAnalyzer()));
            a.indexAnalyzer(new NamedAnalyzer("keyword", AnalyzerScope.INDEX, new KeywordAnalyzer()));
        }),
        new Modifier("boost", true, (a, b) -> {
           a.boost(1.1f);
           b.boost(1.2f);
        }),
        new Modifier("doc_values", false, (a, b) -> {
            a.docValues(true);
            b.docValues(false);
        }),
        booleanModifier("eager_global_ordinals", true, (a, t) -> a.setEagerGlobalOrdinals(t)),
        booleanModifier("index", false, (a, t) -> a.index(t)),
        booleanModifier("norms", false, FieldMapper.Builder::omitNorms),
        new Modifier("search_analyzer", true, (a, b) -> {
            a.searchAnalyzer(new NamedAnalyzer("standard", AnalyzerScope.INDEX, new StandardAnalyzer()));
            a.searchAnalyzer(new NamedAnalyzer("keyword", AnalyzerScope.INDEX, new KeywordAnalyzer()));
        }),
        new Modifier("search_quote_analyzer", true, (a, b) -> {
            a.searchQuoteAnalyzer(new NamedAnalyzer("standard", AnalyzerScope.INDEX, new StandardAnalyzer()));
            a.searchQuoteAnalyzer(new NamedAnalyzer("whitespace", AnalyzerScope.INDEX, new WhitespaceAnalyzer()));
        }),
        new Modifier("store", false, (a, b) -> {
            a.store(true);
            b.store(false);
        }),
        new Modifier("term_vector", false, (a, b) -> {
            a.storeTermVectors(true);
            b.storeTermVectors(false);
        }),
        new Modifier("term_vector_positions", false, (a, b) -> {
            a.storeTermVectors(true);
            b.storeTermVectors(true);
            a.storeTermVectorPositions(true);
            b.storeTermVectorPositions(false);
        }),
        new Modifier("term_vector_payloads", false, (a, b) -> {
            a.storeTermVectors(true);
            b.storeTermVectors(true);
            a.storeTermVectorPositions(true);
            b.storeTermVectorPositions(true);
            a.storeTermVectorPayloads(true);
            b.storeTermVectorPayloads(false);
        }),
        new Modifier("term_vector_offsets", false, (a, b) -> {
            a.storeTermVectors(true);
            b.storeTermVectors(true);
            a.storeTermVectorPositions(true);
            b.storeTermVectorPositions(true);
            a.storeTermVectorOffsets(true);
            b.storeTermVectorOffsets(false);
        })
    ));

    /**
     * Add type-specific modifiers for consistency checking.
     *
     * This should be called in a {@code @Before} method
     */
    protected void addModifier(String property, boolean updateable, BiConsumer<T, T> method) {
        modifiers.add(new Modifier(property, updateable, method));
    }

    /**
     * Add type-specific modifiers for consistency checking.
     *
     * This should be called in a {@code @Before} method
     */
    protected void addBooleanModifier(String property, boolean updateable, BiConsumer<T, Boolean> method) {
        modifiers.add(new Modifier(property, updateable, (a, b) -> {
            method.accept(a, true);
            method.accept(b, false);
        }));
    }

    protected abstract T newBuilder();

    public void testMergeConflicts() {
        Mapper.BuilderContext context = new Mapper.BuilderContext(SETTINGS, new ContentPath(1));
        T builder1 = newBuilder();
        T builder2 = newBuilder();
        {
            FieldMapper mapper = (FieldMapper) builder1.build(context);
            FieldMapper toMerge = (FieldMapper) builder2.build(context);
            mapper.merge(toMerge);  // identical mappers should merge with no issue
        }
        {
            FieldMapper mapper = (FieldMapper) newBuilder().build(context);
            FieldMapper toMerge = new MockFieldMapper("bogus") {
                @Override
                protected String contentType() {
                    return "bogustype";
                }
            };
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> mapper.merge(toMerge));
            assertThat(e.getMessage(), containsString("cannot be changed from type"));
            assertThat(e.getMessage(), containsString("bogustype"));
        }
        for (Modifier modifier : modifiers) {
            if (unsupportedProperties().contains(modifier.property)) {
                continue;
            }
            builder1 = newBuilder();
            builder2 = newBuilder();
            modifier.apply(builder1, builder2);
            FieldMapper mapper = (FieldMapper) builder1.build(context);
            FieldMapper toMerge = (FieldMapper) builder2.build(context);
            if (modifier.updateable) {
                mapper.merge(toMerge);
            } else {
                IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                    "Expected an error when merging property difference " + modifier.property, () -> mapper.merge(toMerge));
                assertThat(e.getMessage(), containsString(modifier.property));
            }
        }
    }

    public void testSerialization() throws IOException {
        for (Modifier modifier : modifiers) {
            if (unsupportedProperties().contains(modifier.property)) {
                continue;
            }
            T builder1 = newBuilder();
            T builder2 = newBuilder();
            modifier.apply(builder1, builder2);
            assertSerializes(modifier.property + "-a", builder1);
            assertSerializes(modifier.property + "-b", builder2);
        }
    }

    protected Settings getIndexMapperSettings() {
        return Settings.EMPTY;
    }

    protected void assertSerializes(String indexname, T builder) throws IOException {

        // TODO can we do this without building an entire index?
        IndexService index = createIndex("serialize-" + indexname, getIndexMapperSettings());
        MapperService mapperService = index.mapperService();

        Mapper.BuilderContext context = new Mapper.BuilderContext(SETTINGS, new ContentPath(1));

        String mappings = mappingsToString(builder.build(context), false);
        String mappingsWithDefault = mappingsToString(builder.build(context), true);

        mapperService.merge("_doc", new CompressedXContent(mappings), MapperService.MergeReason.MAPPING_UPDATE);

        Mapper rebuilt = mapperService.documentMapper().mappers().getMapper(builder.name);
        String reparsed = mappingsToString(rebuilt, false);
        String reparsedWithDefault = mappingsToString(rebuilt, true);

        assertThat(reparsed, equalTo(mappings));
        assertThat(reparsedWithDefault, equalTo(mappingsWithDefault));
    }

    private String mappingsToString(ToXContent builder, boolean includeDefaults) throws IOException {
        ToXContent.Params params = includeDefaults ?
            new ToXContent.MapParams(Collections.singletonMap("include_defaults", "true")) : ToXContent.EMPTY_PARAMS;
        XContentBuilder x = JsonXContent.contentBuilder();
        x.startObject().startObject("properties");
        builder.toXContent(x, params);
        x.endObject().endObject();
        return Strings.toString(x);
    }
}

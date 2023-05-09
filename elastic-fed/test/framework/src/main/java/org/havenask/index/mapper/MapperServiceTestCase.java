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

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.store.Directory;
import org.havenask.Version;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.common.CheckedConsumer;
import org.havenask.common.bytes.BytesArray;
import org.havenask.common.bytes.BytesReference;
import org.havenask.common.compress.CompressedXContent;
import org.havenask.common.settings.Settings;
import org.havenask.common.xcontent.ToXContent;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.common.xcontent.XContentFactory;
import org.havenask.common.xcontent.XContentType;
import org.havenask.common.xcontent.json.JsonXContent;
import org.havenask.index.IndexSettings;
import org.havenask.index.analysis.AnalyzerScope;
import org.havenask.index.analysis.IndexAnalyzers;
import org.havenask.index.analysis.NamedAnalyzer;
import org.havenask.index.query.QueryShardContext;
import org.havenask.index.similarity.SimilarityService;
import org.havenask.indices.IndicesModule;
import org.havenask.indices.mapper.MapperRegistry;
import org.havenask.plugins.MapperPlugin;
import org.havenask.plugins.Plugin;
import org.havenask.plugins.ScriptPlugin;
import org.havenask.script.ScriptModule;
import org.havenask.script.ScriptService;
import org.havenask.search.lookup.SearchLookup;
import org.havenask.test.HavenaskTestCase;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toList;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class MapperServiceTestCase extends HavenaskTestCase {

    protected static final Settings SETTINGS = Settings.builder().put("index.version.created", Version.CURRENT).build();

    protected static final ToXContent.Params INCLUDE_DEFAULTS
        = new ToXContent.MapParams(Collections.singletonMap("include_defaults", "true"));

    protected Collection<? extends Plugin> getPlugins() {
        return emptyList();
    }

    protected Settings getIndexSettings() {
        return SETTINGS;
    }

    protected IndexAnalyzers createIndexAnalyzers(IndexSettings indexSettings) {
        return new IndexAnalyzers(
            singletonMap("default", new NamedAnalyzer("default", AnalyzerScope.INDEX, new StandardAnalyzer())),
            emptyMap(),
            emptyMap()
        );
    }

    protected final String randomIndexOptions() {
        return randomFrom("docs", "freqs", "positions", "offsets");
    }

    protected final DocumentMapper createDocumentMapper(XContentBuilder mappings) throws IOException {
        return createMapperService(mappings).documentMapper();
    }

    protected final DocumentMapper createDocumentMapper(Version version, XContentBuilder mappings) throws IOException {
        return createMapperService(version, mappings).documentMapper();
    }

    protected final DocumentMapper createDocumentMapper(String type, String mappings) throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {}));
        merge(type, mapperService, mappings);
        return mapperService.documentMapper();
    }

    protected MapperService createMapperService(XContentBuilder mappings) throws IOException {
        return createMapperService(Version.CURRENT, mappings);
    }

    protected final MapperService createMapperService(String type, String mappings) throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {}));
        merge(type, mapperService, mappings);
        return mapperService;
    }

    /**
     * Create a {@link MapperService} like we would for an index.
     */
    protected final MapperService createMapperService(Version version, XContentBuilder mapping) throws IOException {
        IndexMetadata meta = IndexMetadata.builder("index")
            .settings(Settings.builder().put("index.version.created", version))
            .numberOfReplicas(0)
            .numberOfShards(1)
            .build();
        IndexSettings indexSettings = new IndexSettings(meta, getIndexSettings());
        MapperRegistry mapperRegistry = new IndicesModule(
            getPlugins().stream().filter(p -> p instanceof MapperPlugin).map(p -> (MapperPlugin) p).collect(toList())
        ).getMapperRegistry();
        ScriptModule scriptModule = new ScriptModule(
            Settings.EMPTY,
            getPlugins().stream().filter(p -> p instanceof ScriptPlugin).map(p -> (ScriptPlugin) p).collect(toList())
        );
        ScriptService scriptService = new ScriptService(getIndexSettings(), scriptModule.engines, scriptModule.contexts);
        SimilarityService similarityService = new SimilarityService(indexSettings, scriptService, emptyMap());
        MapperService mapperService = new MapperService(
            indexSettings,
            createIndexAnalyzers(indexSettings),
            xContentRegistry(),
            similarityService,
            mapperRegistry,
            () -> { throw new UnsupportedOperationException(); },
            () -> true,
            scriptService
        );
        merge(mapperService, mapping);
        return mapperService;
    }

    protected final void withLuceneIndex(
        MapperService mapperService,
        CheckedConsumer<RandomIndexWriter, IOException> builder,
        CheckedConsumer<IndexReader, IOException> test
    ) throws IOException {
        try (
            Directory dir = newDirectory();
            RandomIndexWriter iw = new RandomIndexWriter(random(), dir, new IndexWriterConfig(mapperService.indexAnalyzer()))
        ) {
            builder.accept(iw);
            try (IndexReader reader = iw.getReader()) {
                test.accept(reader);
            }
        }
    }

    protected final SourceToParse source(CheckedConsumer<XContentBuilder, IOException> build) throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder().startObject();
        build.accept(builder);
        builder.endObject();
        return new SourceToParse("test", "_doc", "1", BytesReference.bytes(builder), XContentType.JSON);
    }

    protected final SourceToParse source(String source) {
        return new SourceToParse("test", "_doc", "1", new BytesArray(source), XContentType.JSON);
    }

    /**
     * Merge a new mapping into the one in the provided {@link MapperService}.
     */
    protected final void merge(MapperService mapperService, XContentBuilder mapping) throws IOException {
        merge(mapperService, MapperService.MergeReason.MAPPING_UPDATE, mapping);
    }

    /**
     * Merge a new mapping into the one in the provided {@link MapperService}.
     */
    protected final void merge(String type, MapperService mapperService, String mapping) throws IOException {
        mapperService.merge(type, new CompressedXContent(mapping), MapperService.MergeReason.MAPPING_UPDATE);
    }

    /**
     * Merge a new mapping into the one in the provided {@link MapperService} with a specific {@code MergeReason}
     */
    protected final void merge(MapperService mapperService,
                               MapperService.MergeReason reason,
                               XContentBuilder mapping) throws IOException {
        mapperService.merge("_doc", new CompressedXContent(BytesReference.bytes(mapping)), reason);
    }

    protected final XContentBuilder topMapping(CheckedConsumer<XContentBuilder, IOException> buildFields) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject().startObject("_doc");
        buildFields.accept(builder);
        return builder.endObject().endObject();
    }

    protected final XContentBuilder mapping(CheckedConsumer<XContentBuilder, IOException> buildFields) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject().startObject("_doc").startObject("properties");
        buildFields.accept(builder);
        return builder.endObject().endObject().endObject();
    }

    protected final XContentBuilder dynamicMapping(Mapping dynamicMapping) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
        dynamicMapping.toXContent(builder, ToXContent.EMPTY_PARAMS);
        return builder.endObject();
    }

    protected final XContentBuilder fieldMapping(CheckedConsumer<XContentBuilder, IOException> buildField) throws IOException {
        return mapping(b -> {
            b.startObject("field");
            buildField.accept(b);
            b.endObject();
        });
    }

    QueryShardContext createQueryShardContext(MapperService mapperService) {
        QueryShardContext queryShardContext = mock(QueryShardContext.class);
        when(queryShardContext.getMapperService()).thenReturn(mapperService);
        when(queryShardContext.fieldMapper(anyString())).thenAnswer(inv -> mapperService.fieldType(inv.getArguments()[0].toString()));
        when(queryShardContext.getIndexAnalyzers()).thenReturn(mapperService.getIndexAnalyzers());
        when(queryShardContext.getSearchQuoteAnalyzer(anyObject())).thenCallRealMethod();
        when(queryShardContext.getSearchAnalyzer(anyObject())).thenCallRealMethod();
        when(queryShardContext.getIndexSettings()).thenReturn(mapperService.getIndexSettings());
        when(queryShardContext.simpleMatchToIndexNames(anyObject())).thenAnswer(
            inv -> mapperService.simpleMatchToFullName(inv.getArguments()[0].toString())
        );
        when(queryShardContext.allowExpensiveQueries()).thenReturn(true);
        when(queryShardContext.lookup()).thenReturn(new SearchLookup(
            mapperService,
            (ft, s) -> {
                throw new UnsupportedOperationException("search lookup not available");
            },
            null));
        return queryShardContext;
    }
}

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

package org.havenask.action.admin.indices.template.post;

import org.havenask.Version;
import org.havenask.action.ActionListener;
import org.havenask.action.support.ActionFilters;
import org.havenask.action.support.master.TransportMasterNodeReadAction;
import org.havenask.cluster.ClusterState;
import org.havenask.cluster.block.ClusterBlockException;
import org.havenask.cluster.block.ClusterBlockLevel;
import org.havenask.cluster.metadata.AliasMetadata;
import org.havenask.cluster.metadata.AliasValidator;
import org.havenask.cluster.metadata.ComposableIndexTemplate;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.cluster.metadata.IndexNameExpressionResolver;
import org.havenask.cluster.metadata.Metadata;
import org.havenask.cluster.metadata.MetadataCreateIndexService;
import org.havenask.cluster.metadata.MetadataIndexTemplateService;
import org.havenask.cluster.metadata.Template;
import org.havenask.cluster.service.ClusterService;
import org.havenask.common.UUIDs;
import org.havenask.common.compress.CompressedXContent;
import org.havenask.common.inject.Inject;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.settings.Settings;
import org.havenask.common.xcontent.NamedXContentRegistry;
import org.havenask.index.mapper.DocumentMapper;
import org.havenask.index.mapper.MapperService;
import org.havenask.indices.IndicesService;
import org.havenask.threadpool.ThreadPool;
import org.havenask.transport.TransportService;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.havenask.cluster.metadata.MetadataIndexTemplateService.findConflictingV1Templates;
import static org.havenask.cluster.metadata.MetadataIndexTemplateService.findConflictingV2Templates;
import static org.havenask.cluster.metadata.MetadataIndexTemplateService.findV2Template;
import static org.havenask.cluster.metadata.MetadataIndexTemplateService.resolveSettings;

public class TransportSimulateIndexTemplateAction
    extends TransportMasterNodeReadAction<SimulateIndexTemplateRequest, SimulateIndexTemplateResponse> {

    private final MetadataIndexTemplateService indexTemplateService;
    private final NamedXContentRegistry xContentRegistry;
    private final IndicesService indicesService;
    private AliasValidator aliasValidator;

    @Inject
    public TransportSimulateIndexTemplateAction(TransportService transportService, ClusterService clusterService,
                                                ThreadPool threadPool, MetadataIndexTemplateService indexTemplateService,
                                                ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                                NamedXContentRegistry xContentRegistry, IndicesService indicesService) {
        super(SimulateIndexTemplateAction.NAME, transportService, clusterService, threadPool, actionFilters,
            SimulateIndexTemplateRequest::new, indexNameExpressionResolver);
        this.indexTemplateService = indexTemplateService;
        this.xContentRegistry = xContentRegistry;
        this.indicesService = indicesService;
        this.aliasValidator = new AliasValidator();
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected SimulateIndexTemplateResponse read(StreamInput in) throws IOException {
        return new SimulateIndexTemplateResponse(in);
    }

    @Override
    protected void masterOperation(SimulateIndexTemplateRequest request, ClusterState state,
                                   ActionListener<SimulateIndexTemplateResponse> listener) throws Exception {
        final ClusterState stateWithTemplate;
        if (request.getIndexTemplateRequest() != null) {
            // we'll "locally" add the template defined by the user in the cluster state (as if it existed in the system)
            String simulateTemplateToAdd = "simulate_index_template_" + UUIDs.randomBase64UUID().toLowerCase(Locale.ROOT);
            // Perform validation for things like typos in component template names
            MetadataIndexTemplateService.validateV2TemplateRequest(state.metadata(), simulateTemplateToAdd,
                request.getIndexTemplateRequest().indexTemplate());
            stateWithTemplate = indexTemplateService.addIndexTemplateV2(state, request.getIndexTemplateRequest().create(),
                simulateTemplateToAdd, request.getIndexTemplateRequest().indexTemplate());
        } else {
            stateWithTemplate = state;
        }

        String matchingTemplate = findV2Template(stateWithTemplate.metadata(), request.getIndexName(), false);
        if (matchingTemplate == null) {
            listener.onResponse(new SimulateIndexTemplateResponse(null, null));
            return;
        }

        final ClusterState tempClusterState = resolveTemporaryState(matchingTemplate, request.getIndexName(), stateWithTemplate);
        ComposableIndexTemplate templateV2 = tempClusterState.metadata().templatesV2().get(matchingTemplate);
        assert templateV2 != null : "the matched template must exist";

        final Template template = resolveTemplate(matchingTemplate, request.getIndexName(), stateWithTemplate,
            xContentRegistry, indicesService, aliasValidator);

        final Map<String, List<String>> overlapping = new HashMap<>();
        overlapping.putAll(findConflictingV1Templates(tempClusterState, matchingTemplate, templateV2.indexPatterns()));
        overlapping.putAll(findConflictingV2Templates(tempClusterState, matchingTemplate, templateV2.indexPatterns()));

        listener.onResponse(new SimulateIndexTemplateResponse(template, overlapping));
    }

    @Override
    protected ClusterBlockException checkBlock(SimulateIndexTemplateRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    /**
     * Return a temporary cluster state with an index that exists using the
     * matched template's settings
     */
    public static ClusterState resolveTemporaryState(final String matchingTemplate, final String indexName,
                                                     final ClusterState simulatedState) {
        Settings settings = resolveSettings(simulatedState.metadata(), matchingTemplate);

        // create the index with dummy settings in the cluster state so we can parse and validate the aliases
        Settings dummySettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(settings)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
            .build();
        final IndexMetadata indexMetadata = IndexMetadata.builder(indexName).settings(dummySettings).build();

        return ClusterState.builder(simulatedState)
            .metadata(Metadata.builder(simulatedState.metadata())
                .put(indexMetadata, true)
                .build())
            .build();
    }

    /**
     * Take a template and index name as well as state where the template exists, and return a final
     * {@link Template} that represents all the resolved Settings, Mappings, and Aliases
     */
    public static Template resolveTemplate(final String matchingTemplate, final String indexName,
                                           final ClusterState simulatedState,
                                           final NamedXContentRegistry xContentRegistry,
                                           final IndicesService indicesService,
                                           final AliasValidator aliasValidator) throws Exception {
        Settings settings = resolveSettings(simulatedState.metadata(), matchingTemplate);

        List<Map<String, AliasMetadata>> resolvedAliases = MetadataIndexTemplateService.resolveAliases(simulatedState.metadata(),
            matchingTemplate);

        // create the index with dummy settings in the cluster state so we can parse and validate the aliases
        Settings dummySettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(settings)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
            .build();
        final IndexMetadata indexMetadata = IndexMetadata.builder(indexName).settings(dummySettings).build();

        final ClusterState tempClusterState = ClusterState.builder(simulatedState)
            .metadata(Metadata.builder(simulatedState.metadata())
                .put(indexMetadata, true)
                .build())
            .build();
        List<AliasMetadata> aliases = indicesService.withTempIndexService(indexMetadata, tempIndexService ->
            MetadataCreateIndexService.resolveAndValidateAliases(indexName, Collections.emptySet(),
                resolvedAliases, tempClusterState.metadata(), aliasValidator, xContentRegistry,
                // the context is only used for validation so it's fine to pass fake values for the
                // shard id and the current timestamp
                tempIndexService.newQueryShardContext(0, null, () -> 0L, null)));
        Map<String, AliasMetadata> aliasesByName = aliases.stream().collect(
            Collectors.toMap(AliasMetadata::getAlias, Function.identity()));

        // empty request mapping as the user can't specify any explicit mappings via the simulate api
        List<Map<String, Map<String, Object>>> mappings = MetadataCreateIndexService.collectV2Mappings(
            Collections.emptyMap(), simulatedState, matchingTemplate, xContentRegistry, indexName);

        CompressedXContent mergedMapping = indicesService.<CompressedXContent, Exception>withTempIndexService(indexMetadata,
            tempIndexService -> {
                MapperService mapperService = tempIndexService.mapperService();
                for (Map<String, Map<String, Object>> mapping : mappings) {
                    if (!mapping.isEmpty()) {
                        assert mapping.size() == 1 : mapping;
                        Map.Entry<String, Map<String, Object>> entry = mapping.entrySet().iterator().next();
                        mapperService.merge(entry.getKey(), entry.getValue(), MapperService.MergeReason.INDEX_TEMPLATE);
                    }
                }

                DocumentMapper documentMapper = mapperService.documentMapper();
                return documentMapper != null ? documentMapper.mappingSource() : null;
            });

        return new Template(settings, mergedMapping, aliasesByName);
    }
}

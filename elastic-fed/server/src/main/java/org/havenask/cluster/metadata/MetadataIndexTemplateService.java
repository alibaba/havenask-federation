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

package org.havenask.cluster.metadata;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.CollectionUtil;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
import org.havenask.Version;
import org.havenask.action.ActionListener;
import org.havenask.action.admin.indices.alias.Alias;
import org.havenask.action.support.master.AcknowledgedResponse;
import org.havenask.action.support.master.MasterNodeRequest;
import org.havenask.cluster.ClusterState;
import org.havenask.cluster.ClusterStateUpdateTask;
import org.havenask.cluster.service.ClusterService;
import org.havenask.common.Nullable;
import org.havenask.common.Priority;
import org.havenask.common.Strings;
import org.havenask.common.UUIDs;
import org.havenask.common.ValidationException;
import org.havenask.common.bytes.BytesReference;
import org.havenask.common.compress.CompressedXContent;
import org.havenask.common.inject.Inject;
import org.havenask.common.logging.HeaderWarning;
import org.havenask.common.regex.Regex;
import org.havenask.common.settings.IndexScopedSettings;
import org.havenask.common.settings.Settings;
import org.havenask.common.unit.TimeValue;
import org.havenask.common.util.set.Sets;
import org.havenask.common.xcontent.NamedXContentRegistry;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.common.xcontent.XContentFactory;
import org.havenask.common.xcontent.XContentType;
import org.havenask.index.Index;
import org.havenask.index.IndexService;
import org.havenask.index.mapper.MapperParsingException;
import org.havenask.index.mapper.MapperService;
import org.havenask.index.mapper.MapperService.MergeReason;
import org.havenask.indices.IndexTemplateMissingException;
import org.havenask.indices.IndicesService;
import org.havenask.indices.InvalidIndexTemplateException;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.havenask.cluster.metadata.MetadataCreateDataStreamService.validateTimestampFieldMapping;
import static org.havenask.indices.cluster.IndicesClusterStateService.AllocatedIndices.IndexRemovalReason.NO_LONGER_ASSIGNED;

/**
 * Service responsible for submitting index templates updates
 */
public class MetadataIndexTemplateService {

    private static final Logger logger = LogManager.getLogger(MetadataIndexTemplateService.class);
    private final ClusterService clusterService;
    private final AliasValidator aliasValidator;
    private final IndicesService indicesService;
    private final MetadataCreateIndexService metadataCreateIndexService;
    private final IndexScopedSettings indexScopedSettings;
    private final NamedXContentRegistry xContentRegistry;

    @Inject
    public MetadataIndexTemplateService(ClusterService clusterService,
                                        MetadataCreateIndexService metadataCreateIndexService,
                                        AliasValidator aliasValidator, IndicesService indicesService,
                                        IndexScopedSettings indexScopedSettings, NamedXContentRegistry xContentRegistry) {
        this.clusterService = clusterService;
        this.aliasValidator = aliasValidator;
        this.indicesService = indicesService;
        this.metadataCreateIndexService = metadataCreateIndexService;
        this.indexScopedSettings = indexScopedSettings;
        this.xContentRegistry = xContentRegistry;
    }

    public void removeTemplates(final RemoveRequest request, final RemoveListener listener) {
        clusterService.submitStateUpdateTask("remove-index-template [" + request.name + "]", new ClusterStateUpdateTask(Priority.URGENT) {

            @Override
            public TimeValue timeout() {
                return request.masterTimeout;
            }

            @Override
            public void onFailure(String source, Exception e) {
                listener.onFailure(e);
            }

            @Override
            public ClusterState execute(ClusterState currentState) {
                Set<String> templateNames = new HashSet<>();
                for (ObjectCursor<String> cursor : currentState.metadata().templates().keys()) {
                    String templateName = cursor.value;
                    if (Regex.simpleMatch(request.name, templateName)) {
                        templateNames.add(templateName);
                    }
                }
                if (templateNames.isEmpty()) {
                    // if its a match all pattern, and no templates are found (we have none), don't
                    // fail with index missing...
                    if (Regex.isMatchAllPattern(request.name)) {
                        return currentState;
                    }
                    throw new IndexTemplateMissingException(request.name);
                }
                Metadata.Builder metadata = Metadata.builder(currentState.metadata());
                for (String templateName : templateNames) {
                    logger.info("removing template [{}]", templateName);
                    metadata.removeTemplate(templateName);
                }
                return ClusterState.builder(currentState).metadata(metadata).build();
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                listener.onResponse(new RemoveResponse(true));
            }
        });
    }

    /**
     * Add the given component template to the cluster state. If {@code create} is true, an
     * exception will be thrown if the component template already exists
     */
    public void putComponentTemplate(final String cause, final boolean create, final String name, final TimeValue masterTimeout,
                                     final ComponentTemplate template, final ActionListener<AcknowledgedResponse> listener) {
        clusterService.submitStateUpdateTask("create-component-template [" + name + "], cause [" + cause + "]",
            new ClusterStateUpdateTask(Priority.URGENT) {

                @Override
                public TimeValue timeout() {
                    return masterTimeout;
                }

                @Override
                public void onFailure(String source, Exception e) {
                    listener.onFailure(e);
                }

                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    return addComponentTemplate(currentState, create, name, template);
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    listener.onResponse(new AcknowledgedResponse(true));
                }
            });
    }

    // Package visible for testing
    ClusterState addComponentTemplate(final ClusterState currentState, final boolean create,
                                      final String name, final ComponentTemplate template) throws Exception {
        final ComponentTemplate existing = currentState.metadata().componentTemplates().get(name);
        if (create && existing != null) {
            throw new IllegalArgumentException("component template [" + name + "] already exists");
        }

        CompressedXContent mappings = template.template().mappings();
        String stringMappings = mappings == null ? null : mappings.string();

        // We may need to normalize index settings, so do that also
        Settings finalSettings = template.template().settings();
        if (finalSettings != null) {
            finalSettings = Settings.builder()
                .put(finalSettings).normalizePrefix(IndexMetadata.INDEX_SETTING_PREFIX)
                .build();
        }

        // Collect all the composable (index) templates that use this component template, we'll use
        // this for validating that they're still going to be valid after this component template
        // has been updated
        final Map<String, ComposableIndexTemplate> templatesUsingComponent = currentState.metadata().templatesV2().entrySet().stream()
            .filter(e -> e.getValue().composedOf().contains(name))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        // if we're updating a component template, let's check if it's part of any V2 template that will yield the CT update invalid
        if (create == false && finalSettings != null) {
            // if the CT is specifying the `index.hidden` setting it cannot be part of any global template
            if (IndexMetadata.INDEX_HIDDEN_SETTING.exists(finalSettings)) {
                List<String> globalTemplatesThatUseThisComponent = new ArrayList<>();
                for (Map.Entry<String, ComposableIndexTemplate> entry : templatesUsingComponent.entrySet()) {
                    ComposableIndexTemplate templateV2 = entry.getValue();
                    if (templateV2.indexPatterns().stream().anyMatch(Regex::isMatchAllPattern)) {
                        // global templates don't support configuring the `index.hidden` setting so we don't need to resolve the settings as
                        // no other component template can remove this setting from the resolved settings, so just invalidate this update
                        globalTemplatesThatUseThisComponent.add(entry.getKey());
                    }
                }
                if (globalTemplatesThatUseThisComponent.isEmpty() == false) {
                    throw new IllegalArgumentException("cannot update component template ["
                        + name + "] because the following global templates would resolve to specifying the ["
                        + IndexMetadata.SETTING_INDEX_HIDDEN + "] setting: [" + String.join(",", globalTemplatesThatUseThisComponent)
                        + "]");
                }
            }
        }

        // Mappings in component templates don't include _doc, so update the mappings to include this single type
        if (stringMappings != null) {
            Map<String, Object> parsedMappings = MapperService.parseMapping(xContentRegistry, stringMappings);
            if (parsedMappings.size() > 0) {
                stringMappings = Strings.toString(XContentFactory.jsonBuilder()
                    .startObject()
                    .field(MapperService.SINGLE_MAPPING_NAME, parsedMappings)
                    .endObject());
            }
        }

        final Template finalTemplate = new Template(finalSettings,
            stringMappings == null ? null : new CompressedXContent(stringMappings), template.template().aliases());
        final ComponentTemplate finalComponentTemplate = new ComponentTemplate(finalTemplate, template.version(), template.metadata());

        if (finalComponentTemplate.equals(existing)) {
            return currentState;
        }

        validateTemplate(finalSettings, stringMappings, indicesService, xContentRegistry);
        validate(name, finalComponentTemplate);

        // Validate all composable index templates that use this component template
        if (templatesUsingComponent.size() > 0) {
            ClusterState tempStateWithComponentTemplateAdded = ClusterState.builder(currentState)
                .metadata(Metadata.builder(currentState.metadata()).put(name, finalComponentTemplate))
                .build();
            Exception validationFailure = null;
            for (Map.Entry<String, ComposableIndexTemplate> entry : templatesUsingComponent.entrySet()) {
                final String composableTemplateName = entry.getKey();
                final ComposableIndexTemplate composableTemplate = entry.getValue();
                try {
                    validateCompositeTemplate(tempStateWithComponentTemplateAdded, composableTemplateName,
                        composableTemplate, indicesService, xContentRegistry);
                } catch (Exception e) {
                    if (validationFailure == null) {
                        validationFailure = new IllegalArgumentException("updating component template [" + name +
                            "] results in invalid composable template [" + composableTemplateName + "] after templates are merged", e);
                    } else {
                        validationFailure.addSuppressed(e);
                    }
                }
            }
            if (validationFailure != null) {
                throw validationFailure;
            }
        }

        logger.info("{} component template [{}]", existing == null ? "adding" : "updating", name);
        return ClusterState.builder(currentState)
            .metadata(Metadata.builder(currentState.metadata()).put(name, finalComponentTemplate))
            .build();
    }

    /**
     * Remove the given component template from the cluster state. The component template name
     * supports simple regex wildcards for removing multiple component templates at a time.
     */
    public void removeComponentTemplate(final String name, final TimeValue masterTimeout,
                                        final ActionListener<AcknowledgedResponse> listener) {
        validateNotInUse(clusterService.state().metadata(), name);
        clusterService.submitStateUpdateTask("remove-component-template [" + name + "]",
            new ClusterStateUpdateTask(Priority.URGENT) {

                @Override
                public TimeValue timeout() {
                    return masterTimeout;
                }

                @Override
                public void onFailure(String source, Exception e) {
                    listener.onFailure(e);
                }

                @Override
                public ClusterState execute(ClusterState currentState) {
                    Set<String> templateNames = new HashSet<>();
                    for (String templateName : currentState.metadata().componentTemplates().keySet()) {
                        if (Regex.simpleMatch(name, templateName)) {
                            templateNames.add(templateName);
                        }
                    }
                    if (templateNames.isEmpty()) {
                        // if its a match all pattern, and no templates are found (we have none), don't
                        // fail with index missing...
                        if (Regex.isMatchAllPattern(name)) {
                            return currentState;
                        }
                        // TODO: perhaps introduce a ComponentTemplateMissingException?
                        throw new IndexTemplateMissingException(name);
                    }
                    Metadata.Builder metadata = Metadata.builder(currentState.metadata());
                    for (String templateName : templateNames) {
                        logger.info("removing component template [{}]", templateName);
                        metadata.removeComponentTemplate(templateName);
                    }
                    return ClusterState.builder(currentState).metadata(metadata).build();
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    listener.onResponse(new AcknowledgedResponse(true));
                }
            });
    }

    /**
     * Validates that the given component template is not used by any index
     * templates, throwing an error if it is still in use
     */
    static void validateNotInUse(Metadata metadata, String templateNameOrWildcard) {
        final Set<String> matchingComponentTemplates = metadata.componentTemplates().keySet().stream()
            .filter(name -> Regex.simpleMatch(templateNameOrWildcard, name))
            .collect(Collectors.toSet());
        final Set<String> componentsBeingUsed = new HashSet<>();
        final List<String> templatesStillUsing = metadata.templatesV2().entrySet().stream()
            .filter(e -> {
                Set<String> intersecting = Sets.intersection(new HashSet<>(e.getValue().composedOf()), matchingComponentTemplates);
                if (intersecting.size() > 0) {
                    componentsBeingUsed.addAll(intersecting);
                    return true;
                }
                return false;
            })
            .map(Map.Entry::getKey)
            .collect(Collectors.toList());

        if (templatesStillUsing.size() > 0) {
            throw new IllegalArgumentException("component templates " + componentsBeingUsed +
                " cannot be removed as they are still in use by index templates " + templatesStillUsing);
        }
    }

    /**
     * Add the given index template to the cluster state. If {@code create} is true, an
     * exception will be thrown if the component template already exists
     */
    public void putIndexTemplateV2(final String cause, final boolean create, final String name, final TimeValue masterTimeout,
                                   final ComposableIndexTemplate template, final ActionListener<AcknowledgedResponse> listener) {
        validateV2TemplateRequest(clusterService.state().metadata(), name, template);
        clusterService.submitStateUpdateTask("create-index-template-v2 [" + name + "], cause [" + cause + "]",
            new ClusterStateUpdateTask(Priority.URGENT) {

                @Override
                public TimeValue timeout() {
                    return masterTimeout;
                }

                @Override
                public void onFailure(String source, Exception e) {
                    listener.onFailure(e);
                }

                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    return addIndexTemplateV2(currentState, create, name, template);
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    listener.onResponse(new AcknowledgedResponse(true));
                }
            });
    }

    public static void validateV2TemplateRequest(Metadata metadata, String name, ComposableIndexTemplate template) {
        if (template.indexPatterns().stream().anyMatch(Regex::isMatchAllPattern)) {
            Settings mergedSettings = resolveSettings(metadata, template);
            if (IndexMetadata.INDEX_HIDDEN_SETTING.exists(mergedSettings)) {
                throw new InvalidIndexTemplateException(name, "global composable templates may not specify the setting "
                    + IndexMetadata.INDEX_HIDDEN_SETTING.getKey());
            }
        }

        final Map<String, ComponentTemplate> componentTemplates = metadata.componentTemplates();
        final List<String> missingComponentTemplates = template.composedOf().stream()
            .filter(componentTemplate -> componentTemplates.containsKey(componentTemplate) == false)
            .collect(Collectors.toList());

        if (missingComponentTemplates.size() > 0) {
            throw new InvalidIndexTemplateException(name, "index template [" + name + "] specifies component templates " +
                missingComponentTemplates + " that do not exist");
        }
    }

    public ClusterState addIndexTemplateV2(final ClusterState currentState, final boolean create,
                                           final String name, final ComposableIndexTemplate template) throws Exception {
        final ComposableIndexTemplate existing = currentState.metadata().templatesV2().get(name);
        if (create && existing != null) {
            throw new IllegalArgumentException("index template [" + name + "] already exists");
        }

        Map<String, List<String>> overlaps = findConflictingV2Templates(currentState, name, template.indexPatterns(), true,
            template.priorityOrZero());
        overlaps.remove(name);
        if (overlaps.size() > 0) {
            String error = String.format(Locale.ROOT, "index template [%s] has index patterns %s matching patterns from " +
                    "existing templates [%s] with patterns (%s) that have the same priority [%d], multiple index templates may not " +
                    "match during index creation, please use a different priority",
                name,
                template.indexPatterns(),
                Strings.collectionToCommaDelimitedString(overlaps.keySet()),
                overlaps.entrySet().stream()
                    .map(e -> e.getKey() + " => " + e.getValue())
                    .collect(Collectors.joining(",")),
                template.priorityOrZero());
            throw new IllegalArgumentException(error);
        }

        overlaps = findConflictingV1Templates(currentState, name, template.indexPatterns());
        if (overlaps.size() > 0) {
            String warning = String.format(Locale.ROOT, "index template [%s] has index patterns %s matching patterns from " +
                    "existing older templates [%s] with patterns (%s); this template [%s] will take precedence during new index creation",
                name,
                template.indexPatterns(),
                Strings.collectionToCommaDelimitedString(overlaps.keySet()),
                overlaps.entrySet().stream()
                    .map(e -> e.getKey() + " => " + e.getValue())
                    .collect(Collectors.joining(",")),
                name);
            logger.warn(warning);
            HeaderWarning.addWarning(warning);
        }

        ComposableIndexTemplate finalIndexTemplate = template;
        Template innerTemplate = template.template();
        if (innerTemplate != null) {
            // We may need to normalize index settings, so do that also
            Settings finalSettings = innerTemplate.settings();
            if (finalSettings != null) {
                finalSettings = Settings.builder()
                    .put(finalSettings).normalizePrefix(IndexMetadata.INDEX_SETTING_PREFIX)
                    .build();
            }
            // If an inner template was specified, its mappings may need to be
            // adjusted (to add _doc) and it should be validated
            CompressedXContent mappings = innerTemplate.mappings();
            String stringMappings = mappings == null ? null : mappings.string();

            // Mappings in index templates don't include _doc, so update the mappings to include this single type
            if (stringMappings != null) {
                Map<String, Object> parsedMappings = MapperService.parseMapping(xContentRegistry, stringMappings);
                if (parsedMappings.size() > 0) {
                    stringMappings = Strings.toString(XContentFactory.jsonBuilder()
                        .startObject()
                        .field(MapperService.SINGLE_MAPPING_NAME, parsedMappings)
                        .endObject());
                }
            }
            final Template finalTemplate = new Template(finalSettings,
                stringMappings == null ? null : new CompressedXContent(stringMappings), innerTemplate.aliases());
            finalIndexTemplate = new ComposableIndexTemplate(template.indexPatterns(), finalTemplate, template.composedOf(),
                template.priority(), template.version(), template.metadata(), template.getDataStreamTemplate());
        }

        if (finalIndexTemplate.equals(existing)) {
            return currentState;
        }

        validate(name, finalIndexTemplate);
        validateDataStreamsStillReferenced(currentState, name, finalIndexTemplate);

        // Finally, right before adding the template, we need to ensure that the composite settings,
        // mappings, and aliases are valid after it's been composed with the component templates
        try {
            validateCompositeTemplate(currentState, name, finalIndexTemplate, indicesService, xContentRegistry);
        } catch (Exception e) {
            throw new IllegalArgumentException("composable template [" + name +
                "] template after composition " +
                (finalIndexTemplate.composedOf().size() > 0 ? "with component templates " + finalIndexTemplate.composedOf() + " " : "") +
                "is invalid", e);
        }
        logger.info("{} index template [{}] for index patterns {}", existing == null ? "adding" : "updating", name,
            template.indexPatterns());
        return ClusterState.builder(currentState)
            .metadata(Metadata.builder(currentState.metadata()).put(name, finalIndexTemplate))
            .build();
    }

    /**
     * Validate that by changing or adding {@code newTemplate}, there are
     * no unreferenced data streams. Note that this scenario is still possible
     * due to snapshot restores, but this validation is best-effort at template
     * addition/update time
     */
    private static void validateDataStreamsStillReferenced(ClusterState state, String templateName,
                                                           ComposableIndexTemplate newTemplate) {
        final Set<String> dataStreams = state.metadata().dataStreams().keySet();

        Function<Metadata, Set<String>> findUnreferencedDataStreams = meta -> {
            final Set<String> unreferenced = new HashSet<>();
            // For each data stream that we have, see whether it's covered by a different
            // template (which is great), or whether it's now uncovered by any template
            for (String dataStream : dataStreams) {
                final String matchingTemplate = findV2Template(meta, dataStream, false);
                if (matchingTemplate == null) {
                    unreferenced.add(dataStream);
                } else {
                    // We found a template that still matches, great! Buuuuttt... check whether it
                    // is a data stream template, as it's only useful if it has a data stream definition
                    if (meta.templatesV2().get(matchingTemplate).getDataStreamTemplate() == null) {
                        unreferenced.add(dataStream);
                    }
                }
            }
            return unreferenced;
        };

        // Find data streams that are currently unreferenced
        final Set<String> currentlyUnreferenced = findUnreferencedDataStreams.apply(state.metadata());

        // Generate a metadata as if the new template were actually in the cluster state
        final Metadata updatedMetadata = Metadata.builder(state.metadata()).put(templateName, newTemplate).build();
        // Find the data streams that would be unreferenced now that the template is updated/added
        final Set<String> newlyUnreferenced = findUnreferencedDataStreams.apply(updatedMetadata);

        // If we found any data streams that used to be covered, but will no longer be covered by
        // changing this template, then blow up with as much helpful information as we can muster
        if (newlyUnreferenced.size() > currentlyUnreferenced.size()) {
            throw new IllegalArgumentException("composable template [" + templateName + "] with index patterns " +
                newTemplate.indexPatterns() + ", priority [" + newTemplate.priority() + "] " +
                (newTemplate.getDataStreamTemplate() == null ? "and no data stream configuration " : "") +
                "would cause data streams " +
                newlyUnreferenced + " to no longer match a data stream template");
        }
    }

    /**
     * Return a map of v1 template names to their index patterns for v1 templates that would overlap
     * with the given v2 template's index patterns.
     */
    public static Map<String, List<String>> findConflictingV1Templates(final ClusterState state, final String candidateName,
                                                                       final List<String> indexPatterns) {
        Automaton v2automaton = Regex.simpleMatchToAutomaton(indexPatterns.toArray(Strings.EMPTY_ARRAY));
        Map<String, List<String>> overlappingTemplates = new HashMap<>();
        for (ObjectObjectCursor<String, IndexTemplateMetadata> cursor : state.metadata().templates()) {
            String name = cursor.key;
            IndexTemplateMetadata template = cursor.value;
            Automaton v1automaton = Regex.simpleMatchToAutomaton(template.patterns().toArray(Strings.EMPTY_ARRAY));
            if (Operations.isEmpty(Operations.intersection(v2automaton, v1automaton)) == false) {
                logger.debug("composable template {} and legacy template {} would overlap: {} <=> {}",
                    candidateName, name, indexPatterns, template.patterns());
                overlappingTemplates.put(name, template.patterns());
            }
        }
        return overlappingTemplates;
    }

    /**
     * Return a map of v2 template names to their index patterns for v2 templates that would overlap
     * with the given template's index patterns.
     */
    public static Map<String, List<String>> findConflictingV2Templates(final ClusterState state, final String candidateName,
                                                                       final List<String> indexPatterns) {
        return findConflictingV2Templates(state, candidateName, indexPatterns, false, 0L);
    }

    /**
     * Return a map of v2 template names to their index patterns for v2 templates that would overlap
     * with the given template's index patterns.
     *
     * Based on the provided checkPriority and priority parameters this aims to report the overlapping
     * index templates regardless of the priority (ie. checkPriority == false) or otherwise overlapping
     * templates with the same priority as the given priority parameter (this is useful when trying to
     * add a new template, as we don't support multiple overlapping, from an index pattern perspective,
     * index templates with the same priority).
     */
    static Map<String, List<String>> findConflictingV2Templates(final ClusterState state, final String candidateName,
                                                                final List<String> indexPatterns, boolean checkPriority, long priority) {
        Automaton v1automaton = Regex.simpleMatchToAutomaton(indexPatterns.toArray(Strings.EMPTY_ARRAY));
        Map<String, List<String>> overlappingTemplates = new HashMap<>();
        for (Map.Entry<String, ComposableIndexTemplate> entry : state.metadata().templatesV2().entrySet()) {
            String name = entry.getKey();
            ComposableIndexTemplate template = entry.getValue();
            Automaton v2automaton = Regex.simpleMatchToAutomaton(template.indexPatterns().toArray(Strings.EMPTY_ARRAY));
            if (Operations.isEmpty(Operations.intersection(v1automaton, v2automaton)) == false) {
                if (checkPriority == false || priority == template.priorityOrZero()) {
                    logger.debug("legacy template {} and composable template {} would overlap: {} <=> {}",
                        candidateName, name, indexPatterns, template.indexPatterns());
                    overlappingTemplates.put(name, template.indexPatterns());
                }
            }
        }
        // if the candidate was a V2 template that already exists in the cluster state it will "overlap" with itself so remove it from the
        // results
        overlappingTemplates.remove(candidateName);
        return overlappingTemplates;
    }

    /**
     * Remove the given index template from the cluster state. The index template name
     * supports simple regex wildcards for removing multiple index templates at a time.
     */
    public void removeIndexTemplateV2(final String name, final TimeValue masterTimeout,
                                      final ActionListener<AcknowledgedResponse> listener) {
        clusterService.submitStateUpdateTask("remove-index-template-v2 [" + name + "]",
            new ClusterStateUpdateTask(Priority.URGENT) {

                @Override
                public TimeValue timeout() {
                    return masterTimeout;
                }

                @Override
                public void onFailure(String source, Exception e) {
                    listener.onFailure(e);
                }

                @Override
                public ClusterState execute(ClusterState currentState) {
                    return innerRemoveIndexTemplateV2(currentState, name);
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    listener.onResponse(new AcknowledgedResponse(true));
                }
            });
    }

    // Package visible for testing
    static ClusterState innerRemoveIndexTemplateV2(ClusterState currentState, String name) {
        Set<String> templateNames = new HashSet<>();
        for (String templateName : currentState.metadata().templatesV2().keySet()) {
            if (Regex.simpleMatch(name, templateName)) {
                templateNames.add(templateName);
            }
        }
        if (templateNames.isEmpty()) {
            // if its a match all pattern, and no templates are found (we have none), don't
            // fail with index missing...
            if (Regex.isMatchAllPattern(name)) {
                return currentState;
            }
            throw new IndexTemplateMissingException(name);
        }

        Optional<Set<String>> dataStreamsUsingTemplates = templateNames.stream()
            .map(templateName -> dataStreamsUsingTemplate(currentState, templateName))
            .reduce(Sets::union);
        dataStreamsUsingTemplates.ifPresent(set -> {
            if (set.size() > 0) {
                throw new IllegalArgumentException("unable to remove composable templates " + new TreeSet<>(templateNames) +
                    " as they are in use by a data streams " + new TreeSet<>(set));
            }
        });

        Metadata.Builder metadata = Metadata.builder(currentState.metadata());
        for (String templateName : templateNames) {
            logger.info("removing index template [{}]", templateName);
            metadata.removeIndexTemplate(templateName);
        }
        return ClusterState.builder(currentState).metadata(metadata).build();
    }

    static Set<String> dataStreamsUsingTemplate(final ClusterState state, final String templateName) {
        final ComposableIndexTemplate template = state.metadata().templatesV2().get(templateName);
        if (template == null) {
            return Collections.emptySet();
        }
        final Set<String> dataStreams = state.metadata().dataStreams().keySet();
        Set<String> matches = new HashSet<>();
        template.indexPatterns().forEach(indexPattern ->
            matches.addAll(dataStreams.stream().filter(stream -> Regex.simpleMatch(indexPattern, stream)).collect(Collectors.toList())));
        return matches;
    }

    public void putTemplate(final PutRequest request, final PutListener listener) {
        Settings.Builder updatedSettingsBuilder = Settings.builder();
        updatedSettingsBuilder.put(request.settings).normalizePrefix(IndexMetadata.INDEX_SETTING_PREFIX);
        request.settings(updatedSettingsBuilder.build());

        if (request.name == null) {
            listener.onFailure(new IllegalArgumentException("index_template must provide a name"));
            return;
        }
        if (request.indexPatterns == null) {
            listener.onFailure(new IllegalArgumentException("index_template must provide a template"));
            return;
        }

        try {
            validate(request);
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }

        final IndexTemplateMetadata.Builder templateBuilder = IndexTemplateMetadata.builder(request.name);

        clusterService.submitStateUpdateTask("create-index-template [" + request.name + "], cause [" + request.cause + "]",
                new ClusterStateUpdateTask(Priority.URGENT) {

            @Override
            public TimeValue timeout() {
                return request.masterTimeout;
            }

            @Override
            public void onFailure(String source, Exception e) {
                listener.onFailure(e);
            }

            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                validateTemplate(request.settings, request.mappings, indicesService, xContentRegistry);
                return innerPutTemplate(currentState, request, templateBuilder);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                listener.onResponse(new PutResponse(true));
            }
        });
    }

    // Package visible for testing
    static ClusterState innerPutTemplate(final ClusterState currentState, PutRequest request,
                                         IndexTemplateMetadata.Builder templateBuilder) {
        // Flag for whether this is updating an existing template or adding a new one
        // TODO: in 8.0+, only allow updating index templates, not adding new ones
        boolean isUpdate = currentState.metadata().templates().containsKey(request.name);
        if (request.create && isUpdate) {
            throw new IllegalArgumentException("index_template [" + request.name + "] already exists");
        }

        Map<String, List<String>> overlaps = findConflictingV2Templates(currentState, request.name, request.indexPatterns);
        if (overlaps.size() > 0) {
            String warning = String.format(Locale.ROOT, "legacy template [%s] has index patterns %s matching patterns" +
                    " from existing composable templates [%s] with patterns (%s); this template [%s] may be ignored in favor" +
                    " of a composable template at index creation time",
                request.name,
                request.indexPatterns,
                Strings.collectionToCommaDelimitedString(overlaps.keySet()),
                overlaps.entrySet().stream()
                    .map(e -> e.getKey() + " => " + e.getValue())
                    .collect(Collectors.joining(",")),
                request.name);
            logger.warn(warning);
            HeaderWarning.addWarning(warning);
        }

        templateBuilder.order(request.order);
        templateBuilder.version(request.version);
        templateBuilder.patterns(request.indexPatterns);
        templateBuilder.settings(request.settings);

        for (Map.Entry<String, String> entry : request.mappings.entrySet()) {
            try {
                templateBuilder.putMapping(entry.getKey(), entry.getValue());
            } catch (Exception e) {
                throw new MapperParsingException("Failed to parse mapping [{}]: {}", e, entry.getKey(), e.getMessage());
            }
        }

        for (Alias alias : request.aliases) {
            AliasMetadata aliasMetadata = AliasMetadata.builder(alias.name()).filter(alias.filter())
                .indexRouting(alias.indexRouting()).searchRouting(alias.searchRouting()).build();
            templateBuilder.putAlias(aliasMetadata);
        }
        IndexTemplateMetadata template = templateBuilder.build();

        Metadata.Builder builder = Metadata.builder(currentState.metadata()).put(template);

        logger.info("adding template [{}] for index patterns {}", request.name, request.indexPatterns);
        return ClusterState.builder(currentState).metadata(builder).build();
    }

    /**
     * Finds index templates whose index pattern matched with the given index name. In the case of
     * hidden indices, a template with a match all pattern or global template will not be returned.
     *
     * @param metadata The {@link Metadata} containing all of the {@link IndexTemplateMetadata} values
     * @param indexName The name of the index that templates are being found for
     * @param isHidden Whether or not the index is known to be hidden. May be {@code null} if the index
     *                 being hidden has not been explicitly requested. When {@code null} if the result
     *                 of template application results in a hidden index, then global templates will
     *                 not be returned
     * @return a list of templates sorted by {@link IndexTemplateMetadata#order()} descending.
     *
     */
    public static List<IndexTemplateMetadata> findV1Templates(Metadata metadata, String indexName, @Nullable Boolean isHidden) {
        final Predicate<String> patternMatchPredicate = pattern -> Regex.simpleMatch(pattern, indexName);
        final List<IndexTemplateMetadata> matchedTemplates = new ArrayList<>();
        for (ObjectCursor<IndexTemplateMetadata> cursor : metadata.templates().values()) {
            final IndexTemplateMetadata template = cursor.value;
            if (isHidden == null || isHidden == Boolean.FALSE) {
                final boolean matched = template.patterns().stream().anyMatch(patternMatchPredicate);
                if (matched) {
                    matchedTemplates.add(template);
                }
            } else {
                assert isHidden == Boolean.TRUE;
                final boolean isNotMatchAllTemplate = template.patterns().stream().noneMatch(Regex::isMatchAllPattern);
                if (isNotMatchAllTemplate) {
                    if (template.patterns().stream().anyMatch(patternMatchPredicate)) {
                        matchedTemplates.add(template);
                    }
                }
            }
        }
        CollectionUtil.timSort(matchedTemplates, Comparator.comparingInt(IndexTemplateMetadata::order).reversed());

        // this is complex but if the index is not hidden in the create request but is hidden as the result of template application,
        // then we need to exclude global templates
        if (isHidden == null) {
            final Optional<IndexTemplateMetadata> templateWithHiddenSetting = matchedTemplates.stream()
                .filter(template -> IndexMetadata.INDEX_HIDDEN_SETTING.exists(template.settings())).findFirst();
            if (templateWithHiddenSetting.isPresent()) {
                final boolean templatedIsHidden = IndexMetadata.INDEX_HIDDEN_SETTING.get(templateWithHiddenSetting.get().settings());
                if (templatedIsHidden) {
                    // remove the global templates
                    matchedTemplates.removeIf(current -> current.patterns().stream().anyMatch(Regex::isMatchAllPattern));
                }
                // validate that hidden didn't change
                final Optional<IndexTemplateMetadata> templateWithHiddenSettingPostRemoval = matchedTemplates.stream()
                    .filter(template -> IndexMetadata.INDEX_HIDDEN_SETTING.exists(template.settings())).findFirst();
                if (templateWithHiddenSettingPostRemoval.isPresent() == false ||
                    templateWithHiddenSetting.get() != templateWithHiddenSettingPostRemoval.get()) {
                    throw new IllegalStateException("A global index template [" + templateWithHiddenSetting.get().name() +
                        "] defined the index hidden setting, which is not allowed");
                }
            }
        }
        return Collections.unmodifiableList(matchedTemplates);
    }

    /**
     * Return the name (id) of the highest matching index template for the given index name. In
     * the event that no templates are matched, {@code null} is returned.
     */
    @Nullable
    public static String findV2Template(Metadata metadata, String indexName, boolean isHidden) {
        final Predicate<String> patternMatchPredicate = pattern -> Regex.simpleMatch(pattern, indexName);
        final Map<ComposableIndexTemplate, String> matchedTemplates = new HashMap<>();
        for (Map.Entry<String, ComposableIndexTemplate> entry : metadata.templatesV2().entrySet()) {
            final String name = entry.getKey();
            final ComposableIndexTemplate template = entry.getValue();
            if (isHidden == false) {
                final boolean matched = template.indexPatterns().stream().anyMatch(patternMatchPredicate);
                if (matched) {
                    matchedTemplates.put(template, name);
                }
            } else {
                final boolean isNotMatchAllTemplate = template.indexPatterns().stream().noneMatch(Regex::isMatchAllPattern);
                if (isNotMatchAllTemplate) {
                    if (template.indexPatterns().stream().anyMatch(patternMatchPredicate)) {
                        matchedTemplates.put(template, name);
                    }
                }
            }
        }

        if (matchedTemplates.size() == 0) {
            return null;
        }

        final List<ComposableIndexTemplate> candidates = new ArrayList<>(matchedTemplates.keySet());
        CollectionUtil.timSort(candidates, Comparator.comparing(ComposableIndexTemplate::priorityOrZero, Comparator.reverseOrder()));

        assert candidates.size() > 0 : "we should have returned early with no candidates";
        ComposableIndexTemplate winner = candidates.get(0);
        String winnerName = matchedTemplates.get(winner);

        // if the winner template is a global template that specifies the `index.hidden` setting (which is not allowed, so it'd be due to
        // a restored index cluster state that modified a component template used by this global template such that it has this setting)
        // we will fail and the user will have to update the index template and remove this setting or update the corresponding component
        // template that contributes to the index template resolved settings
        if (winner.indexPatterns().stream().anyMatch(Regex::isMatchAllPattern) &&
            IndexMetadata.INDEX_HIDDEN_SETTING.exists(resolveSettings(metadata, winnerName))) {
            throw new IllegalStateException("global index template [" + winnerName + "], composed of component templates [" +
                String.join(",", winner.composedOf()) + "] defined the index.hidden setting, which is not allowed");
        }

        return winnerName;
    }

    /**
     * Collect the given v2 template into an ordered list of mappings.
     */
    public static List<CompressedXContent> collectMappings(final ClusterState state,
                                                           final String templateName,
                                                           final String indexName) throws Exception {
        final ComposableIndexTemplate template = state.metadata().templatesV2().get(templateName);
        assert template != null : "attempted to resolve mappings for a template [" + templateName +
            "] that did not exist in the cluster state";
        if (template == null) {
            return Collections.emptyList();
        }

        final Map<String, ComponentTemplate> componentTemplates = state.metadata().componentTemplates();
        List<CompressedXContent> mappings = template.composedOf().stream()
            .map(componentTemplates::get)
            .filter(Objects::nonNull)
            .map(ComponentTemplate::template)
            .map(Template::mappings)
            .filter(Objects::nonNull)
            .collect(Collectors.toCollection(LinkedList::new));
        // Add the actual index template's mappings, since it takes the highest precedence
        Optional.ofNullable(template.template())
            .map(Template::mappings)
            .ifPresent(mappings::add);
        if (template.getDataStreamTemplate() != null && indexName.startsWith(DataStream.BACKING_INDEX_PREFIX)) {
            // add a default mapping for the timestamp field, at the lowest precedence, to make bootstrapping data streams more
            // straightforward as all backing indices are required to have a timestamp field
            String timestampFieldName = template.getDataStreamTemplate().getTimestampField().getName();
            mappings.add(0, new CompressedXContent(getTimestampFieldMapping(timestampFieldName)));
        }

        // Only include timestamp mapping snippet if creating backing index.
        if (indexName.startsWith(DataStream.BACKING_INDEX_PREFIX)) {
            // Only if template has data stream definition this should be added and
            // adding this template last, since timestamp field should have highest precedence:
            Optional.ofNullable(template.getDataStreamTemplate())
                .map(ComposableIndexTemplate.DataStreamTemplate::getDataStreamMappingSnippet)
                .map(mapping -> {
                    try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
                        builder.value(mapping);
                        return new CompressedXContent(BytesReference.bytes(builder));
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                })
                .ifPresent(mappings::add);
        }
        return Collections.unmodifiableList(mappings);
    }

    /**
     * Returns the default mapping snippet for the timestamp field by configuring it as a 'date' type.
     * This is added at the lowest precedence to allow users to override this mapping.
     */
    private static String getTimestampFieldMapping(String timestampFieldName) {
        return "{\n" +
            "   \"_doc\": {\n" +
            "     \"properties\": {\n" +
            "       \"" + timestampFieldName + "\": {\n" +
            "         \"type\": \"date\"\n" +
            "       }\n" +
            "     }\n" +
            "   }\n" +
            " }";
    }

    /**
     * Resolve index settings for the given list of v1 templates, templates are apply in reverse
     * order since they should be provided in order of priority/order
     */
    public static Settings resolveSettings(final List<IndexTemplateMetadata> templates) {
        Settings.Builder templateSettings = Settings.builder();
        // apply templates, here, in reverse order, since first ones are better matching
        for (int i = templates.size() - 1; i >= 0; i--) {
            templateSettings.put(templates.get(i).settings());
        }
        return templateSettings.build();
    }

    /**
     * Resolve the given v2 template into a collected {@link Settings} object
     */
    public static Settings resolveSettings(final Metadata metadata, final String templateName) {
        final ComposableIndexTemplate template = metadata.templatesV2().get(templateName);
        assert template != null : "attempted to resolve settings for a template [" + templateName +
            "] that did not exist in the cluster state";
        if (template == null) {
            return Settings.EMPTY;
        }
        return resolveSettings(metadata, template);
    }

    private static Settings resolveSettings(Metadata metadata, ComposableIndexTemplate template) {
        final Map<String, ComponentTemplate> componentTemplates = metadata.componentTemplates();
        List<Settings> componentSettings = template.composedOf().stream()
            .map(componentTemplates::get)
            .filter(Objects::nonNull)
            .map(ComponentTemplate::template)
            .map(Template::settings)
            .filter(Objects::nonNull)
            .collect(Collectors.toList());

        Settings.Builder templateSettings = Settings.builder();
        componentSettings.forEach(templateSettings::put);
        // Add the actual index template's settings to the end, since it takes the highest precedence.
        Optional.ofNullable(template.template())
            .map(Template::settings)
            .ifPresent(templateSettings::put);
        return templateSettings.build();
    }

    /**
     * Resolve the given v1 templates into an ordered list of aliases
     */
    public static List<Map<String, AliasMetadata>> resolveAliases(final List<IndexTemplateMetadata> templates) {
        final List<Map<String, AliasMetadata>> resolvedAliases = new ArrayList<>();
        templates.forEach(template -> {
            if (template.aliases() != null) {
                Map<String, AliasMetadata> aliasMeta = new HashMap<>();
                for (ObjectObjectCursor<String, AliasMetadata> cursor : template.aliases()) {
                    aliasMeta.put(cursor.key, cursor.value);
                }
                resolvedAliases.add(aliasMeta);
            }
        });
        return Collections.unmodifiableList(resolvedAliases);
    }

    /**
     * Resolve the given v2 template into an ordered list of aliases
     */
    public static List<Map<String, AliasMetadata>> resolveAliases(final Metadata metadata, final String templateName) {
        final ComposableIndexTemplate template = metadata.templatesV2().get(templateName);
        assert template != null : "attempted to resolve aliases for a template [" + templateName +
            "] that did not exist in the cluster state";
        if (template == null) {
            return Collections.emptyList();
        }
        final Map<String, ComponentTemplate> componentTemplates = metadata.componentTemplates();
        List<Map<String, AliasMetadata>> aliases = template.composedOf().stream()
            .map(componentTemplates::get)
            .filter(Objects::nonNull)
            .map(ComponentTemplate::template)
            .map(Template::aliases)
            .filter(Objects::nonNull)
            .collect(Collectors.toList());

        // Add the actual index template's aliases to the end if they exist
        Optional.ofNullable(template.template())
            .map(Template::aliases)
            .ifPresent(aliases::add);
        // Aliases are applied in order, but subsequent alias configuration from the same name is
        // ignored, so in order for the order to be correct, alias configuration should be in order
        // of precedence (with the index template first)
        Collections.reverse(aliases);
        return Collections.unmodifiableList(aliases);
    }

    /**
     * Given a state and a composable template, validate that the final composite template
     * generated by the composable template and all of its component templates contains valid
     * settings, mappings, and aliases.
     */
    private static void validateCompositeTemplate(final ClusterState state,
                                                  final String templateName,
                                                  final ComposableIndexTemplate template,
                                                  final IndicesService indicesService,
                                                  final NamedXContentRegistry xContentRegistry) throws Exception {
        final ClusterState stateWithTemplate = ClusterState.builder(state)
            .metadata(Metadata.builder(state.metadata()).put(templateName, template))
            .build();

        final String temporaryIndexName = "validate-template-" + UUIDs.randomBase64UUID().toLowerCase(Locale.ROOT);
        Settings resolvedSettings = resolveSettings(stateWithTemplate.metadata(), templateName);

        // use the provided values, otherwise just pick valid dummy values
        int dummyPartitionSize = IndexMetadata.INDEX_ROUTING_PARTITION_SIZE_SETTING.get(resolvedSettings);
        int dummyShards = resolvedSettings.getAsInt(IndexMetadata.SETTING_NUMBER_OF_SHARDS,
            dummyPartitionSize == 1 ? 1 : dummyPartitionSize + 1);
        int shardReplicas = resolvedSettings.getAsInt(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0);


        // Create the final aggregate settings, which will be used to create the temporary index metadata to validate everything
        Settings finalResolvedSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(resolvedSettings)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, dummyShards)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, shardReplicas)
            .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
            .build();

        // Validate index metadata (settings)
        final ClusterState stateWithIndex = ClusterState.builder(stateWithTemplate)
            .metadata(Metadata.builder(stateWithTemplate.metadata())
                .put(IndexMetadata.builder(temporaryIndexName).settings(finalResolvedSettings))
                .build())
            .build();
        final IndexMetadata tmpIndexMetadata = stateWithIndex.metadata().index(temporaryIndexName);
        indicesService.withTempIndexService(tmpIndexMetadata,
            tempIndexService -> {
                // Validate aliases
                MetadataCreateIndexService.resolveAndValidateAliases(temporaryIndexName, Collections.emptySet(),
                    MetadataIndexTemplateService.resolveAliases(stateWithIndex.metadata(), templateName), stateWithIndex.metadata(),
                    new AliasValidator(),
                    // the context is only used for validation so it's fine to pass fake values for the
                    // shard id and the current timestamp
                    xContentRegistry, tempIndexService.newQueryShardContext(0, null, () -> 0L, null));

                // triggers inclusion of _timestamp field and its validation:
                String indexName = DataStream.BACKING_INDEX_PREFIX + temporaryIndexName;
                // Parse mappings to ensure they are valid after being composed

                List<CompressedXContent> mappings = collectMappings(stateWithIndex, templateName, indexName);
                try {
                    MapperService mapperService = tempIndexService.mapperService();
                    for (CompressedXContent mapping : mappings) {
                        mapperService.merge(MapperService.SINGLE_MAPPING_NAME, mapping, MergeReason.INDEX_TEMPLATE);
                    }

                    if (template.getDataStreamTemplate() != null) {
                        validateTimestampFieldMapping(mapperService);
                    }
                } catch (Exception e) {
                    throw new IllegalArgumentException("invalid composite mappings for [" + templateName + "]", e);
                }
                return null;
            });
    }

    private static void validateTemplate(Settings validateSettings, String mappings,
                                         IndicesService indicesService, NamedXContentRegistry xContentRegistry) throws Exception {
        validateTemplate(validateSettings, Collections.singletonMap(MapperService.SINGLE_MAPPING_NAME, mappings),
            indicesService, xContentRegistry);
    }

    private static void validateTemplate(Settings validateSettings, Map<String, String> mappings,
                                         IndicesService indicesService, NamedXContentRegistry xContentRegistry) throws Exception {
        // First check to see if mappings are valid XContent
        Map<String, Map<String, Object>> mappingsForValidation = new HashMap<>();
        if (mappings != null) {
            for (Map.Entry<String, String> entry : mappings.entrySet()) {
                if (entry.getValue() != null) {
                    try {
                        new CompressedXContent(entry.getValue());
                        mappingsForValidation.put(entry.getKey(), MapperService.parseMapping(xContentRegistry, entry.getValue()));
                    } catch (Exception e) {
                        throw new MapperParsingException("Failed to parse mapping [{}]: {}", e, entry.getKey(), e.getMessage());
                    }
                }
            }
        }

        // Hard to validate settings if they're non-existent, so used empty ones if none were provided
        Settings settings = validateSettings;
        if (settings == null) {
            settings = Settings.EMPTY;
        }

        Index createdIndex = null;
        final String temporaryIndexName = UUIDs.randomBase64UUID();
        try {
            // use the provided values, otherwise just pick valid dummy values
            int dummyPartitionSize = IndexMetadata.INDEX_ROUTING_PARTITION_SIZE_SETTING.get(settings);
            int dummyShards = settings.getAsInt(IndexMetadata.SETTING_NUMBER_OF_SHARDS,
                    dummyPartitionSize == 1 ? 1 : dummyPartitionSize + 1);
            int shardReplicas = settings.getAsInt(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0);


            //create index service for parsing and validating "mappings"
            Settings dummySettings = Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(settings)
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, dummyShards)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, shardReplicas)
                .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
                .build();

            final IndexMetadata tmpIndexMetadata = IndexMetadata.builder(temporaryIndexName).settings(dummySettings).build();
            IndexService dummyIndexService = indicesService.createIndex(tmpIndexMetadata, Collections.emptyList(), false);
            createdIndex = dummyIndexService.index();

            dummyIndexService.mapperService().merge(mappingsForValidation, MergeReason.MAPPING_UPDATE);

        } finally {
            if (createdIndex != null) {
                indicesService.removeIndex(createdIndex, NO_LONGER_ASSIGNED, " created for parsing template mapping");
            }
        }
    }

    private void validate(String name, ComponentTemplate template) {
        validate(name, template.template(), Collections.emptyList());
    }

    private void validate(String name, ComposableIndexTemplate template) {
        validate(name, template.template(), template.indexPatterns());
    }

    private void validate(String name, Template template, List<String> indexPatterns) {
        Optional<Template> maybeTemplate = Optional.ofNullable(template);
        validate(name,
            maybeTemplate.map(Template::settings).orElse(Settings.EMPTY),
            indexPatterns,
            maybeTemplate.map(Template::aliases)
                .orElse(Collections.emptyMap())
                .values().stream()
                .map(MetadataIndexTemplateService::toAlias)
                .collect(Collectors.toList()));
    }

    private static Alias toAlias(AliasMetadata aliasMeta) {
        Alias a = new Alias(aliasMeta.alias());
        if (aliasMeta.filter() != null) {
            a.filter(aliasMeta.filter().string());
        }
        a.searchRouting(aliasMeta.searchRouting());
        a.indexRouting(aliasMeta.indexRouting());
        a.isHidden(aliasMeta.isHidden());
        a.writeIndex(aliasMeta.writeIndex());
        return a;
    }

    private void validate(PutRequest putRequest) {
        validate(putRequest.name, putRequest.settings, putRequest.indexPatterns, putRequest.aliases);
    }

    private void validate(String name, @Nullable Settings settings, List<String> indexPatterns, List<Alias> aliases) {
        List<String> validationErrors = new ArrayList<>();
        if (name.contains(" ")) {
            validationErrors.add("name must not contain a space");
        }
        if (name.contains(",")) {
            validationErrors.add("name must not contain a ','");
        }
        if (name.contains("#")) {
            validationErrors.add("name must not contain a '#'");
        }
        if (name.contains("*")) {
            validationErrors.add("name must not contain a '*'");
        }
        if (name.startsWith("_")) {
            validationErrors.add("name must not start with '_'");
        }
        if (name.toLowerCase(Locale.ROOT).equals(name) == false) {
            validationErrors.add("name must be lower cased");
        }
        for(String indexPattern : indexPatterns) {
            if (indexPattern.contains(" ")) {
                validationErrors.add("index_patterns [" + indexPattern + "] must not contain a space");
            }
            if (indexPattern.contains(",")) {
                validationErrors.add("index_pattern [" + indexPattern + "] must not contain a ','");
            }
            if (indexPattern.contains("#")) {
                validationErrors.add("index_pattern [" + indexPattern + "] must not contain a '#'");
            }
            if (indexPattern.contains(":")) {
                validationErrors.add("index_pattern [" + indexPattern + "] must not contain a ':'");
            }
            if (indexPattern.startsWith("_")) {
                validationErrors.add("index_pattern [" + indexPattern + "] must not start with '_'");
            }
            if (Strings.validFileNameExcludingAstrix(indexPattern) == false) {
                validationErrors.add("index_pattern [" + indexPattern + "] must not contain the following characters " +
                    Strings.INVALID_FILENAME_CHARS);
            }
        }


        if (settings != null) {
            try {
                // templates must be consistent with regards to dependencies
                indexScopedSettings.validate(settings, true);
            } catch (IllegalArgumentException iae) {
                validationErrors.add(iae.getMessage());
                for (Throwable t : iae.getSuppressed()) {
                    validationErrors.add(t.getMessage());
                }
            }
            List<String> indexSettingsValidation = metadataCreateIndexService.getIndexSettingsValidationErrors(settings, true);
            validationErrors.addAll(indexSettingsValidation);
        }

        if (indexPatterns.stream().anyMatch(Regex::isMatchAllPattern)) {
            if (settings != null && IndexMetadata.INDEX_HIDDEN_SETTING.exists(settings)) {
                validationErrors.add("global templates may not specify the setting " + IndexMetadata.INDEX_HIDDEN_SETTING.getKey());
            }
        }

        if (validationErrors.size() > 0) {
            ValidationException validationException = new ValidationException();
            validationException.addValidationErrors(validationErrors);
            throw new InvalidIndexTemplateException(name, validationException.getMessage());
        }

        for (Alias alias : aliases) {
            // we validate the alias only partially, as we don't know yet to which index it'll get applied to
            aliasValidator.validateAliasStandalone(alias);
            if (indexPatterns.contains(alias.name())) {
                throw new IllegalArgumentException("alias [" + alias.name() +
                    "] cannot be the same as any pattern in [" + String.join(", ", indexPatterns) + "]");
            }
        }
    }

    public interface PutListener {

        void onResponse(PutResponse response);

        void onFailure(Exception e);
    }

    public static class PutRequest {
        final String name;
        final String cause;
        boolean create;
        int order;
        Integer version;
        List<String> indexPatterns;
        Settings settings = Settings.Builder.EMPTY_SETTINGS;
        Map<String, String> mappings = new HashMap<>();
        List<Alias> aliases = new ArrayList<>();

        TimeValue masterTimeout = MasterNodeRequest.DEFAULT_MASTER_NODE_TIMEOUT;

        public PutRequest(String cause, String name) {
            this.cause = cause;
            this.name = name;
        }

        public PutRequest order(int order) {
            this.order = order;
            return this;
        }

        public PutRequest patterns(List<String> indexPatterns) {
            this.indexPatterns = indexPatterns;
            return this;
        }

        public PutRequest create(boolean create) {
            this.create = create;
            return this;
        }

        public PutRequest settings(Settings settings) {
            this.settings = settings;
            return this;
        }

        public PutRequest mappings(Map<String, String> mappings) {
            this.mappings.putAll(mappings);
            return this;
        }

        public PutRequest aliases(Set<Alias> aliases) {
            this.aliases.addAll(aliases);
            return this;
        }

        public PutRequest putMapping(String mappingType, String mappingSource) {
            mappings.put(mappingType, mappingSource);
            return this;
        }

        public PutRequest masterTimeout(TimeValue masterTimeout) {
            this.masterTimeout = masterTimeout;
            return this;
        }

        public PutRequest version(Integer version) {
            this.version = version;
            return this;
        }
    }

    public static class PutResponse {
        private final boolean acknowledged;

        public PutResponse(boolean acknowledged) {
            this.acknowledged = acknowledged;
        }

        public boolean acknowledged() {
            return acknowledged;
        }
    }

    public static class RemoveRequest {
        final String name;
        TimeValue masterTimeout = MasterNodeRequest.DEFAULT_MASTER_NODE_TIMEOUT;

        public RemoveRequest(String name) {
            this.name = name;
        }

        public RemoveRequest masterTimeout(TimeValue masterTimeout) {
            this.masterTimeout = masterTimeout;
            return this;
        }
    }

    public static class RemoveResponse {
        private final boolean acknowledged;

        public RemoveResponse(boolean acknowledged) {
            this.acknowledged = acknowledged;
        }

        public boolean acknowledged() {
            return acknowledged;
        }
    }

    public interface RemoveListener {

        void onResponse(RemoveResponse response);

        void onFailure(Exception e);
    }
}

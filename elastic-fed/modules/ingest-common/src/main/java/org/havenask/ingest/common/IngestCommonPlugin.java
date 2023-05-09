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

package org.havenask.ingest.common;

import org.havenask.action.ActionRequest;
import org.havenask.action.ActionResponse;
import org.havenask.cluster.metadata.IndexNameExpressionResolver;
import org.havenask.cluster.node.DiscoveryNodes;
import org.havenask.common.settings.ClusterSettings;
import org.havenask.common.settings.IndexScopedSettings;
import org.havenask.common.settings.Setting;
import org.havenask.common.settings.Settings;
import org.havenask.common.settings.SettingsFilter;
import org.havenask.common.unit.TimeValue;
import org.havenask.grok.Grok;
import org.havenask.grok.MatcherWatchdog;
import org.havenask.ingest.DropProcessor;
import org.havenask.ingest.PipelineProcessor;
import org.havenask.ingest.Processor;
import org.havenask.plugins.ActionPlugin;
import org.havenask.plugins.IngestPlugin;
import org.havenask.plugins.Plugin;
import org.havenask.rest.RestController;
import org.havenask.rest.RestHandler;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class IngestCommonPlugin extends Plugin implements ActionPlugin, IngestPlugin {

    static final Setting<TimeValue> WATCHDOG_INTERVAL =
        Setting.timeSetting("ingest.grok.watchdog.interval", TimeValue.timeValueSeconds(1), Setting.Property.NodeScope);
    static final Setting<TimeValue> WATCHDOG_MAX_EXECUTION_TIME =
        Setting.timeSetting("ingest.grok.watchdog.max_execution_time", TimeValue.timeValueSeconds(1), Setting.Property.NodeScope);

    public IngestCommonPlugin() {
    }

    @Override
    public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
        Map<String, Processor.Factory> processors = new HashMap<>();
        processors.put(DateProcessor.TYPE, new DateProcessor.Factory(parameters.scriptService));
        processors.put(SetProcessor.TYPE, new SetProcessor.Factory(parameters.scriptService));
        processors.put(AppendProcessor.TYPE, new AppendProcessor.Factory(parameters.scriptService));
        processors.put(RenameProcessor.TYPE, new RenameProcessor.Factory(parameters.scriptService));
        processors.put(RemoveProcessor.TYPE, new RemoveProcessor.Factory(parameters.scriptService));
        processors.put(SplitProcessor.TYPE, new SplitProcessor.Factory());
        processors.put(JoinProcessor.TYPE, new JoinProcessor.Factory());
        processors.put(UppercaseProcessor.TYPE, new UppercaseProcessor.Factory());
        processors.put(LowercaseProcessor.TYPE, new LowercaseProcessor.Factory());
        processors.put(TrimProcessor.TYPE, new TrimProcessor.Factory());
        processors.put(ConvertProcessor.TYPE, new ConvertProcessor.Factory());
        processors.put(GsubProcessor.TYPE, new GsubProcessor.Factory());
        processors.put(FailProcessor.TYPE, new FailProcessor.Factory(parameters.scriptService));
        processors.put(ForEachProcessor.TYPE, new ForEachProcessor.Factory(parameters.scriptService));
        processors.put(DateIndexNameProcessor.TYPE, new DateIndexNameProcessor.Factory(parameters.scriptService));
        processors.put(SortProcessor.TYPE, new SortProcessor.Factory());
        processors.put(GrokProcessor.TYPE, new GrokProcessor.Factory(Grok.BUILTIN_PATTERNS, createGrokThreadWatchdog(parameters)));
        processors.put(ScriptProcessor.TYPE, new ScriptProcessor.Factory(parameters.scriptService));
        processors.put(DotExpanderProcessor.TYPE, new DotExpanderProcessor.Factory());
        processors.put(JsonProcessor.TYPE, new JsonProcessor.Factory());
        processors.put(KeyValueProcessor.TYPE, new KeyValueProcessor.Factory());
        processors.put(URLDecodeProcessor.TYPE, new URLDecodeProcessor.Factory());
        processors.put(BytesProcessor.TYPE, new BytesProcessor.Factory());
        processors.put(PipelineProcessor.TYPE, new PipelineProcessor.Factory(parameters.ingestService));
        processors.put(DissectProcessor.TYPE, new DissectProcessor.Factory());
        processors.put(DropProcessor.TYPE, new DropProcessor.Factory());
        processors.put(HtmlStripProcessor.TYPE, new HtmlStripProcessor.Factory());
        processors.put(CsvProcessor.TYPE, new CsvProcessor.Factory());
        return Collections.unmodifiableMap(processors);
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return Arrays.asList(new ActionHandler<>(GrokProcessorGetAction.INSTANCE, GrokProcessorGetAction.TransportAction.class));
    }

    @Override
    public List<RestHandler> getRestHandlers(Settings settings, RestController restController, ClusterSettings clusterSettings,
                                             IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter,
                                             IndexNameExpressionResolver indexNameExpressionResolver,
                                             Supplier<DiscoveryNodes> nodesInCluster) {
        return Collections.singletonList(new GrokProcessorGetAction.RestAction());
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(WATCHDOG_INTERVAL, WATCHDOG_MAX_EXECUTION_TIME);
    }

    private static MatcherWatchdog createGrokThreadWatchdog(Processor.Parameters parameters) {
        long intervalMillis = WATCHDOG_INTERVAL.get(parameters.env.settings()).getMillis();
        long maxExecutionTimeMillis = WATCHDOG_MAX_EXECUTION_TIME.get(parameters.env.settings()).getMillis();
        return MatcherWatchdog.newInstance(intervalMillis, maxExecutionTimeMillis,
            parameters.relativeTimeSupplier, parameters.scheduler::apply);
    }

}

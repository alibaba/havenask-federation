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

import org.havenask.common.Nullable;
import org.havenask.common.bytes.BytesReference;
import org.havenask.common.logging.DeprecationLogger;
import org.havenask.common.util.CollectionUtils;
import org.havenask.common.xcontent.LoggingDeprecationHandler;
import org.havenask.common.xcontent.NamedXContentRegistry;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.common.xcontent.XContentParser;
import org.havenask.common.xcontent.XContentType;
import org.havenask.common.xcontent.json.JsonXContent;
import org.havenask.ingest.AbstractProcessor;
import org.havenask.ingest.IngestDocument;
import org.havenask.ingest.Processor;
import org.havenask.script.DynamicMap;
import org.havenask.script.IngestScript;
import org.havenask.script.Script;
import org.havenask.script.ScriptException;
import org.havenask.script.ScriptService;
import org.havenask.script.ScriptType;

import java.io.InputStream;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;

import static org.havenask.ingest.ConfigurationUtils.newConfigurationException;

/**
 * Processor that evaluates a script with an ingest document in its context.
 */
public final class ScriptProcessor extends AbstractProcessor {

    private static final DeprecationLogger deprecationLogger =
            DeprecationLogger.getLogger(DynamicMap.class);
    private static final Map<String, Function<Object, Object>> PARAMS_FUNCTIONS = org.havenask.common.collect.Map.of(
            "_type", value -> {
                deprecationLogger.deprecate("script_processor",
                        "[types removal] Looking up doc types [_type] in scripts is deprecated.");
                return value;
            });

    public static final String TYPE = "script";

    private final Script script;
    private final ScriptService scriptService;
    private final IngestScript precompiledIngestScript;

    /**
     * Processor that evaluates a script with an ingest document in its context
     *  @param tag The processor's tag.
     * @param description The processor's description.
     * @param script The {@link Script} to execute.
     * @param precompiledIngestScript The {@link Script} precompiled
     * @param scriptService The {@link ScriptService} used to execute the script.
     */
    ScriptProcessor(String tag, String description, Script script, @Nullable IngestScript precompiledIngestScript,
                    ScriptService scriptService) {
        super(tag, description);
        this.script = script;
        this.precompiledIngestScript = precompiledIngestScript;
        this.scriptService = scriptService;
    }

    /**
     * Executes the script with the Ingest document in context.
     *
     * @param document The Ingest document passed into the script context under the "ctx" object.
     */
    @Override
    public IngestDocument execute(IngestDocument document) {
        final IngestScript ingestScript;
        if (precompiledIngestScript == null) {
            IngestScript.Factory factory = scriptService.compile(script, IngestScript.CONTEXT);
            ingestScript = factory.newInstance(script.getParams());
        } else {
            ingestScript = precompiledIngestScript;
        }
        ingestScript.execute(new DynamicMap(document.getSourceAndMetadata(), PARAMS_FUNCTIONS));
        CollectionUtils.ensureNoSelfReferences(document.getSourceAndMetadata(), "ingest script");
        return document;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    Script getScript() {
        return script;
    }

    IngestScript getPrecompiledIngestScript() {
        return precompiledIngestScript;
    }

    public static final class Factory implements Processor.Factory {
        private final ScriptService scriptService;

        public Factory(ScriptService scriptService) {
            this.scriptService = scriptService;
        }

        @Override
        public ScriptProcessor create(Map<String, Processor.Factory> registry, String processorTag,
                                      String description, Map<String, Object> config) throws Exception {
            try (XContentBuilder builder = XContentBuilder.builder(JsonXContent.jsonXContent).map(config);
                 InputStream stream = BytesReference.bytes(builder).streamInput();
                 XContentParser parser = XContentType.JSON.xContent().createParser(NamedXContentRegistry.EMPTY,
                     LoggingDeprecationHandler.INSTANCE, stream)) {
                Script script = Script.parse(parser);

                Arrays.asList("id", "source", "inline", "lang", "params", "options").forEach(config::remove);

                // verify script is able to be compiled before successfully creating processor.
                IngestScript ingestScript = null;
                try {
                    final IngestScript.Factory factory = scriptService.compile(script, IngestScript.CONTEXT);
                    if (ScriptType.INLINE.equals(script.getType())) {
                        ingestScript = factory.newInstance(script.getParams());
                    }
                } catch (ScriptException e) {
                    throw newConfigurationException(TYPE, processorTag, null, e);
                }
                return new ScriptProcessor(processorTag, description, script, ingestScript, scriptService);
            }
        }
    }
}

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

import org.havenask.ingest.AbstractProcessor;
import org.havenask.ingest.ConfigurationUtils;
import org.havenask.ingest.IngestDocument;
import org.havenask.ingest.Processor;
import org.havenask.ingest.WrappingProcessor;
import org.havenask.script.ScriptService;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

import static org.havenask.ingest.ConfigurationUtils.newConfigurationException;
import static org.havenask.ingest.ConfigurationUtils.readBooleanProperty;
import static org.havenask.ingest.ConfigurationUtils.readMap;
import static org.havenask.ingest.ConfigurationUtils.readStringProperty;

/**
 * A processor that for each value in a list executes a one or more processors.
 *
 * This can be useful in cases to do string operations on json array of strings,
 * or remove a field from objects inside a json array.
 *
 * Note that this processor is experimental.
 */
public final class ForEachProcessor extends AbstractProcessor implements WrappingProcessor {

    public static final String TYPE = "foreach";

    private final String field;
    private final Processor processor;
    private final boolean ignoreMissing;

    ForEachProcessor(String tag, String description, String field, Processor processor, boolean ignoreMissing) {
        super(tag, description);
        this.field = field;
        this.processor = processor;
        this.ignoreMissing = ignoreMissing;
    }

    boolean isIgnoreMissing() {
        return ignoreMissing;
    }

    @Override
    public void execute(IngestDocument ingestDocument, BiConsumer<IngestDocument, Exception> handler) {
        List<?> values = ingestDocument.getFieldValue(field, List.class, ignoreMissing);
        if (values == null) {
            if (ignoreMissing) {
                handler.accept(ingestDocument, null);
            } else {
                handler.accept(null, new IllegalArgumentException("field [" + field + "] is null, cannot loop over its elements."));
            }
        } else {
            innerExecute(0, new ArrayList<>(values), new ArrayList<>(values.size()), ingestDocument, handler);
        }
    }

    void innerExecute(int index, List<?> values, List<Object> newValues, IngestDocument document,
                      BiConsumer<IngestDocument, Exception> handler) {
        for (; index < values.size(); index++) {
            AtomicBoolean shouldContinueHere = new AtomicBoolean();
            Object value = values.get(index);
            Object previousValue = document.getIngestMetadata().put("_value", value);
            int nextIndex = index + 1;
            processor.execute(document, (result, e) -> {
                newValues.add(document.getIngestMetadata().put("_value", previousValue));
                if (e != null || result == null) {
                    handler.accept(result, e);
                } else if (shouldContinueHere.getAndSet(true)) {
                    innerExecute(nextIndex, values, newValues, document, handler);
                }
            });

            if (shouldContinueHere.getAndSet(true) == false) {
                return;
            }
        }

        if (index == values.size()) {
            document.setFieldValue(field, new ArrayList<>(newValues));
            handler.accept(document, null);
        }
    }

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
        throw new UnsupportedOperationException("this method should not get executed");
    }

    @Override
    public String getType() {
        return TYPE;
    }

    String getField() {
        return field;
    }

    public Processor getInnerProcessor() {
        return processor;
    }

    public static final class Factory implements Processor.Factory {

        private final ScriptService scriptService;

        Factory(ScriptService scriptService) {
            this.scriptService = scriptService;
        }

        @Override
        public ForEachProcessor create(Map<String, Processor.Factory> factories, String tag,
                                       String description, Map<String, Object> config) throws Exception {
            String field = readStringProperty(TYPE, tag, config, "field");
            boolean ignoreMissing = readBooleanProperty(TYPE, tag, config, "ignore_missing", false);
            Map<String, Map<String, Object>> processorConfig = readMap(TYPE, tag, config, "processor");
            Set<Map.Entry<String, Map<String, Object>>> entries = processorConfig.entrySet();
            if (entries.size() != 1) {
                throw newConfigurationException(TYPE, tag, "processor", "Must specify exactly one processor type");
            }
            Map.Entry<String, Map<String, Object>> entry = entries.iterator().next();
            Processor processor =
                ConfigurationUtils.readProcessor(factories, scriptService, entry.getKey(), entry.getValue());
            return new ForEachProcessor(tag, description, field, processor, ignoreMissing);
        }
    }
}

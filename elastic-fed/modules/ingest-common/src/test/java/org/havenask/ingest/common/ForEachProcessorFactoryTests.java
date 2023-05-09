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

import org.havenask.HavenaskParseException;
import org.havenask.ingest.Processor;
import org.havenask.ingest.TestProcessor;
import org.havenask.script.ScriptService;
import org.havenask.test.HavenaskTestCase;
import org.hamcrest.Matchers;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

public class ForEachProcessorFactoryTests extends HavenaskTestCase {

    private final ScriptService scriptService = mock(ScriptService.class);
    private final Consumer<Runnable> genericExecutor = Runnable::run;

    public void testCreate() throws Exception {
        Processor processor = new TestProcessor(ingestDocument -> { });
        Map<String, Processor.Factory> registry = new HashMap<>();
        registry.put("_name", (r, t, description, c) -> processor);
        ForEachProcessor.Factory forEachFactory = new ForEachProcessor.Factory(scriptService);

        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("processor", Collections.singletonMap("_name", Collections.emptyMap()));
        ForEachProcessor forEachProcessor = forEachFactory.create(registry, null, null, config);
        assertThat(forEachProcessor, Matchers.notNullValue());
        assertThat(forEachProcessor.getField(), equalTo("_field"));
        assertThat(forEachProcessor.getInnerProcessor(), Matchers.sameInstance(processor));
        assertFalse(forEachProcessor.isIgnoreMissing());
    }

    public void testSetIgnoreMissing() throws Exception {
        Processor processor = new TestProcessor(ingestDocument -> { });
        Map<String, Processor.Factory> registry = new HashMap<>();
        registry.put("_name", (r, t, description, c) -> processor);
        ForEachProcessor.Factory forEachFactory = new ForEachProcessor.Factory(scriptService);

        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("processor", Collections.singletonMap("_name", Collections.emptyMap()));
        config.put("ignore_missing", true);
        ForEachProcessor forEachProcessor = forEachFactory.create(registry, null, null, config);
        assertThat(forEachProcessor, Matchers.notNullValue());
        assertThat(forEachProcessor.getField(), equalTo("_field"));
        assertThat(forEachProcessor.getInnerProcessor(), Matchers.sameInstance(processor));
        assertTrue(forEachProcessor.isIgnoreMissing());
    }

    public void testCreateWithTooManyProcessorTypes() throws Exception {
        Processor processor = new TestProcessor(ingestDocument -> { });
        Map<String, Processor.Factory> registry = new HashMap<>();
        registry.put("_first", (r, t, description, c) -> processor);
        registry.put("_second", (r, t, description, c) -> processor);
        ForEachProcessor.Factory forEachFactory = new ForEachProcessor.Factory(scriptService);

        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        Map<String, Object> processorTypes = new HashMap<>();
        processorTypes.put("_first", Collections.emptyMap());
        processorTypes.put("_second", Collections.emptyMap());
        config.put("processor", processorTypes);
        Exception exception = expectThrows(HavenaskParseException.class, () -> forEachFactory.create(registry, null, null, config));
        assertThat(exception.getMessage(), equalTo("[processor] Must specify exactly one processor type"));
    }

    public void testCreateWithNonExistingProcessorType() throws Exception {
        ForEachProcessor.Factory forEachFactory = new ForEachProcessor.Factory(scriptService);
        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("processor", Collections.singletonMap("_name", Collections.emptyMap()));
        Exception expectedException = expectThrows(HavenaskParseException.class,
            () -> forEachFactory.create(Collections.emptyMap(), null, null, config));
        assertThat(expectedException.getMessage(), equalTo("No processor type exists with name [_name]"));
    }

    public void testCreateWithMissingField() throws Exception {
        Processor processor = new TestProcessor(ingestDocument -> { });
        Map<String, Processor.Factory> registry = new HashMap<>();
        registry.put("_name", (r, t, description, c) -> processor);
        ForEachProcessor.Factory forEachFactory = new ForEachProcessor.Factory(scriptService);
        Map<String, Object> config = new HashMap<>();
        config.put("processor", Collections.singletonList(Collections.singletonMap("_name", Collections.emptyMap())));
        Exception exception = expectThrows(Exception.class, () -> forEachFactory.create(registry, null, null, config));
        assertThat(exception.getMessage(), equalTo("[field] required property is missing"));
    }

    public void testCreateWithMissingProcessor() {
        ForEachProcessor.Factory forEachFactory = new ForEachProcessor.Factory(scriptService);
        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        Exception exception = expectThrows(Exception.class, () -> forEachFactory.create(Collections.emptyMap(), null, null, config));
        assertThat(exception.getMessage(), equalTo("[processor] required property is missing"));
    }

}

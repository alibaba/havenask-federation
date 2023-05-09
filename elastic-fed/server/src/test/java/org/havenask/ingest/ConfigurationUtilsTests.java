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

package org.havenask.ingest;

import org.havenask.HavenaskParseException;
import org.havenask.script.ScriptService;
import org.havenask.script.TemplateScript;
import org.havenask.test.HavenaskTestCase;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ConfigurationUtilsTests extends HavenaskTestCase {

    private final ScriptService scriptService = mock(ScriptService.class);

    private Map<String, Object> config;

    @Before
    public void setConfig() {
        config = new HashMap<>();
        config.put("foo", "bar");
        config.put("boolVal", true);
        config.put("null", null);
        config.put("arr", Arrays.asList("1", "2", "3"));
        List<Integer> list = new ArrayList<>();
        list.add(2);
        config.put("int", list);
        config.put("ip", "127.0.0.1");
        Map<String, Object> fizz = new HashMap<>();
        fizz.put("buzz", "hello world");
        config.put("fizz", fizz);
        config.put("num", 1);
    }

    public void testReadStringProperty() {
        String val = ConfigurationUtils.readStringProperty(null, null, config, "foo");
        assertThat(val, equalTo("bar"));
    }

    public void testReadStringPropertyInvalidType() {
        try {
            ConfigurationUtils.readStringProperty(null, null, config, "arr");
        } catch (HavenaskParseException e) {
            assertThat(e.getMessage(), equalTo("[arr] property isn't a string, but of type [java.util.Arrays$ArrayList]"));
        }
    }

    public void testReadBooleanProperty() {
        Boolean val = ConfigurationUtils.readBooleanProperty(null, null, config, "boolVal", false);
        assertThat(val, equalTo(true));
    }

    public void testReadNullBooleanProperty() {
        Boolean val = ConfigurationUtils.readBooleanProperty(null, null, config, "null", false);
        assertThat(val, equalTo(false));
    }

    public void testReadBooleanPropertyInvalidType() {
        try {
            ConfigurationUtils.readBooleanProperty(null, null, config, "arr", true);
        } catch (HavenaskParseException e) {
            assertThat(e.getMessage(), equalTo("[arr] property isn't a boolean, but of type [java.util.Arrays$ArrayList]"));
        }
    }

    public void testReadStringOrIntProperty() {
        String val1 = ConfigurationUtils.readStringOrIntProperty(null, null, config, "foo", null);
        String val2 = ConfigurationUtils.readStringOrIntProperty(null, null, config, "num", null);
        assertThat(val1, equalTo("bar"));
        assertThat(val2, equalTo("1"));
    }

    public void testReadStringOrIntPropertyInvalidType() {
        try {
            ConfigurationUtils.readStringOrIntProperty(null, null, config, "arr", null);
        } catch (HavenaskParseException e) {
            assertThat(e.getMessage(), equalTo(
                "[arr] property isn't a string or int, but of type [java.util.Arrays$ArrayList]"));
        }
    }

    public void testReadProcessors() throws Exception {
        Processor processor = mock(Processor.class);
        Map<String, Processor.Factory> registry =
            Collections.singletonMap("test_processor", (factories, tag, description, config) -> processor);

        List<Map<String, Object>> config = new ArrayList<>();
        Map<String, Object> emptyConfig = Collections.emptyMap();
        config.add(Collections.singletonMap("test_processor", emptyConfig));
        config.add(Collections.singletonMap("test_processor", emptyConfig));

        List<Processor> result = ConfigurationUtils.readProcessorConfigs(config, scriptService, registry);
        assertThat(result.size(), equalTo(2));
        assertThat(result.get(0), sameInstance(processor));
        assertThat(result.get(1), sameInstance(processor));

        Map<String, Object> unknownTaggedConfig = new HashMap<>();
        unknownTaggedConfig.put("tag", "my_unknown");
        if (randomBoolean()) {
            unknownTaggedConfig.put("description", "my_description");
        }
        config.add(Collections.singletonMap("unknown_processor", unknownTaggedConfig));
        HavenaskParseException e = expectThrows(HavenaskParseException.class,
            () -> ConfigurationUtils.readProcessorConfigs(config, scriptService, registry));
        assertThat(e.getMessage(), equalTo("No processor type exists with name [unknown_processor]"));
        assertThat(e.getMetadata("havenask.processor_tag"), equalTo(Collections.singletonList("my_unknown")));
        assertThat(e.getMetadata("havenask.processor_type"), equalTo(Collections.singletonList("unknown_processor")));
        assertThat(e.getMetadata("havenask.property_name"), is(nullValue()));
        assertThat(e.getMetadata("havenask.processor_description"), is(nullValue()));

        List<Map<String, Object>> config2 = new ArrayList<>();
        unknownTaggedConfig = new HashMap<>();
        unknownTaggedConfig.put("tag", "my_unknown");
        config2.add(Collections.singletonMap("unknown_processor", unknownTaggedConfig));
        Map<String, Object> secondUnknownTaggedConfig = new HashMap<>();
        secondUnknownTaggedConfig.put("tag", "my_second_unknown");
        config2.add(Collections.singletonMap("second_unknown_processor", secondUnknownTaggedConfig));
        e = expectThrows(
            HavenaskParseException.class,
            () -> ConfigurationUtils.readProcessorConfigs(config2, scriptService, registry)
        );
        assertThat(e.getMessage(), equalTo("No processor type exists with name [unknown_processor]"));
        assertThat(e.getMetadata("havenask.processor_tag"), equalTo(Collections.singletonList("my_unknown")));
        assertThat(e.getMetadata("havenask.processor_type"), equalTo(Collections.singletonList("unknown_processor")));
        assertThat(e.getMetadata("havenask.property_name"), is(nullValue()));

        assertThat(e.getSuppressed().length, equalTo(1));
        assertThat(e.getSuppressed()[0], instanceOf(HavenaskParseException.class));
        HavenaskParseException e2 = (HavenaskParseException) e.getSuppressed()[0];
        assertThat(e2.getMessage(), equalTo("No processor type exists with name [second_unknown_processor]"));
        assertThat(e2.getMetadata("havenask.processor_tag"), equalTo(Collections.singletonList("my_second_unknown")));
        assertThat(e2.getMetadata("havenask.processor_type"), equalTo(Collections.singletonList("second_unknown_processor")));
        assertThat(e2.getMetadata("havenask.property_name"), is(nullValue()));
    }

    public void testReadProcessorNullDescription() throws Exception {
        Processor processor = new TestProcessor("tag", "type", null, (ingestDocument) -> {});
        Map<String, Processor.Factory> registry =
            Collections.singletonMap("test_processor", (factories, tag, description, config) -> {
                assertNull(description);
                return processor;
            });

        List<Map<String, Object>> config = new ArrayList<>();
        Map<String, Object> emptyConfig = Collections.emptyMap();
        config.add(Collections.singletonMap("test_processor", emptyConfig));
        List<Processor> result = ConfigurationUtils.readProcessorConfigs(config, scriptService, registry);
        assertThat(result.size(), equalTo(1));
        assertThat(result.get(0), sameInstance(processor));
    }

    public void testReadProcessorDescription() throws Exception {
        String testDescription = randomAlphaOfLengthBetween(10, 20);
        Processor processor = new TestProcessor("tag", "type", testDescription, (ingestDocument) -> {});
        Map<String, Processor.Factory> registry =
            Collections.singletonMap("test_processor", (factories, tag, description, config) -> {
                assertThat(description, equalTo(processor.getDescription()));
                return processor;
            });

        List<Map<String, Object>> config = new ArrayList<>();
        Map<String, Object> processorConfig = new HashMap<>();
        processorConfig.put(ConfigurationUtils.DESCRIPTION_KEY, testDescription);
        config.add(Collections.singletonMap("test_processor", processorConfig));
        List<Processor> result = ConfigurationUtils.readProcessorConfigs(config, scriptService, registry);
        assertThat(result.size(), equalTo(1));
        assertThat(result.get(0), sameInstance(processor));
    }

    public void testReadProcessorFromObjectOrMap() throws Exception {
        Processor processor = mock(Processor.class);
        Map<String, Processor.Factory> registry =
            Collections.singletonMap("script", (processorFactories, tag, description, config) -> {
                config.clear();
                return processor;
            });

        Object emptyConfig = Collections.emptyMap();
        Processor processor1 = ConfigurationUtils.readProcessor(registry, scriptService, "script", emptyConfig);
        assertThat(processor1, sameInstance(processor));

        Object inlineScript = "test_script";
        Processor processor2 = ConfigurationUtils.readProcessor(registry, scriptService, "script", inlineScript);
        assertThat(processor2, sameInstance(processor));

        Object invalidConfig = 12L;

        HavenaskParseException ex = expectThrows(HavenaskParseException.class,
            () -> ConfigurationUtils.readProcessor(registry, scriptService, "unknown_processor", invalidConfig));
        assertThat(ex.getMessage(), equalTo("property isn't a map, but of type [" + invalidConfig.getClass().getName() + "]"));
    }

    public void testNoScriptCompilation() {
        ScriptService scriptService = mock(ScriptService.class);
        when(scriptService.isLangSupported(anyString())).thenReturn(true);
        String propertyValue = randomAlphaOfLength(10);
        TemplateScript.Factory result;
        result = ConfigurationUtils.compileTemplate(randomAlphaOfLength(10), randomAlphaOfLength(10), randomAlphaOfLength(10),
            propertyValue, scriptService);
        assertThat(result.newInstance(null).execute(), equalTo(propertyValue));
        verify(scriptService, times(0)).compile(any(), any());
    }

    public void testScriptShouldCompile() {
        ScriptService scriptService = mock(ScriptService.class);
        when(scriptService.isLangSupported(anyString())).thenReturn(true);
        String propertyValue = "{{" + randomAlphaOfLength(10) + "}}";
        String compiledValue = randomAlphaOfLength(10);
        when(scriptService.compile(any(), any())).thenReturn(new TestTemplateService.MockTemplateScript.Factory(compiledValue));
        TemplateScript.Factory result;
        result = ConfigurationUtils.compileTemplate(randomAlphaOfLength(10), randomAlphaOfLength(10), randomAlphaOfLength(10),
            propertyValue, scriptService);
        assertThat(result.newInstance(null).execute(), equalTo(compiledValue));
        verify(scriptService, times(1)).compile(any(), any());
    }
}

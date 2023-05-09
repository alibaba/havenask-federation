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
 *         http://www.apache.org/licenses/LICENSE-2.0
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

import org.havenask.action.ActionRequestValidationException;
import org.havenask.action.admin.indices.template.put.PutComposableIndexTemplateAction;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.cluster.metadata.ComposableIndexTemplate;
import org.havenask.cluster.metadata.ComposableIndexTemplateTests;
import org.havenask.cluster.metadata.Template;
import org.havenask.common.io.stream.Writeable;
import org.havenask.common.settings.Settings;
import org.havenask.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class SimulateIndexTemplateRequestTests extends AbstractWireSerializingTestCase<SimulateIndexTemplateRequest> {

    @Override
    protected Writeable.Reader<SimulateIndexTemplateRequest> instanceReader() {
        return SimulateIndexTemplateRequest::new;
    }

    @Override
    protected SimulateIndexTemplateRequest createTestInstance() {
        SimulateIndexTemplateRequest req = new SimulateIndexTemplateRequest(randomAlphaOfLength(10));
        PutComposableIndexTemplateAction.Request newTemplateRequest = new PutComposableIndexTemplateAction.Request(randomAlphaOfLength(4));
        newTemplateRequest.indexTemplate(ComposableIndexTemplateTests.randomInstance());
        req.indexTemplateRequest(newTemplateRequest);
        return req;
    }

    @Override
    protected SimulateIndexTemplateRequest mutateInstance(SimulateIndexTemplateRequest instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    public void testIndexNameCannotBeNullOrEmpty() {
        expectThrows(IllegalArgumentException.class, () -> new SimulateIndexTemplateRequest((String) null));
        expectThrows(IllegalArgumentException.class, () -> new SimulateIndexTemplateRequest(""));
    }

    public void testAddingGlobalTemplateWithHiddenIndexSettingIsIllegal() {
        Template template = new Template(Settings.builder().put(IndexMetadata.SETTING_INDEX_HIDDEN, true).build(), null, null);
        ComposableIndexTemplate globalTemplate = new ComposableIndexTemplate(Collections.singletonList("*"),
            template, null, null, null, null, null);

        PutComposableIndexTemplateAction.Request request = new PutComposableIndexTemplateAction.Request("test");
        request.indexTemplate(globalTemplate);

        SimulateIndexTemplateRequest simulateRequest = new SimulateIndexTemplateRequest("testing");
        simulateRequest.indexTemplateRequest(request);

        ActionRequestValidationException validationException = simulateRequest.validate();
        assertThat(validationException, is(notNullValue()));
        List<String> validationErrors = validationException.validationErrors();
        assertThat(validationErrors.size(), is(1));
        String error = validationErrors.get(0);
        assertThat(error, is("global composable templates may not specify the setting " + IndexMetadata.SETTING_INDEX_HIDDEN));
    }
}

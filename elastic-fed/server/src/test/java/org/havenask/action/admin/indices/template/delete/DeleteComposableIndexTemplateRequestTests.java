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

package org.havenask.action.admin.indices.template.delete;

import org.havenask.common.io.stream.Writeable;
import org.havenask.test.AbstractWireSerializingTestCase;
import org.havenask.action.admin.indices.template.delete.DeleteComposableIndexTemplateAction;

import java.io.IOException;

public class DeleteComposableIndexTemplateRequestTests
    extends AbstractWireSerializingTestCase<DeleteComposableIndexTemplateAction.Request> {
    @Override
    protected Writeable.Reader<DeleteComposableIndexTemplateAction.Request> instanceReader() {
        return DeleteComposableIndexTemplateAction.Request::new;
    }

    @Override
    protected DeleteComposableIndexTemplateAction.Request createTestInstance() {
        return new DeleteComposableIndexTemplateAction.Request(randomAlphaOfLength(5));
    }

    @Override
    protected DeleteComposableIndexTemplateAction.Request mutateInstance(DeleteComposableIndexTemplateAction.Request instance)
        throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }
}

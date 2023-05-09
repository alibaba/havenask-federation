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

package org.havenask.client.core;

import org.havenask.client.AbstractResponseTestCase;
import org.havenask.common.xcontent.XContentParser;
import org.havenask.common.xcontent.XContentType;

import java.io.IOException;

import static org.hamcrest.Matchers.is;

public class AcknowledgedResponseTests extends AbstractResponseTestCase<org.havenask.action.support.master.AcknowledgedResponse,
    AcknowledgedResponse> {

    @Override
    protected org.havenask.action.support.master.AcknowledgedResponse createServerTestInstance(XContentType xContentType) {
        return new org.havenask.action.support.master.AcknowledgedResponse(randomBoolean());
    }

    @Override
    protected AcknowledgedResponse doParseToClientInstance(XContentParser parser) throws IOException {
        return AcknowledgedResponse.fromXContent(parser);
    }

    @Override
    protected void assertInstances(org.havenask.action.support.master.AcknowledgedResponse serverTestInstance,
                                   AcknowledgedResponse clientInstance) {
        assertThat(clientInstance.isAcknowledged(), is(serverTestInstance.isAcknowledged()));
    }

}

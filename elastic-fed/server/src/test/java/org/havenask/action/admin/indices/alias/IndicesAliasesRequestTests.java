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

package org.havenask.action.admin.indices.alias;

import org.havenask.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.havenask.common.bytes.BytesReference;
import org.havenask.common.xcontent.ToXContent;
import org.havenask.common.xcontent.XContentParser;
import org.havenask.common.xcontent.XContentType;
import org.havenask.test.HavenaskTestCase;

import java.io.IOException;

import static org.havenask.index.alias.RandomAliasActionsGenerator.randomAliasAction;
import static org.hamcrest.CoreMatchers.equalTo;

public class IndicesAliasesRequestTests extends HavenaskTestCase {

    public void testToAndFromXContent() throws IOException {
        IndicesAliasesRequest indicesAliasesRequest = createTestInstance();
        XContentType xContentType = randomFrom(XContentType.values());

        BytesReference shuffled = toShuffledXContent(indicesAliasesRequest, xContentType, ToXContent.EMPTY_PARAMS, true, "filter");

        IndicesAliasesRequest parsedIndicesAliasesRequest;
        try (XContentParser parser = createParser(xContentType.xContent(), shuffled)) {
            parsedIndicesAliasesRequest = IndicesAliasesRequest.fromXContent(parser);
            assertNull(parser.nextToken());
        }

        for (int i = 0; i < parsedIndicesAliasesRequest.getAliasActions().size(); i++) {
            AliasActions expectedAction = indicesAliasesRequest.getAliasActions().get(i);
            AliasActions actualAction = parsedIndicesAliasesRequest.getAliasActions().get(i);
            assertThat(actualAction, equalTo(expectedAction));
        }
    }

    private IndicesAliasesRequest createTestInstance() {
        int numItems = randomIntBetween(0, 32);
        IndicesAliasesRequest request = new IndicesAliasesRequest();
        if (randomBoolean()) {
            request.timeout(randomTimeValue());
        }

        if (randomBoolean()) {
            request.masterNodeTimeout(randomTimeValue());
        }
        for (int i = 0; i < numItems; i++) {
            request.addAliasAction(randomAliasAction());
        }
        return request;
    }
}

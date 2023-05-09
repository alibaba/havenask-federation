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

package org.havenask.action.admin.cluster.storedscripts;

import org.havenask.common.io.stream.Writeable;
import org.havenask.common.xcontent.XContentParser;
import org.havenask.common.xcontent.XContentType;
import org.havenask.script.Script;
import org.havenask.script.StoredScriptSource;
import org.havenask.test.AbstractSerializingTestCase;
import org.havenask.action.admin.cluster.storedscripts.GetStoredScriptResponse;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.function.Predicate;

public class GetStoredScriptResponseTests extends AbstractSerializingTestCase<GetStoredScriptResponse> {

    @Override
    protected GetStoredScriptResponse doParseInstance(XContentParser parser) throws IOException {
        return GetStoredScriptResponse.fromXContent(parser);
    }

    @Override
    protected GetStoredScriptResponse createTestInstance() {
        return new GetStoredScriptResponse(randomAlphaOfLengthBetween(1, 10), randomScriptSource());
    }

    @Override
    protected Writeable.Reader<GetStoredScriptResponse> instanceReader() {
        return GetStoredScriptResponse::new;
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return s -> "script.options".equals(s);
    }

    private static StoredScriptSource randomScriptSource() {
        final String lang = randomFrom("lang", "painless", "mustache");
        final String source = randomAlphaOfLengthBetween(1, 10);
        final Map<String, String> options = randomBoolean()
            ? Collections.singletonMap(Script.CONTENT_TYPE_OPTION, XContentType.JSON.mediaType())
            : Collections.emptyMap();
        return new StoredScriptSource(lang, source, options);
    }
}

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

package org.havenask.action.admin.cluster.repositories.put;

import org.havenask.common.bytes.BytesReference;
import org.havenask.common.settings.Settings;
import org.havenask.common.xcontent.ToXContent;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.common.xcontent.XContentHelper;
import org.havenask.repositories.fs.FsRepository;
import org.havenask.test.HavenaskTestCase;
import org.havenask.action.admin.cluster.repositories.put.PutRepositoryRequest;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.havenask.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;

public class PutRepositoryRequestTests extends HavenaskTestCase {

    @SuppressWarnings("unchecked")
    public void testCreateRepositoryToXContent() throws IOException {
        Map<String, String> mapParams = new HashMap<>();
        PutRepositoryRequest request = new PutRepositoryRequest();
        String repoName = "test";
        request.name(repoName);
        mapParams.put("name", repoName);
        Boolean verify = randomBoolean();
        request.verify(verify);
        mapParams.put("verify", verify.toString());
        String type = FsRepository.TYPE;
        request.type(type);
        mapParams.put("type", type);

        Boolean addSettings = randomBoolean();
        if (addSettings) {
            request.settings(Settings.builder().put(FsRepository.LOCATION_SETTING.getKey(), ".").build());
        }

        XContentBuilder builder = jsonBuilder();
        request.toXContent(builder, new ToXContent.MapParams(mapParams));
        builder.flush();

        Map<String, Object> outputMap = XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2();

        assertThat(outputMap.get("name"), equalTo(request.name()));
        assertThat(outputMap.get("verify"), equalTo(request.verify()));
        assertThat(outputMap.get("type"), equalTo(request.type()));
        Map<String, Object> settings = (Map<String, Object>) outputMap.get("settings");
        if (addSettings) {
            assertThat(settings.get(FsRepository.LOCATION_SETTING.getKey()), equalTo("."));
        } else {
            assertTrue(((Map<String, Object>) outputMap.get("settings")).isEmpty());
        }
    }
}

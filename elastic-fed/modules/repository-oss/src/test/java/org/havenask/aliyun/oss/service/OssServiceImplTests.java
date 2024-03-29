/*
 * Copyright (c) 2021, Alibaba Group;
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.havenask.aliyun.oss.service;

import org.havenask.aliyun.oss.service.exception.CreateStsOssClientException;
import org.havenask.cluster.metadata.RepositoryMetadata;
import org.havenask.common.settings.Settings;
import org.havenask.test.HavenaskTestCase;

public class OssServiceImplTests extends HavenaskTestCase {
    public void isUseStsOssClient() throws CreateStsOssClientException {
        Settings settings = Settings.builder()
            .put("ecs_ram_role", "testUser1")
            .put("endpoint", "http://oss-cn-hangzhou-internal.aliyuncs.com")
            .build();
        RepositoryMetadata metadata = new RepositoryMetadata("test", "ut", settings);
        OssServiceImpl ossServiceImpl = new OssServiceImpl(metadata, settings);
        assertTrue(ossServiceImpl.isUseStsOssClient());
    }
}

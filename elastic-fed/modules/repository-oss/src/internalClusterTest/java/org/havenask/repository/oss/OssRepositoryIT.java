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

package org.havenask.repository.oss;

import java.util.Arrays;
import java.util.Collection;

import org.apache.commons.lang.StringUtils;
import org.havenask.aliyun.oss.blobstore.MockOssService;
import org.havenask.aliyun.oss.service.OssClientSettings;
import org.havenask.aliyun.oss.service.OssService;
import org.havenask.cluster.metadata.RepositoryMetadata;
import org.havenask.common.settings.Settings;
import org.havenask.common.settings.Settings.Builder;
import org.havenask.common.unit.ByteSizeUnit;
import org.havenask.plugin.repository.oss.OssRepositoryPlugin;
import org.havenask.plugins.Plugin;

public class OssRepositoryIT extends CustomESBlobStoreRepositoryIntegTestCase {
    private static final String BUCKET = "oss-repository-test";
    private static final OssService client = new MockOssService();

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockOssRepositoryPlugin.class);
    }

    protected Settings repositorySettings() {
        Builder settings = Settings.builder();
        settings.put(OssClientSettings.BUCKET.getKey(), BUCKET)
            .put(OssClientSettings.BASE_PATH.getKey(), StringUtils.EMPTY)
            .put(OssClientSettings.ACCESS_KEY_ID.getKey(), "test_access_key_id")
            .put(OssClientSettings.SECRET_ACCESS_KEY.getKey(), "test_secret_access_key")
            .put(OssClientSettings.ENDPOINT.getKey(), "test_endpoint")
            .put(OssClientSettings.COMPRESS.getKey(), randomBoolean())
            .put(OssClientSettings.SUPPORT_CNAME.getKey(), randomBoolean())
            .put(OssClientSettings.CHUNK_SIZE.getKey(), randomIntBetween(100, 1000), ByteSizeUnit.MB);
        return settings.build();
    }

    @Override
    protected String repositoryType() {
        return OssRepository.TYPE;
    }

    public static class MockOssRepositoryPlugin extends OssRepositoryPlugin {
        @Override
        protected OssService createStorageService(RepositoryMetadata metadata, Settings settings) {
            return client;
        }
    }

}

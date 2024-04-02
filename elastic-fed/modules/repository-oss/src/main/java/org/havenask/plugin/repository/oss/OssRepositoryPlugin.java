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

package org.havenask.plugin.repository.oss;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.havenask.SpecialPermission;
import org.havenask.aliyun.oss.blobstore.OssBlobContainer;
import org.havenask.aliyun.oss.service.OssClientSettings;
import org.havenask.aliyun.oss.service.OssService;
import org.havenask.aliyun.oss.service.OssServiceImpl;
import org.havenask.aliyun.oss.service.exception.CreateStsOssClientException;
import org.havenask.cluster.metadata.RepositoryMetadata;
import org.havenask.cluster.service.ClusterService;
import org.havenask.common.settings.Setting;
import org.havenask.common.settings.Settings;
import org.havenask.common.xcontent.NamedXContentRegistry;
import org.havenask.env.Environment;
import org.havenask.indices.recovery.RecoverySettings;
import org.havenask.plugins.Plugin;
import org.havenask.plugins.RepositoryPlugin;
import org.havenask.repositories.Repository;
import org.havenask.repository.oss.OssRepository;

public class OssRepositoryPlugin extends Plugin implements RepositoryPlugin {

    private static final Logger logger = LogManager.getLogger(OssBlobContainer.class);

    static {
        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }
        AccessController.doPrivileged((PrivilegedAction<Void>) () -> { return null; });
    }

    protected OssService createStorageService(RepositoryMetadata metadata, Settings settings) throws CreateStsOssClientException {
        return new OssServiceImpl(metadata, settings);
    }

    @Override
    public Map<String, Repository.Factory> getRepositories(
        Environment env,
        NamedXContentRegistry namedXContentRegistry,
        ClusterService clusterService,
        RecoverySettings recoverySettings
    ) {
        return Collections.singletonMap(
            OssRepository.TYPE,
            (metadata) -> new OssRepository(
                metadata,
                namedXContentRegistry,
                createStorageService(metadata, env.settings()),
                clusterService,
                recoverySettings
            )
        );
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(
            OssClientSettings.ACCESS_KEY_ID,
            OssClientSettings.SECRET_ACCESS_KEY,
            OssClientSettings.ENDPOINT,
            OssClientSettings.BUCKET,
            OssClientSettings.SECURITY_TOKEN,
            OssClientSettings.BASE_PATH,
            OssClientSettings.COMPRESS,
            OssClientSettings.CHUNK_SIZE,
            OssClientSettings.AUTO_SNAPSHOT_BUCKET,
            OssClientSettings.ECS_RAM_ROLE,
            OssClientSettings.SUPPORT_CNAME,
            OssClientSettings.RAM_ROLE_CREDENTIALS_ENDPOINT
        );
    }
}

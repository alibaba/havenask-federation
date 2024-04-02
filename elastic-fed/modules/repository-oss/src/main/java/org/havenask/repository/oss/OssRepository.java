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

import java.io.File;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.havenask.aliyun.oss.blobstore.OssBlobContainer;
import org.havenask.aliyun.oss.blobstore.OssBlobStore;
import org.havenask.aliyun.oss.service.OssClientSettings;
import org.havenask.aliyun.oss.service.OssService;
import org.havenask.cluster.metadata.RepositoryMetadata;
import org.havenask.cluster.service.ClusterService;
import org.havenask.common.Strings;
import org.havenask.common.blobstore.BlobPath;
import org.havenask.common.blobstore.BlobStore;
import org.havenask.common.settings.Setting;
import org.havenask.common.unit.ByteSizeValue;
import org.havenask.common.xcontent.NamedXContentRegistry;
import org.havenask.indices.recovery.RecoverySettings;
import org.havenask.repositories.RepositoryException;
import org.havenask.repositories.blobstore.BlobStoreRepository;

/**
 * An oss repository working for snapshot and restore.
 * Implementations are responsible for reading and writing both metadata and shard data to and from
 * a repository backend.
 */
public class OssRepository extends BlobStoreRepository {

    private static final Logger logger = LogManager.getLogger(OssBlobContainer.class);

    public static final String TYPE = "oss";
    private final BlobPath basePath;
    private final boolean compress;
    private final ByteSizeValue chunkSize;
    private final String bucket;
    private final OssService ossService;

    public OssRepository(
        RepositoryMetadata metadata,
        NamedXContentRegistry namedXContentRegistry,
        OssService ossService,
        ClusterService clusterService,
        RecoverySettings recoverySettings
    ) {
        super(metadata, getSetting(OssClientSettings.COMPRESS, metadata), namedXContentRegistry, clusterService, recoverySettings);
        this.ossService = ossService;
        String ecsRamRole = OssClientSettings.ECS_RAM_ROLE.get(metadata.settings()).toString();
        if (StringUtils.isNotEmpty(ecsRamRole)) {
            this.bucket = getSetting(OssClientSettings.AUTO_SNAPSHOT_BUCKET, metadata).toString();
        } else {
            this.bucket = getSetting(OssClientSettings.BUCKET, metadata);
        }
        String basePath = OssClientSettings.BASE_PATH.get(metadata.settings());
        if (Strings.hasLength(basePath)) {
            BlobPath path = new BlobPath();
            for (String elem : basePath.split(File.separator)) {
                path = path.add(elem);
            }
            this.basePath = path;
        } else {
            this.basePath = BlobPath.cleanPath();
        }
        this.compress = getSetting(OssClientSettings.COMPRESS, metadata);
        this.chunkSize = getSetting(OssClientSettings.CHUNK_SIZE, metadata);
        logger.info("Using base_path [{}], chunk_size [{}], compress [{}]", basePath, chunkSize, compress);
    }

    @Override
    protected BlobStore createBlobStore() throws Exception {
        return new OssBlobStore(bucket, ossService);
    }

    @Override
    public BlobPath basePath() {
        return basePath;
    }

    @Override
    protected ByteSizeValue chunkSize() {
        return chunkSize;
    }

    public static <T> T getSetting(Setting<T> setting, RepositoryMetadata metadata) {
        T value = setting.get(metadata.settings());
        if (value == null) {
            throw new RepositoryException(metadata.name(), "Setting [" + setting.getKey() + "] is not defined for repository");
        }
        if ((value instanceof String) && (Strings.hasText((String) value)) == false) {
            throw new RepositoryException(metadata.name(), "Setting [" + setting.getKey() + "] is empty for repository");
        }
        return value;
    }

    /**
     * mainly for test
     **/
    public OssService getOssService() {
        return ossService;
    }
}

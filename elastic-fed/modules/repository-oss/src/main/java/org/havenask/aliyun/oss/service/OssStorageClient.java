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

import static java.lang.Thread.sleep;

import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.havenask.aliyun.oss.blobstore.OssBlobContainer;
import org.havenask.aliyun.oss.service.exception.CreateStsOssClientException;
import org.havenask.cluster.metadata.RepositoryMetadata;
import org.havenask.common.settings.SecureString;
import org.havenask.common.settings.Settings;
import org.havenask.repository.oss.OssRepository;
import org.havenask.utils.DateHelper;
import org.havenask.utils.HttpClientHelper;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.aliyun.oss.ClientConfiguration;
import com.aliyun.oss.ClientException;
import com.aliyun.oss.OSSClient;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.CopyObjectResult;
import com.aliyun.oss.model.DeleteObjectsRequest;
import com.aliyun.oss.model.DeleteObjectsResult;
import com.aliyun.oss.model.GetObjectRequest;
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.OSSObject;
import com.aliyun.oss.model.ObjectListing;
import com.aliyun.oss.model.ObjectMetadata;
import com.aliyun.oss.model.PutObjectResult;

import okhttp3.Response;

public class OssStorageClient {
    private static final Logger logger = LogManager.getLogger(OssBlobContainer.class);

    private RepositoryMetadata metadata;
    private Settings settings;
    private OSSClient client;
    private Date stsTokenExpiration;
    private final int IN_TOKEN_EXPIRED_MS = 5000;
    private final String ACCESS_KEY_ID = "AccessKeyId";
    private final String ACCESS_KEY_SECRET = "AccessKeySecret";
    private final String SECURITY_TOKEN = "SecurityToken";
    private final int REFRESH_RETRY_COUNT = 3;
    private final int CONNECTION_TIMEOUT = 10000;
    private boolean isStsOssClient;
    private ReadWriteLock readWriteLock;

    private final String EXPIRATION = "Expiration";

    public OssStorageClient(RepositoryMetadata metadata, Settings settings) throws CreateStsOssClientException {
        this.metadata = metadata;
        this.settings = settings;
        if (StringUtils.isNotEmpty(OssClientSettings.ECS_RAM_ROLE.get(metadata.settings()).toString())) {
            isStsOssClient = true;
        } else {
            isStsOssClient = false;
        }
        readWriteLock = new ReentrantReadWriteLock();
        client = createClient(metadata);

    }

    public boolean isStsOssClient() {
        return isStsOssClient;
    }

    public DeleteObjectsResult deleteObjects(DeleteObjectsRequest deleteObjectsRequest) throws OSSException, ClientException {
        if (isStsOssClient) {
            readWriteLock.readLock().lock();
            try {
                return this.client.deleteObjects(deleteObjectsRequest);
            } finally {
                readWriteLock.readLock().unlock();
            }
        } else {
            return this.client.deleteObjects(deleteObjectsRequest);
        }
    }

    public boolean doesObjectExist(String bucketName, String key) throws OSSException, ClientException {
        if (isStsOssClient) {
            readWriteLock.readLock().lock();
            try {
                return this.client.doesObjectExist(bucketName, key);
            } finally {
                readWriteLock.readLock().unlock();
            }
        } else {
            return this.client.doesObjectExist(bucketName, key);
        }
    }

    public boolean doesBucketExist(String bucketName) throws OSSException, ClientException {
        if (isStsOssClient) {
            readWriteLock.readLock().lock();
            try {
                return this.client.doesBucketExist(bucketName);
            } finally {
                readWriteLock.readLock().unlock();
            }
        } else {
            return this.client.doesBucketExist(bucketName);
        }
    }

    public ObjectListing listObjects(ListObjectsRequest listObjectsRequest) throws OSSException, ClientException {
        if (isStsOssClient) {
            readWriteLock.readLock().lock();
            try {
                return this.client.listObjects(listObjectsRequest);
            } finally {
                readWriteLock.readLock().unlock();
            }
        } else {
            return this.client.listObjects(listObjectsRequest);
        }
    }

    public OSSObject getObject(String bucketName, String key) throws OSSException, ClientException {
        if (isStsOssClient) {
            readWriteLock.readLock().lock();
            try {
                return this.client.getObject(bucketName, key);
            } finally {
                readWriteLock.readLock().unlock();
            }
        } else {
            return this.client.getObject(bucketName, key);
        }
    }

    public OSSObject getObject(String bucketName, String key, long start, long end) throws OSSException, ClientException {
        GetObjectRequest getObjectRequest = new GetObjectRequest(bucketName, key);
        getObjectRequest.setRange(start, end);
        if (isStsOssClient) {
            readWriteLock.readLock().lock();
            try {
                return this.client.getObject(getObjectRequest);
            } finally {
                readWriteLock.readLock().unlock();
            }
        } else {
            return this.client.getObject(getObjectRequest);
        }
    }

    public PutObjectResult putObject(String bucketName, String key, InputStream input, ObjectMetadata metadata) throws OSSException,
        ClientException {
        if (isStsOssClient) {
            readWriteLock.readLock().lock();
            try {
                return this.client.putObject(bucketName, key, input, metadata);
            } finally {
                readWriteLock.readLock().unlock();
            }
        } else {
            return this.client.putObject(bucketName, key, input, metadata);
        }
    }

    public void deleteObject(String bucketName, String key) throws OSSException, ClientException {
        if (isStsOssClient) {
            readWriteLock.readLock().lock();
            try {
                this.client.deleteObject(bucketName, key);
            } finally {
                readWriteLock.readLock().unlock();
            }
        } else {
            this.client.deleteObject(bucketName, key);
        }

    }

    public CopyObjectResult copyObject(String sourceBucketName, String sourceKey, String destinationBucketName, String destinationKey)
        throws OSSException, ClientException {

        if (isStsOssClient) {
            readWriteLock.readLock().lock();
            try {
                return this.client.copyObject(sourceBucketName, sourceKey, destinationBucketName, destinationKey);
            } finally {
                readWriteLock.readLock().unlock();
            }
        } else {
            return this.client.copyObject(sourceBucketName, sourceKey, destinationBucketName, destinationKey);
        }
    }

    public void refreshStsOssClient() throws CreateStsOssClientException {
        int retryCount = 0;
        while (isStsTokenExpired() || isTokenWillExpired()) {
            retryCount++;
            if (retryCount > REFRESH_RETRY_COUNT) {
                logger.error("Can't get valid token after retry {} times", REFRESH_RETRY_COUNT);
                throw new CreateStsOssClientException("Can't get valid token after retry " + REFRESH_RETRY_COUNT + " times");
            }
            this.client = createStsOssClient(this.metadata);
            try {
                if (isStsTokenExpired() || isTokenWillExpired()) {
                    sleep(IN_TOKEN_EXPIRED_MS * 2);
                }
            } catch (InterruptedException e) {
                logger.error("refresh sleep exception", e);
                throw new CreateStsOssClientException(e);
            }
        }
    }

    public void shutdown() {
        if (isStsOssClient) {
            readWriteLock.writeLock().lock();
            try {
                if (null != this.client) {
                    this.client.shutdown();
                }
            } finally {
                readWriteLock.writeLock().unlock();
            }
        } else {
            if (null != this.client) {
                this.client.shutdown();
            }
        }
    }

    private boolean isStsTokenExpired() {
        boolean expired = true;
        Date now = new Date();
        if (null != stsTokenExpiration) {
            if (stsTokenExpiration.after(now)) {
                expired = false;
            }
        }
        return expired;
    }

    private boolean isTokenWillExpired() {
        boolean in = true;
        Date now = new Date();
        long millisecond = stsTokenExpiration.getTime() - now.getTime();
        if (millisecond >= IN_TOKEN_EXPIRED_MS) {
            in = false;
        }
        return in;
    }

    private OSSClient createClient(RepositoryMetadata repositoryMetadata) throws CreateStsOssClientException {
        OSSClient client;

        String ecsRamRole = OssClientSettings.ECS_RAM_ROLE.get(repositoryMetadata.settings()).toString();
        String stsToken = OssClientSettings.SECURITY_TOKEN.get(repositoryMetadata.settings()).toString();
        /*
         * If ecsRamRole exist
         * means use ECS metadata service to get ststoken for auto snapshot.
         * */
        if (StringUtils.isNotEmpty(ecsRamRole.toString())) {
            client = createStsOssClient(repositoryMetadata);
        } else if (StringUtils.isNotEmpty(stsToken)) {
            // no used still now.
            client = createAKStsTokenClient(repositoryMetadata);
        } else {
            client = createAKOssClient(repositoryMetadata);
        }
        return client;
    }

    private ClientConfiguration extractClientConfiguration(RepositoryMetadata repositoryMetadata) {
        ClientConfiguration configuration = new ClientConfiguration();
        configuration.setSupportCname(OssRepository.getSetting(OssClientSettings.SUPPORT_CNAME, repositoryMetadata));
        configuration.setConnectionTimeout(CONNECTION_TIMEOUT);
        return configuration;
    }

    private OSSClient createAKOssClient(RepositoryMetadata repositoryMetadata) {
        SecureString accessKeyId = OssRepository.getSetting(OssClientSettings.ACCESS_KEY_ID, repositoryMetadata);
        SecureString secretAccessKey = OssRepository.getSetting(OssClientSettings.SECRET_ACCESS_KEY, repositoryMetadata);
        String endpoint = OssRepository.getSetting(OssClientSettings.ENDPOINT, repositoryMetadata);
        return new OSSClient(endpoint, accessKeyId.toString(), secretAccessKey.toString(), extractClientConfiguration(repositoryMetadata));
    }

    private OSSClient createAKStsTokenClient(RepositoryMetadata repositoryMetadata) {
        SecureString securityToken = OssClientSettings.SECURITY_TOKEN.get(repositoryMetadata.settings());
        SecureString accessKeyId = OssRepository.getSetting(OssClientSettings.ACCESS_KEY_ID, repositoryMetadata);
        SecureString secretAccessKey = OssRepository.getSetting(OssClientSettings.SECRET_ACCESS_KEY, repositoryMetadata);
        String endpoint = OssRepository.getSetting(OssClientSettings.ENDPOINT, repositoryMetadata);
        return new OSSClient(
            endpoint,
            accessKeyId.toString(),
            secretAccessKey.toString(),
            securityToken.toString(),
            extractClientConfiguration(repositoryMetadata)
        );
    }

    private synchronized OSSClient createStsOssClient(RepositoryMetadata repositoryMetadata) throws CreateStsOssClientException {
        if (isStsTokenExpired() || isTokenWillExpired()) {
            try {
                if (null == repositoryMetadata) {
                    throw new IOException("repositoryMetaData is null");
                }
                String ecsRamRole = OssClientSettings.ECS_RAM_ROLE.get(repositoryMetadata.settings()).toString();
                String endpoint = OssRepository.getSetting(OssClientSettings.ENDPOINT, repositoryMetadata);

                String fullECSMetaDataServiceUrl = OssClientSettings.RAM_ROLE_CREDENTIALS_ENDPOINT.get(settings) + ecsRamRole;
                Response response = HttpClientHelper.httpRequest(fullECSMetaDataServiceUrl);
                if (!response.isSuccessful()) {
                    throw new IOException("ECS meta service server error");
                }
                String jsonStringResponse = response.body().string();
                JSONObject jsonObjectResponse = JSON.parseObject(jsonStringResponse);
                String accessKeyId = jsonObjectResponse.getString(ACCESS_KEY_ID);
                String accessKeySecret = jsonObjectResponse.getString(ACCESS_KEY_SECRET);
                String securityToken = jsonObjectResponse.getString(SECURITY_TOKEN);
                stsTokenExpiration = DateHelper.convertStringToDate(jsonObjectResponse.getString(EXPIRATION));
                try {
                    readWriteLock.writeLock().lock();
                    if (null != this.client) {
                        this.client.shutdown();
                    }
                    this.client = new OSSClient(
                        endpoint,
                        accessKeyId,
                        accessKeySecret,
                        securityToken,
                        extractClientConfiguration(repositoryMetadata)
                    );
                } finally {
                    readWriteLock.writeLock().unlock();
                }
                response.close();
            } catch (IOException e) {
                logger.error("create stsOssClient exception", e);
                throw new CreateStsOssClientException(e);
            }
            return this.client;
        } else {
            return this.client;
        }
    }
}

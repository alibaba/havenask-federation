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

import java.io.IOException;
import java.io.InputStream;

import org.havenask.aliyun.oss.service.exception.CreateStsOssClientException;
import org.havenask.cluster.metadata.RepositoryMetadata;
import org.havenask.common.settings.Settings;

import com.aliyun.oss.ClientException;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.CopyObjectResult;
import com.aliyun.oss.model.DeleteObjectsRequest;
import com.aliyun.oss.model.DeleteObjectsResult;
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.OSSObject;
import com.aliyun.oss.model.ObjectListing;
import com.aliyun.oss.model.ObjectMetadata;
import com.aliyun.oss.model.PutObjectResult;

/**
 * OSS Service implementation for creating oss client
 */
public class OssServiceImpl implements OssService {

    private OssStorageClient ossStorageClient;

    public OssServiceImpl(RepositoryMetadata metadata, Settings settings) throws CreateStsOssClientException {
        this.ossStorageClient = new OssStorageClient(metadata, settings);
    }

    @Override
    public DeleteObjectsResult deleteObjects(DeleteObjectsRequest deleteObjectsRequest) throws OSSException, ClientException {
        return this.ossStorageClient.deleteObjects(deleteObjectsRequest);
    }

    @Override
    public boolean doesObjectExist(String bucketName, String key) throws OSSException, ClientException {
        return this.ossStorageClient.doesObjectExist(bucketName, key);
    }

    @Override
    public boolean doesBucketExist(String bucketName) throws OSSException, ClientException {
        return this.ossStorageClient.doesBucketExist(bucketName);
    }

    @Override
    public ObjectListing listObjects(ListObjectsRequest listObjectsRequest) throws OSSException, ClientException {
        return this.ossStorageClient.listObjects(listObjectsRequest);
    }

    @Override
    public OSSObject getObject(String bucketName, String key) throws OSSException, ClientException {
        return this.ossStorageClient.getObject(bucketName, key);
    }

    @Override
    public OSSObject getObject(String bucketName, String key, long start, long end) throws OSSException, ClientException, IOException {
        return this.ossStorageClient.getObject(bucketName, key, start, end);
    }

    @Override
    public PutObjectResult putObject(String bucketName, String key, InputStream input, ObjectMetadata metadata) throws OSSException,
        ClientException {
        return this.ossStorageClient.putObject(bucketName, key, input, metadata);
    }

    @Override
    public void deleteObject(String bucketName, String key) throws OSSException, ClientException {
        this.ossStorageClient.deleteObject(bucketName, key);
    }

    @Override
    public CopyObjectResult copyObject(String sourceBucketName, String sourceKey, String destinationBucketName, String destinationKey)
        throws OSSException, ClientException {
        return this.ossStorageClient.copyObject(sourceBucketName, sourceKey, destinationBucketName, destinationKey);
    }

    @Override
    public void shutdown() {
        ossStorageClient.shutdown();
    }

    @Override
    public void refreshStsOssClient() throws CreateStsOssClientException {
        ossStorageClient.refreshStsOssClient();
    }

    @Override
    public boolean isUseStsOssClient() {
        return ossStorageClient.isStsOssClient();
    }
}

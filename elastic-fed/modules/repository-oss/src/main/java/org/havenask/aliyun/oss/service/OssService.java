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

import com.aliyun.oss.ClientException;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.Bucket;
import com.aliyun.oss.model.CopyObjectResult;
import com.aliyun.oss.model.DeleteObjectsRequest;
import com.aliyun.oss.model.DeleteObjectsResult;
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.OSSObject;
import com.aliyun.oss.model.ObjectListing;
import com.aliyun.oss.model.ObjectMetadata;
import com.aliyun.oss.model.PutObjectResult;

/**
 * OSS Service interface for creating oss client.
 */
public interface OssService {

    /**
     * Bulk delete the specified bucket {@link OSSObject}.
     *
     * @param deleteObjectsRequest request parameters {@link DeleteObjectsRequest}
     * @return delete results in bulk.
     */
    DeleteObjectsResult deleteObjects(DeleteObjectsRequest deleteObjectsRequest) throws OSSException, ClientException;

    /**
     * Determines if the specified {@link OSSObject} exists under the specified {@link Bucket}.
     *
     * @param bucketName bucket name
     * @param key        Object Key
     * @return {@code true} if a blob exists with the given name, and {@code false} otherwise.
     */
    boolean doesObjectExist(String bucketName, String key) throws OSSException, ClientException;

    /**
     * Determines if a given {@link Bucket} exists.
     *
     * @param bucketName bucket name
     */
    boolean doesBucketExist(String bucketName) throws OSSException, ClientException;

    /**
     * List {@link OSSObject} under the specified {@link Bucket}.
     *
     * @param listObjectsRequest request information
     * @return object list {@link ObjectListing}
     * @throws OSSException
     * @throws ClientException
     */
    ObjectListing listObjects(ListObjectsRequest listObjectsRequest) throws OSSException, ClientException;

    /**
     * Export {@link OSSObject} from the OSS specified {@link Bucket}.
     *
     * @param bucketName Bucket name.
     * @param key        Object Key.
     * @return Request result {@link OSSObject} instance. After use, you need to manually
     * close the ObjectContent release request connection.
     */
    OSSObject getObject(String bucketName, String key) throws OSSException, ClientException, IOException;

    /**
     * Export {@link OSSObject} from the OSS specified {@link Bucket}.
     *
     * @param bucketName Bucket name.
     * @param key Object Key.
     * @param start Object start position
     * @param end Object end position
     * @return Request result {@link OSSObject} instance. After use, you need to manually
     * close the ObjectContent release request connection.
     */
    OSSObject getObject(String bucketName, String key, long start, long end) throws OSSException, ClientException, IOException;

    /**
     * Upload the specified {@link OSSObject} to the {@link Bucket} specified in OSS.
     *
     * @param bucketName Bucket name
     * @param key        object key
     * @param input      inputStream
     * @param metadata   the meta-information of the object {@link ObjectMetadata},
     *                   if the meta-information does not contain Content-Length,
     *                   transmits the request data in chunked encoding.
     */
    PutObjectResult putObject(String bucketName, String key, InputStream input, ObjectMetadata metadata) throws OSSException,
        ClientException, IOException;

    /**
     * Delete the specified {@link OSSObject}.
     *
     * @param bucketName Bucket name.
     * @param key        Object key.
     * @throws OSSException
     * @throws ClientException
     */
    void deleteObject(String bucketName, String key) throws OSSException, ClientException;

    /**
     * Copy an Object that already exists on the OSS into another Object.
     *
     * @param sourceBucketName      the name of the bucket where the source object resides.
     * @param sourceKey             key of source Object.
     * @param destinationBucketName the name of the bucket where the target object is.
     * @param destinationKey        key of the target Object.
     * @return request result {@link CopyObjectResult} instance.
     * @throws OSSException
     * @throws ClientException
     */
    CopyObjectResult copyObject(String sourceBucketName, String sourceKey, String destinationBucketName, String destinationKey)
        throws OSSException, ClientException;

    /**
     * Closes the Client instance and frees all the resources that are in use.
     * Once closed, no requests to the OSS will be processed anymore.
     */
    void shutdown();

    /**
     * refresh StsOssClient instance which construct via sts-accessKeyId, sts-accessKeySecret, sts-securityToken
     * Once refreshed, sts-securityToken is valid.
     */
    void refreshStsOssClient() throws CreateStsOssClientException;

    /**
     * judge if use Sts OssClient
     */
    boolean isUseStsOssClient();
}

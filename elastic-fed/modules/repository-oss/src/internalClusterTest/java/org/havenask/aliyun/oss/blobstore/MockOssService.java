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

package org.havenask.aliyun.oss.blobstore;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.havenask.aliyun.oss.service.OssService;
import org.havenask.aliyun.oss.service.exception.CreateStsOssClientException;
import org.havenask.common.util.set.Sets;

import com.aliyun.oss.ClientException;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.CopyObjectResult;
import com.aliyun.oss.model.DeleteObjectsRequest;
import com.aliyun.oss.model.DeleteObjectsResult;
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.OSSObject;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectListing;
import com.aliyun.oss.model.ObjectMetadata;
import com.aliyun.oss.model.PutObjectResult;

public class MockOssService implements OssService {
    private static final Logger logger = LogManager.getLogger(MockOssService.class);
    protected final Map<String, OSSObject> blobs = new ConcurrentHashMap<>();

    public MockOssService() {
        super();
    }

    @Override
    public DeleteObjectsResult deleteObjects(DeleteObjectsRequest deleteObjectsRequest) throws OSSException, ClientException {
        logger.info("delete oss objects {}", deleteObjectsRequest.getKeys());
        for (String key : deleteObjectsRequest.getKeys()) {
            blobs.remove(key);
        }
        return null;
    }

    @Override
    public boolean doesObjectExist(String bucketName, String key) throws OSSException, ClientException {
        return blobs.containsKey(key);
    }

    @Override
    public boolean doesBucketExist(String bucketName) throws OSSException, ClientException {
        return true;
    }

    @Override
    public ObjectListing listObjects(ListObjectsRequest listObjectsRequest) throws OSSException, ClientException {
        ObjectListing objectListing = new ObjectListing();
        objectListing.setTruncated(false);
        String prefix = listObjectsRequest.getPrefix();
        String delimiter = listObjectsRequest.getDelimiter();
        Set<String> commonPrefixSet = Sets.newHashSet();
        blobs.forEach((String blobName, OSSObject ossObject) -> {
            if (startsWithIgnoreCase(blobName, prefix)) {
                String afterPrefixString = blobName.substring(prefix.length());
                logger.info("blobName {}, afterPrefixString {}", blobName, afterPrefixString);
                if (null == delimiter || !afterPrefixString.contains(delimiter)) {
                    OSSObjectSummary summary = new OSSObjectSummary();
                    summary.setKey(blobName);
                    summary.setSize(ossObject.getObjectMetadata().getContentLength());
                    objectListing.addObjectSummary(summary);
                } else {
                    commonPrefixSet.add(blobName.substring(0, blobName.indexOf(delimiter, prefix.length()) + 1));
                }
            }
        });
        for (String commonPrefix : commonPrefixSet) {
            objectListing.addCommonPrefix(commonPrefix);
        }
        logger.info(
            "listObjects objectSummary {}",
            objectListing.getObjectSummaries().stream().map(f -> f.getKey()).collect(Collectors.toList())
        );
        logger.info("listObjects commonPrefix {}", commonPrefixSet);
        return objectListing;
    }

    @Override
    public OSSObject getObject(String bucketName, String key) throws OSSException, ClientException, IOException {
        OSSObject ossObject = new OSSObject();
        synchronized (this) {
            OSSObject oldObject = blobs.get(key);
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            org.havenask.core.internal.io.Streams.copy(oldObject.getObjectContent(), outputStream);
            oldObject.setObjectContent(new ByteArrayInputStream(outputStream.toByteArray()));
            ossObject.setObjectContent(new ByteArrayInputStream(outputStream.toByteArray()));
        }
        return ossObject;
    }

    @Override
    public OSSObject getObject(String bucketName, String key, long start, long end) throws OSSException, ClientException, IOException {
        OSSObject ossObject = new OSSObject();
        synchronized (this) {
            OSSObject oldObject = blobs.get(key);
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            org.havenask.core.internal.io.Streams.copy(oldObject.getObjectContent(), outputStream);
            oldObject.setObjectContent(new ByteArrayInputStream(outputStream.toByteArray()));
            ossObject.setObjectContent(new ByteArrayInputStream(outputStream.toByteArray(), (int) start, (int) (end - start + 1)));
        }
        return ossObject;
    }

    @Override
    public PutObjectResult putObject(String bucketName, String key, InputStream input, ObjectMetadata metadata) throws OSSException,
        ClientException, IOException {
        synchronized (this) {
            OSSObject ossObject = new OSSObject();
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            org.havenask.core.internal.io.Streams.copy(input, outputStream);
            ossObject.setObjectContent(new ByteArrayInputStream(outputStream.toByteArray()));
            ossObject.setObjectMetadata(metadata);
            blobs.put(key, ossObject);
        }
        return null;
    }

    @Override
    public void deleteObject(String bucketName, String key) throws OSSException, ClientException {
        logger.info("delete oss objects {}", key);
        blobs.remove(key);
    }

    @Override
    public CopyObjectResult copyObject(String sourceBucketName, String sourceKey, String destinationBucketName, String destinationKey)
        throws OSSException, ClientException {
        OSSObject sourceOssObject = blobs.get(sourceKey);
        blobs.put(destinationKey, sourceOssObject);
        return null;
    }

    @Override
    public void shutdown() {
        blobs.clear();
    }

    @Override
    public void refreshStsOssClient() throws CreateStsOssClientException {

    }

    @Override
    public boolean isUseStsOssClient() {
        return false;
    }

    /**
     * Test if the given String starts with the specified prefix,
     * ignoring upper/lower case.
     *
     * @param str    the String to check
     * @param prefix the prefix to look for
     * @see java.lang.String#startsWith
     */
    public static boolean startsWithIgnoreCase(String str, String prefix) {
        if (str == null || prefix == null) {
            return false;
        }
        if (str.startsWith(prefix)) {
            return true;
        }
        if (str.length() < prefix.length()) {
            return false;
        }
        String lcStr = str.substring(0, prefix.length()).toLowerCase(Locale.ROOT);
        String lcPrefix = prefix.toLowerCase(Locale.ROOT);
        return lcStr.equals(lcPrefix);
    }

}

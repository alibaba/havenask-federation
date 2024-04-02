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
import java.io.IOException;
import java.io.InputStream;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.havenask.aliyun.oss.service.OssService;
import org.havenask.common.blobstore.BlobContainer;
import org.havenask.common.blobstore.BlobMetadata;
import org.havenask.common.blobstore.BlobPath;
import org.havenask.common.blobstore.BlobStore;
import org.havenask.common.blobstore.BlobStoreException;
import org.havenask.common.blobstore.DeleteResult;
import org.havenask.common.blobstore.support.PlainBlobMetadata;
import org.havenask.common.collect.MapBuilder;
import org.havenask.utils.PermissionHelper;

import com.aliyun.oss.ClientException;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.DeleteObjectsRequest;
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectListing;
import com.aliyun.oss.model.ObjectMetadata;

/**
 * An oss blob store for managing oss client write and read blob directly
 */
public class OssBlobStore implements BlobStore {

    private static final Logger logger = LogManager.getLogger(OssBlobStore.class);
    private static final String FILE_DELIMITER = "/";
    private final OssService client;
    private final String bucket;

    public OssBlobStore(String bucket, OssService client) {
        this.client = client;
        this.bucket = bucket;
        if (!doesBucketExist(bucket)) {
            throw new BlobStoreException("bucket does not exist");
        }
    }

    public String getBucket() {
        return this.bucket;
    }

    @Override
    public BlobContainer blobContainer(BlobPath blobPath) {
        return new OssBlobContainer(blobPath, this);
    }

    public DeleteResult delete(BlobPath blobPath) throws IOException {
        logger.trace("delete blobPath {}", blobPath);
        DeleteObjectsRequest deleteRequest = new DeleteObjectsRequest(bucket);
        String actualPrefix = blobPath.buildAsString();
        String nextMarker = null;
        ObjectListing blobs;
        List<String> toBeDeletedBlobs = new ArrayList<>();
        final AtomicLong filesDeleted = new AtomicLong(0L);
        final AtomicLong bytesDeleted = new AtomicLong(0L);
        do {
            blobs = listBlobs(actualPrefix, nextMarker, null);
            for (OSSObjectSummary summary : blobs.getObjectSummaries()) {
                toBeDeletedBlobs.add(summary.getKey());
                filesDeleted.incrementAndGet();
                bytesDeleted.addAndGet(summary.getSize());
            }
            deleteRequest.setKeys(toBeDeletedBlobs);
            deleteObjects(deleteRequest);
            nextMarker = blobs.getNextMarker();
            toBeDeletedBlobs.clear();
        } while (blobs.isTruncated());
        return new DeleteResult(filesDeleted.get(), bytesDeleted.get());
    }

    public Map<String, BlobContainer> children(BlobPath path) throws IOException {
        final String pathStr = path.buildAsString();
        final MapBuilder<String, BlobContainer> mapBuilder = MapBuilder.newMapBuilder();
        String nextMarker = null;
        ObjectListing blobs;
        do {
            blobs = listBlobs(pathStr, nextMarker, FILE_DELIMITER);
            for (String commonPrefix : blobs.getCommonPrefixes()) {
                int endIndex = commonPrefix.endsWith(FILE_DELIMITER) ? commonPrefix.length() - 1 : commonPrefix.length();
                String blobName = commonPrefix.substring(pathStr.length(), endIndex);
                mapBuilder.put(blobName, new OssBlobContainer(path.add(blobName), this));
            }
            nextMarker = blobs.getNextMarker();
        } while (blobs.isTruncated());
        return mapBuilder.immutableMap();
    }

    @Override
    public void close() {
        logger.warn("oss blob shutdown......");
        client.shutdown();
    }

    /**
     * Return true if the given bucket exists
     *
     * @param bucketName name of the bucket
     * @return true if the bucket exists, false otherwise
     */
    boolean doesBucketExist(String bucketName) {
        try {
            return doPrivilegedAndRefreshClient(() -> this.client.doesBucketExist(bucketName));
        } catch (IOException e) {
            throw new BlobStoreException("do privileged has failed", e);
        }
    }

    /**
     * List all blobs in the bucket which have a prefix
     *
     * @param prefix prefix of the blobs to list
     * @return a map of blob names and their metadata
     */
    Map<String, BlobMetadata> listBlobsByPrefix(String keyPath, String prefix) throws IOException {
        MapBuilder<String, BlobMetadata> blobsBuilder = MapBuilder.newMapBuilder();
        String actualPrefix = keyPath + (prefix == null ? StringUtils.EMPTY : prefix);
        String nextMarker = null;
        ObjectListing blobs;
        do {
            blobs = listBlobs(actualPrefix, nextMarker, FILE_DELIMITER);
            for (OSSObjectSummary summary : blobs.getObjectSummaries()) {
                String blobName = summary.getKey().substring(keyPath.length());
                blobsBuilder.put(blobName, new PlainBlobMetadata(blobName, summary.getSize()));
            }
            nextMarker = blobs.getNextMarker();
        } while (blobs.isTruncated());
        return blobsBuilder.immutableMap();
    }

    /**
     * list blob with privilege check
     *
     * @param actualPrefix actual prefix of the blobs to list
     * @param nextMarker   blobs next marker
     * @return {@link ObjectListing}
     */
    ObjectListing listBlobs(String actualPrefix, String nextMarker, String delimiter) throws IOException {
        logger.trace("listBlobs bucket {}, prefix {}, nextMarker {}, delimiter {}", bucket, actualPrefix, nextMarker, delimiter);
        return doPrivilegedAndRefreshClient(
            () -> this.client.listObjects(
                new ListObjectsRequest(bucket).withPrefix(actualPrefix).withMarker(nextMarker).withDelimiter(delimiter)
            )
        );
    }

    /**
     * Delete Objects
     *
     * @param deleteRequest {@link DeleteObjectsRequest}
     */
    void deleteObjects(DeleteObjectsRequest deleteRequest) throws IOException {
        doPrivilegedAndRefreshClient(() -> this.client.deleteObjects(deleteRequest));
    }

    /**
     * Returns true if the blob exists in the bucket
     *
     * @param blobName name of the blob
     * @return true if the blob exists, false otherwise
     */
    boolean blobExists(String blobName) throws OSSException, ClientException, IOException {
        return doPrivilegedAndRefreshClient(() -> this.client.doesObjectExist(bucket, blobName));
    }

    /**
     * Returns an {@link java.io.InputStream} for a given blob
     *
     * @param blobName name of the blob
     * @return an InputStream
     */
    InputStream readBlob(String blobName) throws OSSException, ClientException, IOException {
        return doPrivilegedAndRefreshClient(() -> this.client.getObject(bucket, blobName).getObjectContent());
    }

    /**
     * Returns an {@link java.io.InputStream} for a given blob
     *
     * @param blobName name of the blob
     * @param position blob start position
     * @param length  blob content length
     * @return InputStream InputStream
     * @throws OSSException OSSException
     * @throws ClientException ClientException
     * @throws IOException IOException
     */
    InputStream readBlob(String blobName, long position, long length) throws OSSException, ClientException, IOException {
        if (position < 0L) {
            throw new IllegalArgumentException("position must be non-negative");
        }
        if (length < 0) {
            throw new IllegalArgumentException("length must be non-negative");
        }
        if (length == 0) {
            return new ByteArrayInputStream(new byte[0]);
        } else {
            return doPrivilegedAndRefreshClient(
                () -> this.client.getObject(bucket, blobName, position, Math.addExact(position, length - 1)).getObjectContent()
            );
        }

    }

    /**
     * Writes a blob in the bucket.
     *
     * @param inputStream content of the blob to be written
     * @param blobSize    expected size of the blob to be written
     */
    void writeBlob(String blobName, InputStream inputStream, long blobSize) throws OSSException, ClientException, IOException {
        ObjectMetadata meta = new ObjectMetadata();
        meta.setContentLength(blobSize);
        doPrivilegedAndRefreshClient(() -> this.client.putObject(bucket, blobName, inputStream, meta));
    }

    /**
     * Deletes a blob in the bucket
     *
     * @param blobName name of the blob
     */
    void deleteBlob(String blobName) throws OSSException, ClientException, IOException {
        doPrivilegedAndRefreshClient(() -> {
            this.client.deleteObject(bucket, blobName);
            return null;
        });
    }

    public void move(String sourceBlobName, String targetBlobName) throws OSSException, ClientException, IOException {
        doPrivilegedAndRefreshClient(() -> {
            this.client.copyObject(bucket, sourceBlobName, bucket, targetBlobName);
            return null;
        });
        doPrivilegedAndRefreshClient(() -> {
            this.client.deleteObject(bucket, sourceBlobName);
            return null;
        });
    }

    /**
     * Executes a {@link PrivilegedExceptionAction} with privileges enabled.
     */
    <T> T doPrivilegedAndRefreshClient(PrivilegedExceptionAction<T> operation) throws IOException {
        refreshStsOssClient();
        return PermissionHelper.doPrivileged(operation);
    }

    private void refreshStsOssClient() throws IOException {
        if (this.client.isUseStsOssClient()) {
            this.client.refreshStsOssClient();// refresh token to avoid expired
        }
    }
}

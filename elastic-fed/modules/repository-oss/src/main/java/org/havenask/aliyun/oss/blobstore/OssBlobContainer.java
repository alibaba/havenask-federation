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

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.havenask.common.blobstore.BlobContainer;
import org.havenask.common.blobstore.BlobMetadata;
import org.havenask.common.blobstore.BlobPath;
import org.havenask.common.blobstore.BlobStoreException;
import org.havenask.common.blobstore.DeleteResult;
import org.havenask.common.blobstore.support.AbstractBlobContainer;
import org.havenask.common.unit.ByteSizeUnit;
import org.havenask.common.unit.ByteSizeValue;

import com.aliyun.oss.ClientException;
import com.aliyun.oss.OSSException;

/**
 * A class for managing a oss repository of blob entries, where each blob entry is just a named group of bytes
 */
public class OssBlobContainer extends AbstractBlobContainer {
    private static final Logger logger = LogManager.getLogger(OssBlobContainer.class);
    protected final OssBlobStore blobStore;
    protected final String keyPath;

    public OssBlobContainer(BlobPath path, OssBlobStore blobStore) {
        super(path);
        this.keyPath = path.buildAsString();
        this.blobStore = blobStore;
    }

    /**
     * Tests whether a blob with the given blob name exists in the container.
     *
     * @param blobName The name of the blob whose existence is to be determined.
     * @return {@code true} if a blob exists in the BlobContainer with the given name, and {@code false} otherwise.
     */
    @Override
    public boolean blobExists(String blobName) {
        logger.trace("blobExists keyPath {} blobName {}", keyPath, blobName);
        try {
            return blobStore.blobExists(buildKey(blobName));
        } catch (OSSException | ClientException | IOException e) {
            logger.warn("can not access [{}] : {}", blobName, e.getMessage());
            throw new BlobStoreException("Failed to check if blob [" + blobName + "] exists", e);
        }
    }

    /**
     * Creates a new {@link InputStream} for the given blob name.
     *
     * @param blobName The name of the blob to get an {@link InputStream} for.
     * @return The {@code InputStream} to read the blob.
     * @throws NoSuchFileException if the blob does not exist
     * @throws IOException if the blob can not be read.
     */
    @Override
    public InputStream readBlob(String blobName) throws IOException {
        logger.trace("readBlob keyPath {} blobName {}", keyPath, blobName);
        if (!blobExists(blobName)) {
            throw new NoSuchFileException("[" + blobName + "] blob not found");
        }
        return blobStore.readBlob(buildKey(blobName));
    }

    /**
     * Creates a new {@link InputStream} for the given blob name.
     *
     * @param blobName The name of the blob to get an {@link InputStream} for.
     * @param position The position in the blob where the next byte will be read.
     * @param length   An indication of the number of bytes to be read.
     * @return The {@code InputStream} to read the blob.
     * @throws NoSuchFileException if the blob does not exist
     * @throws IOException if the blob can not be read.
     */
    @Override
    public InputStream readBlob(String blobName, long position, long length) throws IOException {
        logger.trace("readBlob keyPath {} blobName {} position {} length {}", keyPath, blobName, position, length);
        if (!blobExists(blobName)) {
            throw new NoSuchFileException("[" + blobName + "] blob not found");
        }
        return blobStore.readBlob(buildKey(blobName), position, length);
    }

    @Override
    public long readBlobPreferredLength() {
        // TODO check OSS bast length
        // This container returns streams that must be fully consumed, so we tell consumers to make bounded requests.
        return new ByteSizeValue(32, ByteSizeUnit.MB).getBytes();
    }

    /**
     * Reads blob content from the input stream and writes it to the container in a new blob with the given name.
     * This method assumes the container does not already contain a blob of the same blobName.  If a blob by the
     * same name already exists, the operation will fail and an {@link IOException} will be thrown.
     *
     * @param blobName            The name of the blob to write the contents of the input stream to.
     * @param inputStream         The input stream from which to retrieve the bytes to write to the blob.
     * @param blobSize            The size of the blob to be written, in bytes.  It is implementation dependent whether
     *                            this value is used in writing the blob to the repository.
     * @param failIfAlreadyExists whether to throw a FileAlreadyExistsException if the given blob already exists
     * @throws FileAlreadyExistsException if failIfAlreadyExists is true and a blob by the same name already exists
     * @throws IOException if the input stream could not be read, or the target blob could not be written to.
     */
    @Override
    public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
        if (blobExists(blobName)) {
            if (failIfAlreadyExists) {
                throw new FileAlreadyExistsException("blob [" + blobName + "] already exists, cannot overwrite");
            } else {
                deleteBlobIgnoringIfNotExists(blobName);
            }
        }
        logger.trace("writeBlob({}, stream, {})", blobName, blobSize);
        blobStore.writeBlob(buildKey(blobName), inputStream, blobSize);
    }

    @Override
    public void writeBlobAtomic(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
        writeBlob(blobName, inputStream, blobSize, failIfAlreadyExists);
    }

    /**
     * Deletes a blob with giving name, if the blob exists.  If the blob does not exist, this method throws an
     * IOException.
     *
     * @param blobName The name of the blob to delete.
     * @throws NoSuchFileException if the blob does not exist
     * @throws IOException if the blob exists but could not be deleted.
     */
    public void deleteBlob(String blobName) throws IOException {
        logger.trace("deleteBlob keyPath {} blobName {}", keyPath, blobName);
        if (!blobExists(blobName)) {
            throw new NoSuchFileException("Blob [" + blobName + "] does not exist");
        }
        try {
            blobStore.deleteBlob(buildKey(blobName));
        } catch (OSSException | ClientException e) {
            logger.warn("can not access [{}] : {}", blobName, e.getMessage());
            throw new IOException(e);
        }

    }

    @Override
    public DeleteResult delete() throws IOException {
        try {
            return blobStore.delete(path());
        } catch (IOException e) {
            throw new IOException("Exception when deleting blob container [" + path().buildAsString() + "]", e);
        }
    }

    @Override
    public void deleteBlobsIgnoringIfNotExists(List<String> blobNames) throws IOException {
        IOException ioe = null;
        for (String blobName : blobNames) {
            try {
                deleteBlobIgnoringIfNotExists(blobName);
            } catch (IOException e) {
                if (ioe == null) {
                    ioe = e;
                } else {
                    ioe.addSuppressed(e);
                }
            }
        }
        if (ioe != null) {
            throw ioe;
        }
    }

    private void deleteBlobIgnoringIfNotExists(String blobName) throws IOException {
        try {
            deleteBlob(blobName);
        } catch (final NoSuchFileException ignored) {
            // This exception is ignored
        }
    }

    /**
     * Lists all blobs in the container.
     *
     * @return A map of all the blobs in the container.  The keys in the map are the names of the blobs and
     * the values are {@link BlobMetadata}, containing basic information about each blob.
     * @throws IOException if there were any failures in reading from the blob container.
     */
    @Override
    public Map<String, BlobMetadata> listBlobs() throws IOException {
        return listBlobsByPrefix(null);
    }

    @Override
    public Map<String, BlobContainer> children() throws IOException {
        try {
            return blobStore.children(path());
        } catch (IOException e) {
            throw new IOException("Failed to list children in path [" + path().buildAsString() + "].", e);
        }
    }

    /**
     * Lists all blobs in the container.
     *
     * @return A map of all the blobs in the container.  The keys in the map are the names of the blobs and
     * the values are {@link BlobMetadata}, containing basic information about each blob.
     * @throws IOException if there were any failures in reading from the blob container.
     */
    @Override
    public Map<String, BlobMetadata> listBlobsByPrefix(String blobNamePrefix) throws IOException {
        logger.trace("listBlobsByPrefix({})", blobNamePrefix);
        try {
            Map<String, BlobMetadata> result = blobStore.listBlobsByPrefix(keyPath, blobNamePrefix);
            logger.trace("listBlobsByPrefix result count {}", result.keySet().size());
            return result;
        } catch (IOException e) {
            logger.warn("can not access [{}] : {}", blobNamePrefix, e.getMessage());
            throw new IOException(e);
        }
    }

    protected String buildKey(String blobName) {
        return keyPath + (blobName == null ? StringUtils.EMPTY : blobName);
    }
}

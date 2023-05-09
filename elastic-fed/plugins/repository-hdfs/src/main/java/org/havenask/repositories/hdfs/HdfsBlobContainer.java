/*
*Copyright (c) 2021, Alibaba Group;
*Licensed under the Apache License, Version 2.0 (the "License");
*you may not use this file except in compliance with the License.
*You may obtain a copy of the License at

*   http://www.apache.org/licenses/LICENSE-2.0

*Unless required by applicable law or agreed to in writing, software
*distributed under the License is distributed on an "AS IS" BASIS,
*WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*See the License for the specific language governing permissions and
*limitations under the License.
*/

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Modifications Copyright Havenask Contributors. See
 * GitHub history for details.
 */

package org.havenask.repositories.hdfs;

import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Options.CreateOpts;
import org.apache.hadoop.fs.Path;
import org.havenask.common.Nullable;
import org.havenask.common.blobstore.BlobContainer;
import org.havenask.common.blobstore.BlobMetadata;
import org.havenask.common.blobstore.BlobPath;
import org.havenask.common.blobstore.DeleteResult;
import org.havenask.common.blobstore.fs.FsBlobContainer;
import org.havenask.common.blobstore.support.AbstractBlobContainer;
import org.havenask.common.blobstore.support.PlainBlobMetadata;
import org.havenask.repositories.hdfs.HdfsBlobStore.Operation;

import java.io.FileNotFoundException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class HdfsBlobContainer extends AbstractBlobContainer {
    private final HdfsBlobStore store;
    private final HdfsSecurityContext securityContext;
    private final Path path;
    private final int bufferSize;

    HdfsBlobContainer(BlobPath blobPath, HdfsBlobStore store, Path path, int bufferSize, HdfsSecurityContext hdfsSecurityContext) {
        super(blobPath);
        this.store = store;
        this.securityContext = hdfsSecurityContext;
        this.path = path;
        this.bufferSize = bufferSize;
    }

    // TODO: See if we can get precise result reporting.
    private static final DeleteResult DELETE_RESULT = new DeleteResult(1L, 0L);

    @Override
    public boolean blobExists(String blobName) throws IOException {
        return store.execute(fileContext -> fileContext.util().exists(new Path(path, blobName)));
    }

    @Override
    public DeleteResult delete() throws IOException {
        store.execute(fileContext -> fileContext.delete(path, true));
        return DELETE_RESULT;
    }

    @Override
    public void deleteBlobsIgnoringIfNotExists(final List<String> blobNames) throws IOException {
        IOException ioe = null;
        for (String blobName : blobNames) {
            try {
                store.execute(fileContext -> fileContext.delete(new Path(path, blobName), true));
            } catch (final FileNotFoundException ignored) {
                // This exception is ignored
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

    @Override
    public InputStream readBlob(String blobName) throws IOException {
        // FSDataInputStream does buffering internally
        // FSDataInputStream can open connections on read() or skip() so we wrap in
        // HDFSPrivilegedInputSteam which will ensure that underlying methods will
        // be called with the proper privileges.
        try {
            return store.execute(fileContext ->
                new HDFSPrivilegedInputSteam(fileContext.open(new Path(path, blobName), bufferSize), securityContext)
            );
        } catch (FileNotFoundException fnfe) {
            throw new NoSuchFileException("[" + blobName + "] blob not found");
        }
    }

    @Override
    public InputStream readBlob(String blobName, long position, long length) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
        Path blob = new Path(path, blobName);
        // we pass CREATE, which means it fails if a blob already exists.
        final EnumSet<CreateFlag> flags = failIfAlreadyExists ? EnumSet.of(CreateFlag.CREATE, CreateFlag.SYNC_BLOCK)
            : EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE, CreateFlag.SYNC_BLOCK);
        store.execute((Operation<Void>) fileContext -> {
            try {
                writeToPath(inputStream, blobSize, fileContext, blob, flags);
            } catch (org.apache.hadoop.fs.FileAlreadyExistsException faee) {
                throw new FileAlreadyExistsException(blob.toString(), null, faee.getMessage());
            }
            return null;
        });
    }

    @Override
    public void writeBlobAtomic(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
        final String tempBlob = FsBlobContainer.tempBlobName(blobName);
        final Path tempBlobPath = new Path(path, tempBlob);
        final Path blob = new Path(path, blobName);
        store.execute((Operation<Void>) fileContext -> {
            writeToPath(inputStream, blobSize, fileContext, tempBlobPath, EnumSet.of(CreateFlag.CREATE, CreateFlag.SYNC_BLOCK));
            try {
                fileContext.rename(tempBlobPath, blob, failIfAlreadyExists ? Options.Rename.NONE : Options.Rename.OVERWRITE);
            } catch (org.apache.hadoop.fs.FileAlreadyExistsException faee) {
                throw new FileAlreadyExistsException(blob.toString(), null, faee.getMessage());
            }
            return null;
        });
    }

    private void writeToPath(InputStream inputStream, long blobSize, FileContext fileContext, Path blobPath,
                             EnumSet<CreateFlag> createFlags) throws IOException {
        final byte[] buffer = new byte[blobSize < bufferSize ? Math.toIntExact(blobSize) : bufferSize];
        try (FSDataOutputStream stream = fileContext.create(blobPath, createFlags, CreateOpts.bufferSize(buffer.length))) {
            int bytesRead;
            while ((bytesRead = inputStream.read(buffer)) != -1) {
                stream.write(buffer, 0, bytesRead);
            }
        }
    }

    @Override
    public Map<String, BlobMetadata> listBlobsByPrefix(@Nullable final String prefix) throws IOException {
        FileStatus[] files = store.execute(fileContext -> fileContext.util().listStatus(path,
            path -> prefix == null || path.getName().startsWith(prefix)));
        Map<String, BlobMetadata> map = new LinkedHashMap<>();
        for (FileStatus file : files) {
            if (file.isFile()) {
                map.put(file.getPath().getName(), new PlainBlobMetadata(file.getPath().getName(), file.getLen()));
            }
        }
        return Collections.unmodifiableMap(map);
    }

    @Override
    public Map<String, BlobMetadata> listBlobs() throws IOException {
        return listBlobsByPrefix(null);
    }

    @Override
    public Map<String, BlobContainer> children() throws IOException {
        FileStatus[] files = store.execute(fileContext -> fileContext.util().listStatus(path));
        Map<String, BlobContainer> map = new LinkedHashMap<>();
        for (FileStatus file : files) {
            if (file.isDirectory()) {
                final String name = file.getPath().getName();
                map.put(name, new HdfsBlobContainer(path().add(name), store, new Path(path, name), bufferSize, securityContext));
            }
        }
        return Collections.unmodifiableMap(map);
    }

    /**
     * Exists to wrap underlying InputStream methods that might make socket connections in
     * doPrivileged blocks. This is due to the way that hdfs client libraries might open
     * socket connections when you are reading from an InputStream.
     */
    private static class HDFSPrivilegedInputSteam extends FilterInputStream {

        private final HdfsSecurityContext securityContext;

        HDFSPrivilegedInputSteam(InputStream in, HdfsSecurityContext hdfsSecurityContext) {
            super(in);
            this.securityContext = hdfsSecurityContext;
        }

        public int read() throws IOException {
            return securityContext.doPrivilegedOrThrow(in::read);
        }

        public int read(byte b[]) throws IOException {
            return securityContext.doPrivilegedOrThrow(() -> in.read(b));
        }

        public int read(byte b[], int off, int len) throws IOException {
            return securityContext.doPrivilegedOrThrow(() -> in.read(b, off, len));
        }

        public long skip(long n) throws IOException {
            return securityContext.doPrivilegedOrThrow(() -> in.skip(n));
        }

        public int available() throws IOException {
            return securityContext.doPrivilegedOrThrow(() -> in.available());
        }

        public synchronized void reset() throws IOException {
            securityContext.doPrivilegedOrThrow(() -> {
                in.reset();
                return null;
            });
        }
    }
}

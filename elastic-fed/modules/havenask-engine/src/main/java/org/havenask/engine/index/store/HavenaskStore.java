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

package org.havenask.engine.index.store;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.OutputStreamIndexOutput;
import org.apache.lucene.util.Version;
import org.havenask.common.Strings;
import org.havenask.engine.index.config.EntryTable;
import org.havenask.engine.index.engine.EngineSettings;
import org.havenask.engine.index.engine.HavenaskEngine.HavenaskCommitInfo;
import org.havenask.engine.util.VersionUtils;
import org.havenask.env.ShardLock;
import org.havenask.index.IndexSettings;
import org.havenask.index.shard.ShardId;
import org.havenask.index.store.Store;
import org.havenask.index.store.StoreFileMetadata;

import static org.apache.lucene.index.IndexFileNames.SEGMENTS;

public class HavenaskStore extends Store {

    public static final Version HAVENASK_VERSION = Version.fromBits(1, 0, 0);
    private static final String HAVENASK_VERSION_FILE_PREFIX = "version.";
    private static final String HAVENASK_ENTRY_TABLE_FILE_PREFIX = "entry_table.";
    private static final int CHUNK_SIZE = 8192;

    private final Path shardPath;

    public HavenaskStore(
        ShardId shardId,
        IndexSettings indexSettings,
        Directory directory,
        ShardLock shardLock,
        OnClose onClose,
        Path shardPath
    ) {
        super(shardId, indexSettings, new HavenaskDirectory(directory, shardPath), shardLock, onClose);
        this.shardPath = shardPath;
    }

    @Override
    public MetadataSnapshot getMetadata(IndexCommit commit) throws IOException {
        MetadataSnapshot luceneSnapshot = super.getMetadata(commit, false);
        Map<String, StoreFileMetadata> metadata = new HashMap<>(luceneSnapshot.asMap());
        if (EngineSettings.isHavenaskEngine(indexSettings.getSettings())) {
            metadata.putAll(getHavenaskMetadata(commit));
        }
        return new MetadataSnapshot(metadata, luceneSnapshot.getCommitUserData(), luceneSnapshot.getNumDocs());
    }

    Map<String, StoreFileMetadata> getHavenaskMetadata(IndexCommit commit) throws IOException {
        long commitVersion = 0;
        if (commit == null) {
            commitVersion = VersionUtils.getMaxVersion(shardPath, commitVersion);
        } else if (commit.getUserData().containsKey(HavenaskCommitInfo.COMMIT_VERSION_KEY)) {
            commitVersion = Long.valueOf(commit.getUserData().get(HavenaskCommitInfo.COMMIT_VERSION_KEY));
        }
        String versionFile = HAVENASK_VERSION_FILE_PREFIX + commitVersion;
        String content = Files.readString(shardPath.resolve(versionFile));
        JSONObject jsonObject = JSON.parseObject(content);
        String fenceName = jsonObject.getString("fence_name");

        String entryTableFile = HAVENASK_ENTRY_TABLE_FILE_PREFIX + commitVersion;
        Path entryTablePath = shardPath.resolve(entryTableFile);
        if (false == Strings.isEmpty(fenceName)) {
            entryTablePath = shardPath.resolve(fenceName).resolve(entryTableFile);
        }
        String entryTableContent = Files.readString(entryTablePath);
        EntryTable entryTable = EntryTable.parse(entryTableContent);

        // TODO 如何处理目录
        Map<String, StoreFileMetadata> metadata = new LinkedHashMap<>();
        entryTable.files.forEach((name, file) -> {
            StoreFileMetadata storeFileMetadata = new StoreFileMetadata(file.name, file.length, "", HAVENASK_VERSION);
            metadata.put(file.name, storeFileMetadata);
        });

        // add entry_table file
        metadata.put(entryTableFile, new StoreFileMetadata(entryTableFile, entryTableContent.length(), "", HAVENASK_VERSION));

        return metadata;
    }

    @Override
    public IndexOutput createVerifyingOutput(String fileName, final StoreFileMetadata metadata, final IOContext context)
        throws IOException {
        if (isHavenaskFile(metadata.writtenBy())) {
            Path filePath = shardPath.resolve(fileName);
            Path fileDir = filePath.getParent();
            if (Files.notExists(fileDir)) {
                Files.createDirectories(fileDir);
            }

            OutputStream os;
            if (metadata.length() < 0) {
                Files.createDirectories(filePath);
                 os = new ByteArrayOutputStream();
            } else {
                 os = Files.newOutputStream(shardPath.resolve(fileName), StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);
            }

            return new OutputStreamIndexOutput(
                "OutputStreamIndexOutput(path=\"" + shardPath.resolve(fileName) + "\")",
                fileName,
                os,
                CHUNK_SIZE
            );
        } else {
            return super.createVerifyingOutput(fileName, metadata, context);
        }
    }

    @Override
    public IndexInput openInput(StoreFileMetadata metadata, IOContext context) throws IOException {
        if (isHavenaskFile(metadata.writtenBy())) {
            Path filePath = shardPath.resolve(metadata.name());
            SeekableByteChannel channel = Files.newByteChannel(filePath, StandardOpenOption.READ);

            return new SimpleFSIndexInput("SimpleFSIndexInput(path=\"" + filePath + "\")", channel, context);
        } else {
            return super.openInput(metadata, context);
        }
    }

    /**
     * 迁移lucene的SimpleFSIndexInput到HavenaskStore类
     */
    static final class SimpleFSIndexInput extends BufferedIndexInput {
        /**
         * The maximum chunk size for reads of 16384 bytes.
         */
        private static final int CHUNK_SIZE = 16384;

        /** the channel we will read from */
        protected final SeekableByteChannel channel;
        /** is this instance a clone and hence does not own the file to close it */
        boolean isClone = false;
        /** start offset: non-zero in the slice case */
        protected final long off;
        /** end offset (start+length) */
        protected final long end;

        private ByteBuffer byteBuf; // wraps the buffer for NIO

        SimpleFSIndexInput(String resourceDesc, SeekableByteChannel channel, IOContext context) throws IOException {
            super(resourceDesc, context);
            this.channel = channel;
            this.off = 0L;
            this.end = channel.size();
        }

        SimpleFSIndexInput(String resourceDesc, SeekableByteChannel channel, long off, long length, int bufferSize) {
            super(resourceDesc, bufferSize);
            this.channel = channel;
            this.off = off;
            this.end = off + length;
            this.isClone = true;
        }

        @Override
        public void close() throws IOException {
            if (!isClone) {
                channel.close();
            }
        }

        @Override
        public SimpleFSIndexInput clone() {
            SimpleFSIndexInput clone = (SimpleFSIndexInput) super.clone();
            clone.isClone = true;
            return clone;
        }

        @Override
        public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
            if (offset < 0 || length < 0 || offset + length > this.length()) {
                throw new IllegalArgumentException(
                    "slice() "
                        + sliceDescription
                        + " out of bounds: offset="
                        + offset
                        + ",length="
                        + length
                        + ",fileLength="
                        + this.length()
                        + ": "
                        + this
                );
            }
            return new SimpleFSIndexInput(getFullSliceDescription(sliceDescription), channel, off + offset, length, getBufferSize());
        }

        @Override
        public long length() {
            return end - off;
        }

        @Override
        protected void readInternal(ByteBuffer b) throws IOException {
            synchronized (channel) {
                long pos = getFilePointer() + off;

                if (pos + b.remaining() > end) {
                    throw new EOFException("read past EOF: " + this);
                }

                try {
                    channel.position(pos);

                    int readLength = b.remaining();
                    while (readLength > 0) {
                        final int toRead = Math.min(CHUNK_SIZE, readLength);
                        b.limit(b.position() + toRead);
                        assert b.remaining() == toRead;
                        final int i = channel.read(b);
                        if (i < 0) { // be defensive here, even though we checked before hand, something could have changed
                            throw new EOFException("read past EOF: " + this + " buffer: " + b + " chunkLen: " + toRead + " end: " + end);
                        }
                        assert i > 0 : "SeekableByteChannel.read with non zero-length bb.remaining() must always read at least"
                            + " one byte (Channel is in blocking mode, see spec of ReadableByteChannel)";
                        pos += i;
                        readLength -= i;
                    }
                    assert readLength == 0;
                } catch (IOException ioe) {
                    throw new IOException(ioe.getMessage() + ": " + this, ioe);
                }
            }
        }

        @Override
        protected void seekInternal(long pos) throws IOException {
            if (pos > length()) {
                throw new EOFException("read past EOF: pos=" + pos + " vs length=" + length() + ": " + this);
            }
        }
    }

    @Override
    public void renameTempFilesSafe(Map<String, String> tempFileMap) throws IOException {
        Map<String, String> havenaskTempFileMap = new HashMap<>(tempFileMap);
        Map<String, String> luceneTempFileMap = new HashMap<>();
        havenaskTempFileMap.forEach((tempFileName, fileName) -> {
            if (fileName.startsWith(SEGMENTS) || Store.isAutogenerated(fileName)) {
                luceneTempFileMap.put(tempFileName, fileName);
            }
        });

        luceneTempFileMap.forEach((tempFileName, fileName) -> havenaskTempFileMap.remove(tempFileName));

        super.renameTempFilesSafe(luceneTempFileMap);
        renameHavenaskTempFilesSafe(havenaskTempFileMap);
    }

    void renameHavenaskTempFilesSafe(Map<String, String> tempFileMap) {
        tempFileMap.forEach((tempFileName, fileName) -> {
            try {
                Path tempFilePath = shardPath.resolve(tempFileName);
                Path filePath = shardPath.resolve(fileName);
                Path fileDir = filePath.getParent();
                if (Files.notExists(fileDir)) {
                    Files.createDirectories(fileDir);
                }
                Files.move(tempFilePath, filePath);
            } catch (IOException e) {
                // TODO check if this is the right thing to do
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public void deleteQuiet(String... files) {
        super.deleteQuiet(files);
        for (String file : files) {
            try {
                Files.deleteIfExists(shardPath.resolve(file));
            } catch (IOException e) {
                // ignore :(
            }
        }
    }

    @Override
    public void cleanupAndVerify(String reason, MetadataSnapshot sourceMetadata) throws IOException {
        // 创建空目录和空文件
        sourceMetadata.asMap().forEach((name, metadata) -> {
            if (name.startsWith(SEGMENTS) || Store.isAutogenerated(name)) {
                return;
            }

            if (Files.notExists(shardPath.resolve(name))) {
                try {
                    if (metadata.length() >= Integer.MAX_VALUE) {
                        Files.createDirectories(shardPath.resolve(name));
                    } else if (metadata.length() == 0) {
                        Files.createFile(shardPath.resolve(name));
                    }
                } catch (IOException e) {
                    logger.warn(new ParameterizedMessage("cleanupAndVerify: failed to create file [{}]", name), e);
                }
            }
        });

        for (String existingFile : directory().listAll()) {
            if (Store.isAutogenerated(existingFile) || sourceMetadata.contains(existingFile)) {
                continue;
            }

            try {
                Files.delete(shardPath.resolve(existingFile));
                logger.debug("cleanupAndVerify: deleted unreferenced file [{}]", existingFile);
            } catch (IOException e) {
                logger.warn(new ParameterizedMessage("cleanupAndVerify: failed to delete unreferenced file [{}]", existingFile), e);
            }
        }
        super.cleanupAndVerify(reason, sourceMetadata);
    }

    public static boolean isHavenaskFile(Version version) {
        return version.major == HAVENASK_VERSION.major;
    }
}

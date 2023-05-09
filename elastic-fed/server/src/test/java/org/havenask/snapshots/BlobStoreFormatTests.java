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

package org.havenask.snapshots;

import org.havenask.HavenaskCorruptionException;
import org.havenask.HavenaskParseException;
import org.havenask.common.blobstore.BlobContainer;
import org.havenask.common.blobstore.BlobMetadata;
import org.havenask.common.blobstore.BlobPath;
import org.havenask.common.blobstore.BlobStore;
import org.havenask.common.blobstore.fs.FsBlobStore;
import org.havenask.common.bytes.BytesArray;
import org.havenask.common.io.Streams;
import org.havenask.common.io.stream.BytesStreamOutput;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.xcontent.ToXContent;
import org.havenask.common.xcontent.ToXContentFragment;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.common.xcontent.XContentParser;
import org.havenask.index.translog.BufferedChecksumStreamOutput;
import org.havenask.repositories.blobstore.ChecksumBlobStoreFormat;
import org.havenask.test.HavenaskTestCase;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;

public class BlobStoreFormatTests extends HavenaskTestCase {

    public static final String BLOB_CODEC = "blob";

    private static class BlobObj implements ToXContentFragment {

        private final String text;

        BlobObj(String text) {
            this.text = text;
        }

        public String getText() {
            return text;
        }

        public static BlobObj fromXContent(XContentParser parser) throws IOException {
            String text = null;
            XContentParser.Token token = parser.currentToken();
            if (token == null) {
                token = parser.nextToken();
            }
            if (token == XContentParser.Token.START_OBJECT) {
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token != XContentParser.Token.FIELD_NAME) {
                        throw new HavenaskParseException("unexpected token [{}]", token);
                    }
                    String currentFieldName = parser.currentName();
                    token = parser.nextToken();
                    if (token.isValue()) {
                        if ("text" .equals(currentFieldName)) {
                            text = parser.text();
                        } else {
                            throw new HavenaskParseException("unexpected field [{}]", currentFieldName);
                        }
                    } else {
                        throw new HavenaskParseException("unexpected token [{}]", token);
                    }
                }
            }
            if (text == null) {
                throw new HavenaskParseException("missing mandatory parameter text");
            }
            return new BlobObj(text);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
            builder.field("text", getText());
            return builder;
        }
    }

    public void testBlobStoreOperations() throws IOException {
        BlobStore blobStore = createTestBlobStore();
        BlobContainer blobContainer = blobStore.blobContainer(BlobPath.cleanPath());
        ChecksumBlobStoreFormat<BlobObj> checksumSMILE = new ChecksumBlobStoreFormat<>(BLOB_CODEC, "%s", BlobObj::fromXContent);

        // Write blobs in different formats
        checksumSMILE.write(new BlobObj("checksum smile"), blobContainer, "check-smile", false);
        checksumSMILE.write(new BlobObj("checksum smile compressed"), blobContainer, "check-smile-comp", true);

        // Assert that all checksum blobs can be read
        assertEquals(checksumSMILE.read(blobContainer, "check-smile", xContentRegistry()).getText(), "checksum smile");
        assertEquals(checksumSMILE.read(blobContainer, "check-smile-comp", xContentRegistry()).getText(), "checksum smile compressed");
    }

    public void testCompressionIsApplied() throws IOException {
        BlobStore blobStore = createTestBlobStore();
        BlobContainer blobContainer = blobStore.blobContainer(BlobPath.cleanPath());
        StringBuilder veryRedundantText = new StringBuilder();
        for (int i = 0; i < randomIntBetween(100, 300); i++) {
            veryRedundantText.append("Blah ");
        }
        ChecksumBlobStoreFormat<BlobObj> checksumFormat = new ChecksumBlobStoreFormat<>(BLOB_CODEC, "%s", BlobObj::fromXContent);
        BlobObj blobObj = new BlobObj(veryRedundantText.toString());
        checksumFormat.write(blobObj, blobContainer, "blob-comp", true);
        checksumFormat.write(blobObj, blobContainer, "blob-not-comp", false);
        Map<String, BlobMetadata> blobs = blobContainer.listBlobsByPrefix("blob-");
        assertEquals(blobs.size(), 2);
        assertThat(blobs.get("blob-not-comp").length(), greaterThan(blobs.get("blob-comp").length()));
    }

    public void testBlobCorruption() throws IOException {
        BlobStore blobStore = createTestBlobStore();
        BlobContainer blobContainer = blobStore.blobContainer(BlobPath.cleanPath());
        String testString = randomAlphaOfLength(randomInt(10000));
        BlobObj blobObj = new BlobObj(testString);
        ChecksumBlobStoreFormat<BlobObj> checksumFormat = new ChecksumBlobStoreFormat<>(BLOB_CODEC, "%s", BlobObj::fromXContent);
        checksumFormat.write(blobObj, blobContainer, "test-path", randomBoolean());
        assertEquals(checksumFormat.read(blobContainer, "test-path", xContentRegistry()).getText(), testString);
        randomCorruption(blobContainer, "test-path");
        try {
            checksumFormat.read(blobContainer, "test-path", xContentRegistry());
            fail("Should have failed due to corruption");
        } catch (HavenaskCorruptionException ex) {
            assertThat(ex.getMessage(), containsString("test-path"));
        } catch (EOFException ex) {
            // This can happen if corrupt the byte length
        }
    }

    protected BlobStore createTestBlobStore() throws IOException {
        return new FsBlobStore(randomIntBetween(1, 8) * 1024, createTempDir(), false);
    }

    protected void randomCorruption(BlobContainer blobContainer, String blobName) throws IOException {
        byte[] buffer = new byte[(int) blobContainer.listBlobsByPrefix(blobName).get(blobName).length()];
        long originalChecksum = checksum(buffer);
        try (InputStream inputStream = blobContainer.readBlob(blobName)) {
            Streams.readFully(inputStream, buffer);
        }
        do {
            int location = randomIntBetween(0, buffer.length - 1);
            buffer[location] = (byte) (buffer[location] ^ 42);
        } while (originalChecksum == checksum(buffer));
        BytesArray bytesArray = new BytesArray(buffer);
        try (StreamInput stream = bytesArray.streamInput()) {
            blobContainer.writeBlob(blobName, stream, bytesArray.length(), false);
        }
    }

    private long checksum(byte[] buffer) throws IOException {
        try (BytesStreamOutput streamOutput = new BytesStreamOutput()) {
            try (BufferedChecksumStreamOutput checksumOutput = new BufferedChecksumStreamOutput(streamOutput)) {
                checksumOutput.write(buffer);
                return checksumOutput.getChecksum();
            }
        }
    }
}

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

package org.havenask.core.internal.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Simple utility methods for file and stream copying.
 * All copy methods use a block size of 4096 bytes,
 * and close all affected streams when done.
 * <p>
 * Mainly for use within the framework,
 * but also useful for application code.
 */
public class Streams {

    private static final ThreadLocal<byte[]> buffer = ThreadLocal.withInitial(() -> new byte[8 * 1024]);

    private Streams() {

    }

    /**
     * Copy the contents of the given InputStream to the given OutputStream. Optionally, closes both streams when done.
     *
     * @param in     the stream to copy from
     * @param out    the stream to copy to
     * @param close  whether to close both streams after copying
     * @param buffer buffer to use for copying
     * @return the number of bytes copied
     * @throws IOException in case of I/O errors
     */
    public static long copy(final InputStream in, final OutputStream out, byte[] buffer, boolean close) throws IOException {
        Exception err = null;
        try {
            long byteCount = 0;
            int bytesRead;
            while ((bytesRead = in.read(buffer)) != -1) {
                out.write(buffer, 0, bytesRead);
                byteCount += bytesRead;
            }
            out.flush();
            return byteCount;
        } catch (IOException | RuntimeException e) {
            err = e;
            throw e;
        } finally {
            if (close) {
                IOUtils.close(err, in, out);
            }
        }
    }

    /**
     * @see #copy(InputStream, OutputStream, byte[], boolean)
     */
    public static long copy(final InputStream in, final OutputStream out, boolean close) throws IOException {
        return copy(in, out, buffer.get(), close);
    }

    /**
     * @see #copy(InputStream, OutputStream, byte[], boolean)
     */
    public static long copy(final InputStream in, final OutputStream out, byte[] buffer) throws IOException {
        return copy(in, out, buffer, true);
    }

    /**
     * @see #copy(InputStream, OutputStream, byte[], boolean)
     */
    public static long copy(final InputStream in, final OutputStream out) throws IOException {
        return copy(in, out, buffer.get(), true);
    }
}

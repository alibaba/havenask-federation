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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.havenask.common.io.stream;

import org.havenask.common.bytes.ReleasableBytesReference;
import org.havenask.common.lease.Releasable;
import org.havenask.common.lease.Releasables;
import org.havenask.common.util.BigArrays;
import org.havenask.common.util.PageCacheRecycler;

/**
 * An bytes stream output that allows providing a {@link BigArrays} instance
 * expecting it to require releasing its content ({@link #bytes()}) once done.
 * <p>
 * Please note, closing this stream will release the bytes that are in use by any
 * {@link ReleasableBytesReference} returned from {@link #bytes()}, so this
 * stream should only be closed after the bytes have been output or copied
 * elsewhere.
 */
public class ReleasableBytesStreamOutput extends BytesStreamOutput implements Releasable {

    public ReleasableBytesStreamOutput(BigArrays bigarrays) {
        this(PageCacheRecycler.PAGE_SIZE_IN_BYTES, bigarrays);
    }

    public ReleasableBytesStreamOutput(int expectedSize, BigArrays bigArrays) {
        super(expectedSize, bigArrays);
    }

    @Override
    public void close() {
        Releasables.close(bytes);
    }
}

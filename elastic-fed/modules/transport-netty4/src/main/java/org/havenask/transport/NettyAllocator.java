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

package org.havenask.transport;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ServerChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.havenask.common.Booleans;
import org.havenask.common.unit.ByteSizeValue;
import org.havenask.monitor.jvm.JvmInfo;

import java.util.concurrent.atomic.AtomicBoolean;

public class NettyAllocator {

    private static final Logger logger = LogManager.getLogger(NettyAllocator.class);
    private static final AtomicBoolean descriptionLogged = new AtomicBoolean(false);

    private static final long SUGGESTED_MAX_ALLOCATION_SIZE;
    private static final ByteBufAllocator ALLOCATOR;
    private static final String DESCRIPTION;

    private static final String USE_UNPOOLED = "havenask.use_unpooled_allocator";
    private static final String USE_NETTY_DEFAULT = "havenask.unsafe.use_netty_default_allocator";
    private static final String USE_NETTY_DEFAULT_CHUNK = "havenask.unsafe.use_netty_default_chunk_and_page_size";

    static {
        if (Booleans.parseBoolean(System.getProperty(USE_NETTY_DEFAULT), false)) {
            ALLOCATOR = ByteBufAllocator.DEFAULT;
            SUGGESTED_MAX_ALLOCATION_SIZE = 1024 * 1024;
            DESCRIPTION = "[name=netty_default, suggested_max_allocation_size=" + new ByteSizeValue(SUGGESTED_MAX_ALLOCATION_SIZE)
                + ", factors={havenask.unsafe.use_netty_default_allocator=true}]";
        } else {
            final long heapSizeInBytes = JvmInfo.jvmInfo().getMem().getHeapMax().getBytes();
            final boolean g1gcEnabled = Boolean.parseBoolean(JvmInfo.jvmInfo().useG1GC());
            final long g1gcRegionSizeInBytes = JvmInfo.jvmInfo().getG1RegionSize();
            final boolean g1gcRegionSizeIsKnown = g1gcRegionSizeInBytes != -1;
            ByteSizeValue heapSize = new ByteSizeValue(heapSizeInBytes);
            ByteSizeValue g1gcRegionSize = new ByteSizeValue(g1gcRegionSizeInBytes);

            ByteBufAllocator delegate;
            if (useUnpooled(heapSizeInBytes, g1gcEnabled, g1gcRegionSizeIsKnown, g1gcRegionSizeInBytes)) {
                delegate = UnpooledByteBufAllocator.DEFAULT;
                if (g1gcEnabled && g1gcRegionSizeIsKnown) {
                    // Suggested max allocation size 1/4 of region size. Guard against unknown edge cases
                    // where this value would be less than 256KB.
                    SUGGESTED_MAX_ALLOCATION_SIZE = Math.max(g1gcRegionSizeInBytes >> 2, 256 * 1024);
                } else {
                    SUGGESTED_MAX_ALLOCATION_SIZE = 1024 * 1024;
                }
                DESCRIPTION = "[name=unpooled, suggested_max_allocation_size=" + new ByteSizeValue(SUGGESTED_MAX_ALLOCATION_SIZE)
                    + ", factors={havenask.unsafe.use_unpooled_allocator=" + System.getProperty(USE_UNPOOLED)
                    + ", g1gc_enabled=" + g1gcEnabled
                    + ", g1gc_region_size=" + g1gcRegionSize
                    + ", heap_size=" + heapSize + "}]";
            } else {
                int nHeapArena = PooledByteBufAllocator.defaultNumHeapArena();
                int pageSize;
                int maxOrder;
                if (useDefaultChunkAndPageSize()) {
                    pageSize = PooledByteBufAllocator.defaultPageSize();
                    maxOrder = PooledByteBufAllocator.defaultMaxOrder();
                } else {
                    pageSize = 8192;
                    if (g1gcEnabled == false || g1gcRegionSizeIsKnown == false || g1gcRegionSizeInBytes >= (4 * 1024 * 1024)) {
                        // This combined with a 8192 page size = 1 MB chunk sizes
                        maxOrder = 7;
                    } else if (g1gcRegionSizeInBytes >= (2 * 1024 * 1024)) {
                        // This combined with a 8192 page size = 512 KB chunk sizes
                        maxOrder = 6;
                    } else {
                        // This combined with a 8192 page size = 256 KB chunk sizes
                        maxOrder = 5;
                    }
                }
                int tinyCacheSize = PooledByteBufAllocator.defaultTinyCacheSize();
                int smallCacheSize = PooledByteBufAllocator.defaultSmallCacheSize();
                int normalCacheSize = PooledByteBufAllocator.defaultNormalCacheSize();
                boolean useCacheForAllThreads = PooledByteBufAllocator.defaultUseCacheForAllThreads();
                delegate = new PooledByteBufAllocator(false, nHeapArena, 0, pageSize, maxOrder, tinyCacheSize,
                    smallCacheSize, normalCacheSize, useCacheForAllThreads);
                int chunkSizeInBytes = pageSize << maxOrder;
                ByteSizeValue chunkSize = new ByteSizeValue(chunkSizeInBytes);
                SUGGESTED_MAX_ALLOCATION_SIZE = chunkSizeInBytes;
                DESCRIPTION = "[name=havenask_configured, chunk_size=" + chunkSize
                    + ", suggested_max_allocation_size=" + new ByteSizeValue(SUGGESTED_MAX_ALLOCATION_SIZE)
                    + ", factors={havenask.unsafe.use_netty_default_chunk_and_page_size=" + useDefaultChunkAndPageSize()
                    + ", g1gc_enabled=" + g1gcEnabled
                    + ", g1gc_region_size=" + g1gcRegionSize + "}]";
            }
            ALLOCATOR = new NoDirectBuffers(delegate);
        }
    }

    public static void logAllocatorDescriptionIfNeeded() {
        if (descriptionLogged.compareAndSet(false, true)) {
            logger.info("creating NettyAllocator with the following configs: " + NettyAllocator.getAllocatorDescription());
        }
    }

    public static ByteBufAllocator getAllocator() {
        return ALLOCATOR;
    }

    public static long suggestedMaxAllocationSize() {
        return SUGGESTED_MAX_ALLOCATION_SIZE;
    }

    public static String getAllocatorDescription() {
        return DESCRIPTION;
    }

    public static Class<? extends Channel> getChannelType() {
        if (ALLOCATOR instanceof NoDirectBuffers) {
            return CopyBytesSocketChannel.class;
        } else {
            return Netty4NioSocketChannel.class;
        }
    }

    public static Class<? extends ServerChannel> getServerChannelType() {
        if (ALLOCATOR instanceof NoDirectBuffers) {
            return CopyBytesServerSocketChannel.class;
        } else {
            return NioServerSocketChannel.class;
        }
    }

    private static boolean useUnpooled(long heapSizeInBytes, boolean g1gcEnabled, boolean g1gcRegionSizeIsKnown, long g1RegionSize) {
        if (userForcedUnpooled()) {
            return true;
        } else if (userForcedPooled()) {
            return true;
        } else if (heapSizeInBytes <= 1 << 30) {
            // If the heap is 1GB or less we use unpooled
            return true;
        } else if (g1gcEnabled == false) {
            return false;
        } else {
            // If the G1GC is enabled and the region size is known and is less than 1MB we use unpooled.
            boolean g1gcRegionIsLessThan1MB = g1RegionSize < 1 << 20;
            return (g1gcRegionSizeIsKnown && g1gcRegionIsLessThan1MB);
        }
    }

    private static boolean userForcedUnpooled() {
        if (System.getProperty(USE_UNPOOLED) != null) {
            return Booleans.parseBoolean(System.getProperty(USE_UNPOOLED));
        } else {
            return false;
        }
    }

    private static boolean userForcedPooled() {
        if (System.getProperty(USE_UNPOOLED) != null) {
            return Booleans.parseBoolean(System.getProperty(USE_UNPOOLED)) == false;
        } else {
            return false;
        }
    }

    private static boolean useDefaultChunkAndPageSize() {
        if (System.getProperty(USE_NETTY_DEFAULT_CHUNK) != null) {
            return Booleans.parseBoolean(System.getProperty(USE_NETTY_DEFAULT_CHUNK));
        } else {
            return false;
        }
    }

    public static class NoDirectBuffers implements ByteBufAllocator {

        private final ByteBufAllocator delegate;

        private NoDirectBuffers(ByteBufAllocator delegate) {
            this.delegate = delegate;
        }

        @Override
        public ByteBuf buffer() {
            return heapBuffer();
        }

        @Override
        public ByteBuf buffer(int initialCapacity) {
            return heapBuffer(initialCapacity);
        }

        @Override
        public ByteBuf buffer(int initialCapacity, int maxCapacity) {
            return heapBuffer(initialCapacity, maxCapacity);
        }

        @Override
        public ByteBuf ioBuffer() {
            return heapBuffer();
        }

        @Override
        public ByteBuf ioBuffer(int initialCapacity) {
            return heapBuffer(initialCapacity);
        }

        @Override
        public ByteBuf ioBuffer(int initialCapacity, int maxCapacity) {
            return heapBuffer(initialCapacity, maxCapacity);
        }

        @Override
        public ByteBuf heapBuffer() {
            return delegate.heapBuffer();
        }

        @Override
        public ByteBuf heapBuffer(int initialCapacity) {
            return delegate.heapBuffer(initialCapacity);
        }

        @Override
        public ByteBuf heapBuffer(int initialCapacity, int maxCapacity) {
            return delegate.heapBuffer(initialCapacity, maxCapacity);
        }

        @Override
        public ByteBuf directBuffer() {
            throw new UnsupportedOperationException("Direct buffers not supported");
        }

        @Override
        public ByteBuf directBuffer(int initialCapacity) {
            throw new UnsupportedOperationException("Direct buffers not supported");
        }

        @Override
        public ByteBuf directBuffer(int initialCapacity, int maxCapacity) {
            throw new UnsupportedOperationException("Direct buffers not supported");
        }

        @Override
        public CompositeByteBuf compositeBuffer() {
            return compositeHeapBuffer();
        }

        @Override
        public CompositeByteBuf compositeBuffer(int maxNumComponents) {
            return compositeHeapBuffer(maxNumComponents);
        }

        @Override
        public CompositeByteBuf compositeHeapBuffer() {
            return delegate.compositeHeapBuffer();
        }

        @Override
        public CompositeByteBuf compositeHeapBuffer(int maxNumComponents) {
            return delegate.compositeHeapBuffer(maxNumComponents);
        }

        @Override
        public CompositeByteBuf compositeDirectBuffer() {
            throw new UnsupportedOperationException("Direct buffers not supported.");
        }

        @Override
        public CompositeByteBuf compositeDirectBuffer(int maxNumComponents) {
            throw new UnsupportedOperationException("Direct buffers not supported.");
        }

        @Override
        public boolean isDirectBufferPooled() {
            assert delegate.isDirectBufferPooled() == false;
            return false;
        }

        @Override
        public int calculateNewCapacity(int minNewCapacity, int maxCapacity) {
            return delegate.calculateNewCapacity(minNewCapacity, maxCapacity);
        }

        public ByteBufAllocator getDelegate() {
            return delegate;
        }
    }
}

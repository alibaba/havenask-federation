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

package org.havenask.engine.search.internal;

import org.havenask.common.lease.Releasable;
import org.havenask.common.lease.Releasables;
import org.havenask.engine.search.dsl.DSLSession;
import org.havenask.threadpool.ThreadPool;

import java.util.concurrent.atomic.AtomicLong;

public class HavenaskScrollContext {
    private final String scrollSessionId;
    private final ThreadPool threadPool;
    private final DSLSession dslSession;
    private final AtomicLong keepAlive;
    private final AtomicLong lastAccessTime;
    private final long startTimeInNano = System.nanoTime();

    public HavenaskScrollContext(ThreadPool threadPool, DSLSession dslSession, long keepAliveInMillis) {
        this.scrollSessionId = dslSession.getSessionId();
        this.threadPool = threadPool;
        this.dslSession = dslSession;
        this.keepAlive = new AtomicLong(keepAliveInMillis);
        this.lastAccessTime = new AtomicLong(nowInMillis());
    }

    public DSLSession getDSLSession() {
        return dslSession;
    }

    private void tryUpdateKeepAlive(long keepAlive) {
        this.keepAlive.updateAndGet(curr -> Math.max(curr, keepAlive));
    }

    private long nowInMillis() {
        return threadPool.relativeTimeInMillis();
    }

    public String getScrollSessionId() {
        return scrollSessionId;
    }

    public Releasable markAsUsed(long keepAliveInMillis) {
        tryUpdateKeepAlive(keepAliveInMillis);
        return Releasables.releaseOnce(() -> { this.lastAccessTime.updateAndGet(curr -> Math.max(curr, nowInMillis())); });
    }

    public boolean isExpired() {
        final long elapsed = nowInMillis() - lastAccessTime.get();
        return elapsed > keepAlive.get();
    }

    public long getStartTimeInNano() {
        return startTimeInNano;
    }
}

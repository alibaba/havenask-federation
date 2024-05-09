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

package org.havenask.engine;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.havenask.cluster.service.ClusterService;
import org.havenask.common.component.AbstractLifecycleComponent;
import org.havenask.common.inject.Inject;
import org.havenask.common.settings.Settings;
import org.havenask.common.unit.TimeValue;
import org.havenask.engine.search.internal.HavenaskScrollContext;
import org.havenask.search.Scroll;
import org.havenask.search.SearchService;
import org.havenask.threadpool.Scheduler.Cancellable;
import org.havenask.threadpool.ThreadPool;

import java.util.concurrent.ConcurrentHashMap;

import static org.havenask.search.SearchService.KEEPALIVE_INTERVAL_SETTING;

public class HavenaskScrollService extends AbstractLifecycleComponent {
    private static final Logger logger = LogManager.getLogger(HavenaskScrollService.class);
    private ThreadPool threadPool;
    private Settings settings;
    private volatile long defaultKeepAlive;
    private volatile long maxKeepAlive;
    private final Cancellable keepAliveReaper;
    private final ConcurrentHashMap<String, HavenaskScrollContext> activeScrollContexts = new ConcurrentHashMap<>();

    @Inject
    public HavenaskScrollService(ClusterService clusterService, ThreadPool threadPool) {
        settings = clusterService.getSettings();
        this.threadPool = threadPool;
        TimeValue keepAliveInterval = KEEPALIVE_INTERVAL_SETTING.get(settings);
        this.keepAliveReaper = threadPool.scheduleWithFixedDelay(new Reaper(), keepAliveInterval, ThreadPool.Names.SAME);
        setKeepAlives(SearchService.DEFAULT_KEEPALIVE_SETTING.get(settings), SearchService.MAX_KEEPALIVE_SETTING.get(settings));
    }

    @Override
    protected void doStart() {}

    @Override
    protected void doStop() {
        removeAllHavenaskScrollContext();
    }

    @Override
    protected void doClose() {
        doStop();
        keepAliveReaper.cancel();
    }

    public void putScrollContext(HavenaskScrollContext havenaskScrollContext) {
        final HavenaskScrollContext previous = activeScrollContexts.put(havenaskScrollContext.getScrollSessionId(), havenaskScrollContext);
        assert previous == null;
    }

    public HavenaskScrollContext removeScrollContext(String scrollSessionId) {
        return activeScrollContexts.remove(scrollSessionId);
    }

    public HavenaskScrollContext getScrollContext(String scrollSessionId) {
        return activeScrollContexts.get(scrollSessionId);
    }

    public void removeAllHavenaskScrollContext() {
        for (HavenaskScrollContext havenaskScrollContext : activeScrollContexts.values()) {
            removeScrollContext(havenaskScrollContext.getScrollSessionId());
        }
    }

    private void validateKeepAlives(TimeValue defaultKeepAlive, TimeValue maxKeepAlive) {
        if (defaultKeepAlive.millis() > maxKeepAlive.millis()) {
            throw new IllegalArgumentException(
                "Default keep alive setting for request ["
                    + SearchService.DEFAULT_KEEPALIVE_SETTING.getKey()
                    + "]"
                    + " should be smaller than max keep alive ["
                    + SearchService.MAX_KEEPALIVE_SETTING.getKey()
                    + "], "
                    + "was ("
                    + defaultKeepAlive
                    + " > "
                    + maxKeepAlive
                    + ")"
            );
        }
    }

    private void setKeepAlives(TimeValue defaultKeepAlive, TimeValue maxKeepAlive) {
        validateKeepAlives(defaultKeepAlive, maxKeepAlive);
        this.defaultKeepAlive = defaultKeepAlive.millis();
        this.maxKeepAlive = maxKeepAlive.millis();
    }

    public long getScrollKeepAlive(Scroll scroll) {
        if (scroll != null && scroll.keepAlive() != null) {
            checkKeepAliveLimit(scroll.keepAlive().millis());
            return scroll.keepAlive().getMillis();
        }
        return defaultKeepAlive;
    }

    private void checkKeepAliveLimit(long keepAlive) {
        if (keepAlive > maxKeepAlive) {
            throw new IllegalArgumentException(
                "Keep alive for request ("
                    + TimeValue.timeValueMillis(keepAlive)
                    + ") is too large. "
                    + "It must be less than ("
                    + TimeValue.timeValueMillis(maxKeepAlive)
                    + "). "
                    + "This limit can be set by changing the ["
                    + SearchService.MAX_KEEPALIVE_SETTING.getKey()
                    + "] cluster level setting."
            );
        }
    }

    public int getActiveContextSize() {
        return activeScrollContexts.size();
    }

    public ThreadPool getThreadPool() {
        return threadPool;
    }

    class Reaper implements Runnable {
        @Override
        public void run() {
            for (HavenaskScrollContext havenaskScrollContext : activeScrollContexts.values()) {
                if (havenaskScrollContext.isExpired()) {
                    logger.debug("freeing HavenaskScrollContext [{}]", havenaskScrollContext.getScrollSessionId());
                    removeScrollContext(havenaskScrollContext.getScrollSessionId());
                }
            }
        }
    }
}

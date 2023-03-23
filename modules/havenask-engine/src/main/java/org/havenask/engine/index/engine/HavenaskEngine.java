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

package org.havenask.engine.index.engine;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.havenask.engine.HavenaskEngineEnvironment;
import org.havenask.engine.index.config.TargetInfo;
import org.havenask.engine.index.config.generator.BizConfigGenerator;
import org.havenask.engine.index.config.generator.TableConfigGenerator;
import org.havenask.engine.rpc.HeartbeatTargetResponse;
import org.havenask.engine.rpc.SearcherClient;
import org.havenask.engine.rpc.UpdateHeartbeatTargetRequest;
import org.havenask.index.engine.EngineConfig;
import org.havenask.index.engine.InternalEngine;

public class HavenaskEngine extends InternalEngine {

    private final SearcherClient searcherClient;
    private final HavenaskEngineEnvironment env;

    public HavenaskEngine(EngineConfig engineConfig, SearcherClient searcherClient, HavenaskEngineEnvironment env) {
        super(engineConfig);
        this.searcherClient = searcherClient;
        this.env = env;

        // 加载配置表
        try {
            activeTable();
        } catch (IOException e) {
            // TODO
        }
    }

    /**
     * TODO 如何像es一样,解决在关闭engine时,不影响正在进行的查询请求
     */
    @Override
    protected final void closeNoLock(String reason, CountDownLatch closedLatch) {
        super.closeNoLock(reason, closedLatch);
        try {
            inactiveTable();
        } catch (IOException e) {
            // TODO
        }
    }

    /**
     * 加载数据表
     * TODO 注意加锁,防止并发更新冲突
     * @throws IOException
     */
    private synchronized void activeTable() throws IOException {
        BizConfigGenerator.generateBiz(engineConfig, env.getConfigPath());
        TableConfigGenerator.generateTable(engineConfig, env.getConfigPath());
        // TODO 调整具体参数
        TargetInfo targetInfo = TargetInfo.createSearchDefault("in0", "", "", "");
        UpdateHeartbeatTargetRequest request = new UpdateHeartbeatTargetRequest(targetInfo);

        long timeout = 300000;
        while (timeout > 0) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                // TODO
                // e.printStackTrace();
            }
            timeout -= 100;
            HeartbeatTargetResponse response = searcherClient.updateHeartbeatTarget(request);
            if (response.getSignature().equals(targetInfo)) {
                logger.info("table [{}] is ready for search", shardId);
                return;
            }
        }
    }

    /**
     * 卸载数据表
     * @throws IOException
     */
    private synchronized void inactiveTable() throws IOException {
        BizConfigGenerator.removeBiz(engineConfig, env.getConfigPath());
        TableConfigGenerator.removeTable(engineConfig, env.getConfigPath());

        // TODO 调整具体参数
        TargetInfo targetInfo = TargetInfo.createSearchDefault("in0", "", "", "");
        UpdateHeartbeatTargetRequest request = new UpdateHeartbeatTargetRequest(targetInfo);
        long timeout = 300000;
        while (timeout > 0) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                // TODO
            }
            timeout -= 100;
            HeartbeatTargetResponse response = searcherClient.updateHeartbeatTarget(request);
            if (response.getSignature().equals(targetInfo)) {
                logger.info("table [{}] has removed from searcher", shardId);
                return;
            }
        }
    }
}

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
import org.havenask.engine.NativeProcessControlService;
import org.havenask.engine.index.config.generator.BizConfigGenerator;
import org.havenask.engine.index.config.generator.TableConfigGenerator;
import org.havenask.engine.rpc.SearcherClient;
import org.havenask.index.engine.EngineConfig;
import org.havenask.index.engine.InternalEngine;

public class HavenaskEngine extends InternalEngine {

    private final SearcherClient searcherClient;
    private final HavenaskEngineEnvironment env;
    private final NativeProcessControlService nativeProcessControlService;

    public HavenaskEngine(
        EngineConfig engineConfig,
        SearcherClient searcherClient,
        HavenaskEngineEnvironment env,
        NativeProcessControlService nativeProcessControlService
    ) {
        super(engineConfig);
        this.searcherClient = searcherClient;
        this.env = env;
        this.nativeProcessControlService = nativeProcessControlService;

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
    private void activeTable() throws IOException {
        BizConfigGenerator.generateBiz(engineConfig, env.getConfigPath());
        TableConfigGenerator.generateTable(engineConfig, env.getConfigPath());
        // 更新配置表信息
        nativeProcessControlService.updateDataNodeTarget();
    }

    /**
     * 卸载数据表
     * @throws IOException
     */
    private synchronized void inactiveTable() throws IOException {
        BizConfigGenerator.removeBiz(engineConfig, env.getConfigPath());
        TableConfigGenerator.removeTable(engineConfig, env.getConfigPath());
        // 更新配置表信息
        nativeProcessControlService.updateDataNodeTarget();
    }
}

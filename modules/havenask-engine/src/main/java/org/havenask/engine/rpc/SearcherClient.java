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

package org.havenask.engine.rpc;

import java.io.IOException;

public interface SearcherClient {
    /**
     * 获取searcher heartbeat target数据
     * @return Heartbeat target响应结果
     */
    HeartbeatTargetResponse getHeartbeatTarget() throws IOException;

    /**
     * 更新searcher heartbeat target
     * @param request 更新请求
     * @return 更新searcher heartbeat target的响应结果
     */
    HeartbeatTargetResponse updateHeartbeatTarget(UpdateHeartbeatTargetRequest request) throws IOException;
}

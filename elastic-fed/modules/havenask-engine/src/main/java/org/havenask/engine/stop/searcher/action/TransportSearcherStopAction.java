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

package org.havenask.engine.stop.searcher.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.havenask.action.ActionListener;
import org.havenask.action.support.ActionFilters;
import org.havenask.action.support.HandledTransportAction;
import org.havenask.common.inject.Inject;
import org.havenask.common.unit.TimeValue;
import org.havenask.engine.search.action.TransportHavenaskSqlAction;
import org.havenask.rest.RestStatus;
import org.havenask.tasks.Task;
import org.havenask.threadpool.ThreadPool;
import org.havenask.transport.TransportService;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.TimeUnit;

public class TransportSearcherStopAction extends HandledTransportAction<SearcherStopRequest, SearcherStopResponse> {

    private static final Logger logger = LogManager.getLogger(TransportHavenaskSqlAction.class);
    private static final String stopSearcherCommand = "ps -ef | grep searcher | grep -v grep | awk '{print $2}' | xargs kill -9";
    private final TimeValue commandTimeout;

    @Inject
    public TransportSearcherStopAction(TransportService transportService, ActionFilters actionFilters) {
        super(SearcherStopAction.NAME, transportService, actionFilters, SearcherStopRequest::new, ThreadPool.Names.SEARCH);
        commandTimeout = TimeValue.timeValueSeconds(10);
    }

    @Override
    protected void doExecute(Task task, SearcherStopRequest request, ActionListener<SearcherStopResponse> listener) {
        // use shell to kill the process
        try {
            boolean success = runCommand(stopSearcherCommand);
            if (success) {
                listener.onResponse(new SearcherStopResponse("kill searcher success", RestStatus.OK.getStatus()));
            } else {
                listener.onResponse(new SearcherStopResponse("kill searcher failed", RestStatus.EXPECTATION_FAILED.getStatus()));
            }
        } catch (Exception e) {
            listener.onResponse(new SearcherStopResponse("exception occur :" + e, RestStatus.EXPECTATION_FAILED.getStatus()));
        }

    }

    private boolean runCommand(String command) {
        return AccessController.doPrivileged((PrivilegedAction<Boolean>) () -> {
            try {
                logger.debug("run command: {}", command);
                long start = System.currentTimeMillis();
                Process process = Runtime.getRuntime().exec(new String[] { "sh", "-c", command });
                boolean timeout = process.waitFor(commandTimeout.seconds(), TimeUnit.SECONDS);
                if (false == timeout) {
                    logger.warn("run command timeout, command: {}", command);
                    process.destroy();
                    return false;
                }
                if (process.exitValue() != 0) {
                    try (InputStream inputStream = process.getInputStream()) {
                        byte[] bytes = inputStream.readAllBytes();
                        String result = new String(bytes, StandardCharsets.UTF_8);
                        logger.warn("run command {} failed, exit value: {}, failed reason: {}", command, process.exitValue(), result);
                    }
                    return false;
                } else {
                    // logger success
                    logger.info(
                        "run command success, cost [{}], command: [{}]",
                        TimeValue.timeValueMillis(System.currentTimeMillis() - start),
                        command
                    );
                    return true;
                }
            } catch (Exception e) {
                logger.warn(() -> new ParameterizedMessage("run command {} unexpected failed", command), e);
            }
            return false;
        });
    }
}

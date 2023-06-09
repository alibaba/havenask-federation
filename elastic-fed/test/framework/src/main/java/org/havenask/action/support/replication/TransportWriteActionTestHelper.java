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

package org.havenask.action.support.replication;

import org.apache.logging.log4j.Logger;
import org.havenask.action.support.WriteRequest;
import org.havenask.common.Nullable;
import org.havenask.index.shard.IndexShard;
import org.havenask.index.translog.Translog;
import org.havenask.action.support.replication.TransportWriteAction;

import java.util.concurrent.CountDownLatch;

public abstract class TransportWriteActionTestHelper {


    public static void performPostWriteActions(final IndexShard indexShard,
                                              final WriteRequest<?> request,
                                              @Nullable final Translog.Location location,
                                              final Logger logger) {
        final CountDownLatch latch = new CountDownLatch(1);
        TransportWriteAction.RespondingWriteResult writerResult = new TransportWriteAction.RespondingWriteResult() {
            @Override
            public void onSuccess(boolean forcedRefresh) {
                latch.countDown();
            }

            @Override
            public void onFailure(Exception ex) {
                throw new AssertionError(ex);
            }
        };
        new TransportWriteAction.AsyncAfterWriteAction(indexShard, request, location, writerResult, logger).run();
        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new AssertionError(e);
        }
    }
}

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

package org.havenask.index.reindex.spi;

import org.havenask.action.ActionListener;
import org.havenask.index.reindex.BulkByScrollResponse;
import org.havenask.index.reindex.ReindexRequest;

/**
 * This interface provides an extension point for {@link org.havenask.index.reindex.ReindexPlugin}.
 * This interface can be implemented to provide a custom Rest interceptor and {@link ActionListener}
 * The Rest interceptor can be used to pre-process any reindex request and perform any action
 * on the response. The ActionListener listens to the success and failure events on every reindex request
 * and can be used to take any actions based on the success or failure.
 */
public interface RemoteReindexExtension {
    /**
     * Get an InterceptorProvider.
     * @return ReindexRestInterceptorProvider implementation.
     */
    ReindexRestInterceptorProvider getInterceptorProvider();

    /**
     * Get a wrapper of ActionListener which is can used to perform any action based on
     * the success/failure of the remote reindex call.
     * @return ActionListener wrapper implementation.
     */
    ActionListener<BulkByScrollResponse> getRemoteReindexActionListener(ActionListener<BulkByScrollResponse> listener,
        ReindexRequest reindexRequest);
}


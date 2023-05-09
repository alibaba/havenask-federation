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

package org.havenask.action.support.master;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.havenask.action.ActionListener;
import org.havenask.action.ActionListenerResponseHandler;
import org.havenask.action.ActionResponse;
import org.havenask.action.ActionRunnable;
import org.havenask.action.support.ActionFilters;
import org.havenask.action.support.HandledTransportAction;
import org.havenask.cluster.ClusterState;
import org.havenask.cluster.ClusterStateObserver;
import org.havenask.cluster.MasterNodeChangePredicate;
import org.havenask.cluster.NotMasterException;
import org.havenask.cluster.block.ClusterBlockException;
import org.havenask.cluster.coordination.FailedToCommitClusterStateException;
import org.havenask.cluster.metadata.IndexNameExpressionResolver;
import org.havenask.cluster.node.DiscoveryNode;
import org.havenask.cluster.node.DiscoveryNodes;
import org.havenask.cluster.service.ClusterService;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.io.stream.Writeable;
import org.havenask.common.unit.TimeValue;
import org.havenask.discovery.MasterNotDiscoveredException;
import org.havenask.node.NodeClosedException;
import org.havenask.tasks.Task;
import org.havenask.threadpool.ThreadPool;
import org.havenask.transport.ConnectTransportException;
import org.havenask.transport.RemoteTransportException;
import org.havenask.transport.TransportException;
import org.havenask.transport.TransportService;

import java.io.IOException;
import java.util.function.Predicate;

/**
 * A base class for operations that needs to be performed on the master node.
 */
public abstract class TransportMasterNodeAction<Request extends MasterNodeRequest<Request>, Response extends ActionResponse>
    extends HandledTransportAction<Request, Response> {

    private static final Logger logger = LogManager.getLogger(TransportMasterNodeAction.class);

    protected final ThreadPool threadPool;
    protected final TransportService transportService;
    protected final ClusterService clusterService;
    protected final IndexNameExpressionResolver indexNameExpressionResolver;

    private final String executor;

    protected TransportMasterNodeAction(String actionName, TransportService transportService,
                                        ClusterService clusterService, ThreadPool threadPool, ActionFilters actionFilters,
                                        Writeable.Reader<Request> request, IndexNameExpressionResolver indexNameExpressionResolver) {
        this(actionName, true, transportService, clusterService, threadPool, actionFilters, request, indexNameExpressionResolver);
    }

    protected TransportMasterNodeAction(String actionName, boolean canTripCircuitBreaker,
                                        TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
                                        ActionFilters actionFilters, Writeable.Reader<Request> request,
                                        IndexNameExpressionResolver indexNameExpressionResolver) {
        super(actionName, canTripCircuitBreaker, transportService, actionFilters, request);
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.executor = executor();
    }

    protected abstract String executor();

    protected abstract Response read(StreamInput in) throws IOException;

    protected abstract void masterOperation(Request request, ClusterState state, ActionListener<Response> listener) throws Exception;

    /**
     * Override this operation if access to the task parameter is needed
     */
    protected void masterOperation(Task task, Request request, ClusterState state, ActionListener<Response> listener) throws Exception {
        masterOperation(request, state, listener);
    }

    protected boolean localExecute(Request request) {
        return false;
    }

    protected abstract ClusterBlockException checkBlock(Request request, ClusterState state);

    @Override
    protected void doExecute(Task task, final Request request, ActionListener<Response> listener) {
        ClusterState state = clusterService.state();
        logger.trace("starting processing request [{}] with cluster state version [{}]", request, state.version());
        if (task != null) {
            request.setParentTask(clusterService.localNode().getId(), task.getId());
        }
        new AsyncSingleAction(task, request, listener).doStart(state);
    }

    class AsyncSingleAction {

        private final ActionListener<Response> listener;
        private final Request request;
        private ClusterStateObserver observer;
        private final long startTime;
        private final Task task;

        AsyncSingleAction(Task task, Request request, ActionListener<Response> listener) {
            this.task = task;
            this.request = request;
            this.listener = listener;
            this.startTime = threadPool.relativeTimeInMillis();
        }

        protected void doStart(ClusterState clusterState) {
            try {
                final DiscoveryNodes nodes = clusterState.nodes();
                if (nodes.isLocalNodeElectedMaster() || localExecute(request)) {
                    // check for block, if blocked, retry, else, execute locally
                    final ClusterBlockException blockException = checkBlock(request, clusterState);
                    if (blockException != null) {
                        if (!blockException.retryable()) {
                            listener.onFailure(blockException);
                        } else {
                            logger.debug("can't execute due to a cluster block, retrying", blockException);
                            retry(clusterState, blockException, newState -> {
                                try {
                                    ClusterBlockException newException = checkBlock(request, newState);
                                    return (newException == null || !newException.retryable());
                                } catch (Exception e) {
                                    // accept state as block will be rechecked by doStart() and listener.onFailure() then called
                                    logger.trace("exception occurred during cluster block checking, accepting state", e);
                                    return true;
                                }
                            });
                        }
                    } else {
                        ActionListener<Response> delegate = ActionListener.delegateResponse(listener, (delegatedListener, t) -> {
                            if (t instanceof FailedToCommitClusterStateException || t instanceof NotMasterException) {
                                logger.debug(() -> new ParameterizedMessage("master could not publish cluster state or " +
                                    "stepped down before publishing action [{}], scheduling a retry", actionName), t);
                                retryOnMasterChange(clusterState, t);
                            } else {
                                delegatedListener.onFailure(t);
                            }
                        });
                        threadPool.executor(executor)
                            .execute(ActionRunnable.wrap(delegate, l -> masterOperation(task, request, clusterState, l)));
                    }
                } else {
                    if (nodes.getMasterNode() == null) {
                        logger.debug("no known master node, scheduling a retry");
                        retryOnMasterChange(clusterState, null);
                    } else {
                        DiscoveryNode masterNode = nodes.getMasterNode();
                        final String actionName = getMasterActionName(masterNode);
                        transportService.sendRequest(masterNode, actionName, request,
                            new ActionListenerResponseHandler<Response>(listener, TransportMasterNodeAction.this::read) {
                                @Override
                                public void handleException(final TransportException exp) {
                                    Throwable cause = exp.unwrapCause();
                                    if (cause instanceof ConnectTransportException ||
                                        (exp instanceof RemoteTransportException && cause instanceof NodeClosedException)) {
                                        // we want to retry here a bit to see if a new master is elected
                                        logger.debug("connection exception while trying to forward request with action name [{}] to " +
                                                "master node [{}], scheduling a retry. Error: [{}]",
                                            actionName, nodes.getMasterNode(), exp.getDetailedMessage());
                                        retryOnMasterChange(clusterState, cause);
                                    } else {
                                        listener.onFailure(exp);
                                    }
                                }
                        });
                    }
                }
            } catch (Exception e) {
                listener.onFailure(e);
            }
        }

        private void retryOnMasterChange(ClusterState state, Throwable failure) {
            retry(state, failure, MasterNodeChangePredicate.build(state));
        }

        private void retry(ClusterState state, final Throwable failure, final Predicate<ClusterState> statePredicate) {
            if (observer == null) {
                final long remainingTimeoutMS = request.masterNodeTimeout().millis() - (threadPool.relativeTimeInMillis() - startTime);
                if (remainingTimeoutMS <= 0) {
                    logger.debug(() -> new ParameterizedMessage("timed out before retrying [{}] after failure", actionName), failure);
                    listener.onFailure(new MasterNotDiscoveredException(failure));
                    return;
                }
                this.observer = new ClusterStateObserver(
                        state, clusterService, TimeValue.timeValueMillis(remainingTimeoutMS), logger, threadPool.getThreadContext());
            }
            observer.waitForNextChange(
                new ClusterStateObserver.Listener() {
                    @Override
                    public void onNewClusterState(ClusterState state) {
                        doStart(state);
                    }

                    @Override
                    public void onClusterServiceClose() {
                        listener.onFailure(new NodeClosedException(clusterService.localNode()));
                    }

                    @Override
                    public void onTimeout(TimeValue timeout) {
                        logger.debug(() -> new ParameterizedMessage("timed out while retrying [{}] after failure (timeout [{}])",
                            actionName, timeout), failure);
                        listener.onFailure(new MasterNotDiscoveredException(failure));
                    }
                }, statePredicate
            );
        }
    }

    /**
     * Allows to conditionally return a different master node action name in the case an action gets renamed.
     * This mainly for backwards compatibility should be used rarely
     */
    protected String getMasterActionName(DiscoveryNode node) {
        return actionName;
    }
}

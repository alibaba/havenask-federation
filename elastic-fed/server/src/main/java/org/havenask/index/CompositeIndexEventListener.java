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

package org.havenask.index;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.havenask.cluster.routing.ShardRouting;
import org.havenask.common.Nullable;
import org.havenask.common.logging.Loggers;
import org.havenask.common.settings.Settings;
import org.havenask.index.shard.IndexEventListener;
import org.havenask.index.shard.IndexShard;
import org.havenask.index.shard.IndexShardState;
import org.havenask.index.shard.ShardId;
import org.havenask.indices.cluster.IndicesClusterStateService.AllocatedIndices.IndexRemovalReason;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * A composite {@link IndexEventListener} that forwards all callbacks to an immutable list of IndexEventListener
 */
final class CompositeIndexEventListener implements IndexEventListener {

    private final List<IndexEventListener> listeners;
    private final Logger logger;

    CompositeIndexEventListener(IndexSettings indexSettings, Collection<IndexEventListener> listeners) {
        for (IndexEventListener listener : listeners) {
            if (listener == null) {
                throw new IllegalArgumentException("listeners must be non-null");
            }
        }
        this.listeners = Collections.unmodifiableList(new ArrayList<>(listeners));
        this.logger = Loggers.getLogger(getClass(), indexSettings.getIndex());
    }

    @Override
    public void shardRoutingChanged(IndexShard indexShard, @Nullable ShardRouting oldRouting, ShardRouting newRouting) {
        for (IndexEventListener listener : listeners) {
            try {
                listener.shardRoutingChanged(indexShard, oldRouting, newRouting);
            } catch (Exception e) {
                logger.warn(() -> new ParameterizedMessage("[{}] failed to invoke shard touring changed callback",
                    indexShard.shardId().getId()), e);
            }
        }
    }

    @Override
    public void afterIndexShardCreated(IndexShard indexShard) {
        for (IndexEventListener listener : listeners) {
            try {
                listener.afterIndexShardCreated(indexShard);
            } catch (Exception e) {
                logger.warn(() -> new ParameterizedMessage("[{}] failed to invoke after shard created callback",
                    indexShard.shardId().getId()), e);
                throw e;
            }
        }
    }

    @Override
    public void afterIndexShardStarted(IndexShard indexShard) {
        for (IndexEventListener listener : listeners) {
            try {
                listener.afterIndexShardStarted(indexShard);
            } catch (Exception e) {
                logger.warn(() -> new ParameterizedMessage("[{}] failed to invoke after shard started callback",
                    indexShard.shardId().getId()), e);
                throw e;
            }
        }
    }

    @Override
    public void beforeIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard,
                                       Settings indexSettings) {
        for (IndexEventListener listener : listeners) {
            try {
                listener.beforeIndexShardClosed(shardId, indexShard, indexSettings);
            } catch (Exception e) {
                logger.warn(() -> new ParameterizedMessage("[{}] failed to invoke before shard closed callback",
                    shardId.getId()), e);
                throw e;
            }
        }
    }

    @Override
    public void afterIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard,
                                      Settings indexSettings) {
        for (IndexEventListener listener : listeners) {
            try {
                listener.afterIndexShardClosed(shardId, indexShard, indexSettings);
            } catch (Exception e) {
                logger.warn(() -> new ParameterizedMessage("[{}] failed to invoke after shard closed callback",
                    shardId.getId()), e);
                throw e;
            }
        }
    }

    @Override
    public void onShardInactive(IndexShard indexShard) {
        for (IndexEventListener listener : listeners) {
            try {
                listener.onShardInactive(indexShard);
            } catch (Exception e) {
                logger.warn(() -> new ParameterizedMessage("[{}] failed to invoke on shard inactive callback",
                    indexShard.shardId().getId()), e);
                throw e;
            }
        }
    }

    @Override
    public void indexShardStateChanged(IndexShard indexShard, @Nullable IndexShardState previousState, IndexShardState currentState,
                                       @Nullable String reason) {
        for (IndexEventListener listener : listeners) {
            try {
                listener.indexShardStateChanged(indexShard, previousState, indexShard.state(), reason);
            } catch (Exception e) {
                logger.warn(() -> new ParameterizedMessage("[{}] failed to invoke index shard state changed callback",
                    indexShard.shardId().getId()), e);
                throw e;
            }
        }
    }

    @Override
    public void beforeIndexCreated(Index index, Settings indexSettings) {
        for (IndexEventListener listener : listeners) {
            try {
                listener.beforeIndexCreated(index, indexSettings);
            } catch (Exception e) {
                logger.warn("failed to invoke before index created callback", e);
                throw e;
            }
        }
    }

    @Override
    public void afterIndexCreated(IndexService indexService) {
        for (IndexEventListener listener : listeners) {
            try {
                listener.afterIndexCreated(indexService);
            } catch (Exception e) {
                logger.warn("failed to invoke after index created callback", e);
                throw e;
            }
        }
    }

    @Override
    public void beforeIndexShardCreated(ShardId shardId, Settings indexSettings) {
        for (IndexEventListener listener : listeners) {
            try {
                listener.beforeIndexShardCreated(shardId, indexSettings);
            } catch (Exception e) {
                logger.warn(() ->
                    new ParameterizedMessage("[{}] failed to invoke before shard created callback", shardId), e);
                throw e;
            }
        }
    }

    @Override
    public void beforeIndexRemoved(IndexService indexService, IndexRemovalReason reason) {
        for (IndexEventListener listener : listeners) {
            try {
                listener.beforeIndexRemoved(indexService, reason);
            } catch (Exception e) {
                logger.warn("failed to invoke before index removed callback", e);
                throw e;
            }
        }
    }

    @Override
    public void afterIndexRemoved(Index index, IndexSettings indexSettings, IndexRemovalReason reason) {
        for (IndexEventListener listener : listeners) {
            try {
                listener.afterIndexRemoved(index, indexSettings, reason);
            } catch (Exception e) {
                logger.warn("failed to invoke after index removed callback", e);
                throw e;
            }
        }
    }

    @Override
    public void beforeIndexShardDeleted(ShardId shardId,
                                        Settings indexSettings) {
        for (IndexEventListener listener : listeners) {
            try {
                listener.beforeIndexShardDeleted(shardId, indexSettings);
            } catch (Exception e) {
                logger.warn(() -> new ParameterizedMessage("[{}] failed to invoke before shard deleted callback",
                    shardId.getId()), e);
                throw e;
            }
        }
    }

    @Override
    public void afterIndexShardDeleted(ShardId shardId,
                                       Settings indexSettings) {
        for (IndexEventListener listener : listeners) {
            try {
                listener.afterIndexShardDeleted(shardId, indexSettings);
            } catch (Exception e) {
                logger.warn(() -> new ParameterizedMessage("[{}] failed to invoke after shard deleted callback",
                    shardId.getId()), e);
                throw e;
            }
        }
    }

    @Override
    public void beforeIndexAddedToCluster(Index index, Settings indexSettings) {
        for (IndexEventListener listener  : listeners) {
            try {
                listener.beforeIndexAddedToCluster(index, indexSettings);
            } catch (Exception e) {
                logger.warn("failed to invoke before index added to cluster callback", e);
                throw e;
            }
        }
    }

    @Override
    public void onStoreCreated(ShardId shardId) {
        for (IndexEventListener listener : listeners) {
            try {
                listener.onStoreCreated(shardId);
            } catch (Exception e) {
                logger.warn("failed to invoke on store created", e);
                throw e;
            }
        }
    }

    @Override
    public void onStoreClosed(ShardId shardId) {
        for (IndexEventListener listener  : listeners) {
            try {
                listener.onStoreClosed(shardId);
            } catch (Exception e) {
                logger.warn("failed to invoke on store closed", e);
                throw e;
            }
        }
    }

    @Override
    public void beforeIndexShardRecovery(final IndexShard indexShard, final IndexSettings indexSettings) {
        for (IndexEventListener listener  : listeners) {
            try {
                listener.beforeIndexShardRecovery(indexShard, indexSettings);
            } catch (Exception e) {
                logger.warn(() -> new ParameterizedMessage("failed to invoke the listener before the shard recovery starts for {}",
                    indexShard.shardId()), e);
                throw e;
            }
        }
    }
}

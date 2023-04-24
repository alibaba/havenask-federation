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

package org.havenask.plugins;

import java.io.IOException;

import org.havenask.common.settings.Settings;
import org.havenask.env.Environment;
import org.havenask.env.ShardLock;
import org.havenask.index.Index;
import org.havenask.index.IndexSettings;

public interface NodeEnvironmentPlugin {

    interface CustomEnvironment {
        /**
         * Deletes the index directory for the given index under a lock.
         * @param index the index to delete
         * @param indexSettings settings for the index being deleted
         */
        void deleteIndexDirectoryUnderLock(Index index, IndexSettings indexSettings) throws IOException;

        /**
         * Deletes the shard directory for the given shard under a lock.
         * @param lock the shard lock
         * @param indexSettings settings for the index being deleted
         */
        void deleteShardDirectoryUnderLock(ShardLock lock, IndexSettings indexSettings) throws IOException;
    }

    CustomEnvironment newEnvironment(Environment environment, Settings settings);
}

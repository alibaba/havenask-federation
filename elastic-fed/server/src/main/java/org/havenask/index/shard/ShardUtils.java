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

package org.havenask.index.shard;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReader;
import org.havenask.common.Nullable;
import org.havenask.common.lucene.index.HavenaskDirectoryReader;
import org.havenask.common.lucene.index.HavenaskLeafReader;

public final class ShardUtils {

    private ShardUtils() {}

    /**
     * Tries to extract the shard id from a reader if possible, when its not possible,
     * will return null.
     */
    @Nullable
    public static ShardId extractShardId(LeafReader reader) {
        final HavenaskLeafReader esReader = HavenaskLeafReader.getHavenaskLeafReader(reader);
        if (esReader != null) {
            assert reader.getRefCount() > 0 : "HavenaskLeafReader is already closed";
            return esReader.shardId();
        }
        return null;
    }

    /**
     * Tries to extract the shard id from a reader if possible, when its not possible,
     * will return null.
     */
    @Nullable
    public static ShardId extractShardId(DirectoryReader reader) {
        final HavenaskDirectoryReader esReader = HavenaskDirectoryReader.getHavenaskDirectoryReader(reader);
        if (esReader != null) {
            return esReader.shardId();
        }
        throw new IllegalArgumentException("can't extract shard ID, can't unwrap HavenaskDirectoryReader");
    }
}

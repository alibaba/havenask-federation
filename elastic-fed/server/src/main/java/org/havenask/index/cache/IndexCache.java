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

package org.havenask.index.cache;

import org.havenask.core.internal.io.IOUtils;
import org.havenask.index.AbstractIndexComponent;
import org.havenask.index.IndexSettings;
import org.havenask.index.cache.bitset.BitsetFilterCache;
import org.havenask.index.cache.query.QueryCache;

import java.io.Closeable;
import java.io.IOException;

public class IndexCache extends AbstractIndexComponent implements Closeable {

    private final QueryCache queryCache;
    private final BitsetFilterCache bitsetFilterCache;

    public IndexCache(IndexSettings indexSettings, QueryCache queryCache, BitsetFilterCache bitsetFilterCache) {
        super(indexSettings);
        this.queryCache = queryCache;
        this.bitsetFilterCache = bitsetFilterCache;
    }

    public QueryCache query() {
        return queryCache;
    }

    /**
     * Return the {@link BitsetFilterCache} for this index.
     */
    public BitsetFilterCache bitsetFilterCache() {
        return bitsetFilterCache;
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(queryCache, bitsetFilterCache);
    }

    public void clear(String reason) {
        queryCache.clear(reason);
        bitsetFilterCache.clear(reason);
    }

}

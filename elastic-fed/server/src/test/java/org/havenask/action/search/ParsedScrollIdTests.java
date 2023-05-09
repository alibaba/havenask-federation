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

package org.havenask.action.search;

import org.havenask.search.internal.ShardSearchContextId;
import org.havenask.test.HavenaskTestCase;

public class ParsedScrollIdTests extends HavenaskTestCase {
    public void testHasLocalIndices() {
        final int nResults = randomIntBetween(1, 3);
        final SearchContextIdForNode[] searchContextIdForNodes = new SearchContextIdForNode[nResults];

        boolean hasLocal = false;
        for (int i = 0; i < nResults; i++) {
            String clusterAlias = randomBoolean() ? randomAlphaOfLength(8) : null;
            hasLocal = hasLocal || (clusterAlias == null);
            searchContextIdForNodes[i] =
                new SearchContextIdForNode(clusterAlias, "node_" + i, new ShardSearchContextId(randomAlphaOfLength(8), randomLong()));
        }
        final ParsedScrollId parsedScrollId = new ParsedScrollId(randomAlphaOfLength(8), randomAlphaOfLength(8), searchContextIdForNodes);

        assertEquals(hasLocal, parsedScrollId.hasLocalIndices());
    }
}

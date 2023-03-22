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

package org.havenask.upgrades;

import org.havenask.Version;
import org.havenask.client.Request;
import org.havenask.client.Response;
import org.havenask.common.io.Streams;

import java.io.IOException;

public class RefreshVersionInClusterStateIT extends AbstractRollingTestCase {

    /*
    This test ensures that after the upgrade from ElasticSearch/ Havenask all nodes report the version on and after 1.0.0
     */
    public void testRefresh() throws IOException {
        switch (CLUSTER_TYPE) {
            case OLD:
            case MIXED:
                break;
            case UPGRADED:
                Response response = client().performRequest(new Request("GET", "/_cat/nodes?h=id,version"));
                for (String nodeLine : Streams.readAllLines(response.getEntity().getContent())) {
                    String[] elements = nodeLine.split(" +");
                    assertEquals(Version.fromString(elements[1]), Version.CURRENT);
                }
                break;
            default:
                throw new UnsupportedOperationException("Unknown cluster type [" + CLUSTER_TYPE + "]");
        }
    }
}

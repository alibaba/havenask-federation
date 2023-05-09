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

package org.havenask.env;

import org.havenask.test.HavenaskTestCase;

import java.nio.file.FileStore;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HavenaskFileStoreTests extends HavenaskTestCase {
    public void testNegativeSpace() throws Exception {
        FileStore mocked = mock(FileStore.class);
        when(mocked.getUsableSpace()).thenReturn(-1L);
        when(mocked.getTotalSpace()).thenReturn(-1L);
        when(mocked.getUnallocatedSpace()).thenReturn(-1L);
        assertEquals(-1, mocked.getUsableSpace());
        FileStore store = new HavenaskFileStore(mocked);
        assertEquals(Long.MAX_VALUE, store.getUsableSpace());
        assertEquals(Long.MAX_VALUE, store.getTotalSpace());
        assertEquals(Long.MAX_VALUE, store.getUnallocatedSpace());
    }
}

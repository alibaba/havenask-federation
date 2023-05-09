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

package org.havenask.search;

import org.havenask.search.internal.ReaderContext;
import org.havenask.test.HavenaskTestCase;

import static org.mockito.Mockito.mock;

public class MockSearchServiceTests extends HavenaskTestCase {

    public void testAssertNoInFlightContext() {
        ReaderContext reader = mock(ReaderContext.class);
        MockSearchService.addActiveContext(reader);
        try {
            Throwable e = expectThrows(AssertionError.class, () -> MockSearchService.assertNoInFlightContext());
            assertEquals("There are still [1] in-flight contexts. The first one's creation site is listed as the cause of this exception.",
                    e.getMessage());
            e = e.getCause();
            assertEquals(MockSearchService.class.getName(), e.getStackTrace()[0].getClassName());
            assertEquals(MockSearchServiceTests.class.getName(), e.getStackTrace()[1].getClassName());
        } finally {
            MockSearchService.removeActiveContext(reader);
        }
    }
}

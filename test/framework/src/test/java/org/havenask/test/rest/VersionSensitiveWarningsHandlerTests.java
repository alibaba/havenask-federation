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

package org.havenask.test.rest;

import org.havenask.Version;
import org.havenask.client.WarningsHandler;
import org.havenask.test.HavenaskTestCase;
import org.havenask.test.rest.HavenaskRestTestCase.VersionSensitiveWarningsHandler;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;

public class VersionSensitiveWarningsHandlerTests extends HavenaskTestCase {

    public void testSameVersionCluster() throws IOException {
        Set<Version> nodeVersions= new HashSet<>();
        nodeVersions.add(Version.CURRENT);
        WarningsHandler handler = expectVersionSpecificWarnings(nodeVersions, (v)->{
            v.current("expectedCurrent1");
        });
        assertFalse(handler.warningsShouldFailRequest(Arrays.asList("expectedCurrent1")));
        assertTrue(handler.warningsShouldFailRequest(Arrays.asList("expectedCurrent1", "unexpected")));
        assertTrue(handler.warningsShouldFailRequest(Collections.emptyList()));

    }
    public void testMixedVersionCluster() throws IOException {
        Set<Version> nodeVersions= new HashSet<>();
        nodeVersions.add(Version.CURRENT);
        nodeVersions.add(Version.CURRENT.minimumIndexCompatibilityVersion());
        WarningsHandler handler = expectVersionSpecificWarnings(nodeVersions, (v)->{
            v.current("expectedCurrent1");
            v.compatible("Expected legacy warning");
        });
        assertFalse(handler.warningsShouldFailRequest(Arrays.asList("expectedCurrent1")));
        assertFalse(handler.warningsShouldFailRequest(Arrays.asList("Expected legacy warning")));
        assertFalse(handler.warningsShouldFailRequest(Arrays.asList("expectedCurrent1", "Expected legacy warning")));
        assertTrue(handler.warningsShouldFailRequest(Arrays.asList("expectedCurrent1", "Unexpected legacy warning")));
        assertTrue(handler.warningsShouldFailRequest(Arrays.asList("Unexpected legacy warning")));
        assertFalse(handler.warningsShouldFailRequest(Collections.emptyList()));
    }

    private static WarningsHandler expectVersionSpecificWarnings(Set<Version> nodeVersions,
            Consumer<VersionSensitiveWarningsHandler> expectationsSetter) {
        //Based on EsRestTestCase.expectVersionSpecificWarnings helper method but without HavenaskRestTestCase dependency
        VersionSensitiveWarningsHandler warningsHandler = new VersionSensitiveWarningsHandler(nodeVersions);
        expectationsSetter.accept(warningsHandler);
        return warningsHandler;
    }
}

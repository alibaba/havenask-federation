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

package org.havenask.gradle;

import org.havenask.gradle.test.GradleUnitTestCase;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;

/**
 * Tests to specifically verify the Havenask version 1.x with Legacy ES versions.
 * This supplements the tests in BwcVersionsTests.
 *
 * Currently the versioning logic doesn't work for Havenask 2.x as the masking
 * is only applied specifically for 1.x.
 */
public class BwcHavenaskVersionsTests extends GradleUnitTestCase {

    private static final Map<String, List<String>> sampleVersions = new HashMap<>();

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    static {
        sampleVersions.put("1.0.0", asList("5_6_13", "6_6_1", "6_8_15", "7_0_0", "7_9_1", "7_10_0", "7_10_1", "7_10_2", "1_0_0"));
        sampleVersions.put("1.1.0", asList("5_6_13", "6_6_1", "6_8_15", "7_0_0", "7_9_1", "7_10_0", "7_10_1", "7_10_2", "1_0_0", "1_1_0"));
    }

    public void testWireCompatible() {
        assertVersionsEquals(
            asList("6.8.15", "7.0.0", "7.9.1", "7.10.0", "7.10.1", "7.10.2"),
            getVersionCollection("1.0.0").getWireCompatible()
        );
        assertVersionsEquals(
            asList("6.8.15", "7.0.0", "7.9.1", "7.10.0", "7.10.1", "7.10.2", "1.0.0"),
            getVersionCollection("1.1.0").getWireCompatible()
        );
    }

    public void testWireCompatibleUnreleased() {
        assertVersionsEquals(Collections.emptyList(), getVersionCollection("1.0.0").getUnreleasedWireCompatible());
    }

    public void testIndexCompatible() {
        assertVersionsEquals(
            asList("6.6.1", "6.8.15", "7.0.0", "7.9.1", "7.10.0", "7.10.1", "7.10.2"),
            getVersionCollection("1.0.0").getIndexCompatible()
        );
        assertVersionsEquals(
            asList("6.6.1", "6.8.15", "7.0.0", "7.9.1", "7.10.0", "7.10.1", "7.10.2", "1.0.0"),
            getVersionCollection("1.1.0").getIndexCompatible()
        );
    }

    public void testIndexCompatibleUnreleased() {
        assertVersionsEquals(Collections.emptyList(), getVersionCollection("1.0.0").getUnreleasedIndexCompatible());
    }

    public void testGetUnreleased() {
        assertVersionsEquals(Collections.singletonList("1.0.0"), getVersionCollection("1.0.0").getUnreleased());
    }

    private String formatVersionToLine(final String version) {
        return " public static final Version V_" + version.replaceAll("\\.", "_") + " ";
    }

    private void assertVersionsEquals(List<String> expected, List<Version> actual) {
        assertEquals(expected.stream().map(Version::fromString).collect(Collectors.toList()), actual);
    }

    private BwcVersions getVersionCollection(String versionString) {
        List<String> versionMap = sampleVersions.get(versionString);
        assertNotNull(versionMap);
        Version version = Version.fromString(versionString);
        assertNotNull(version);
        return new BwcVersions(versionMap.stream().map(this::formatVersionToLine).collect(Collectors.toList()), version);
    }
}

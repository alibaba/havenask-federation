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

package org.havenask.engine.util;

import org.havenask.common.collect.Tuple;
import org.havenask.test.HavenaskTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.stream.Stream;

public class UtilsTests extends HavenaskTestCase {
    private final Path configPath;
    private final String prefix = "0000000000000000ffffffffffffffff0100000000000000";
    private final String suffix = "00000000ffff0000";

    public UtilsTests() {
        configPath = createTempDir();
    }

    // write file containing certain timestamp
    private void writeTestFile(Path path, String fileName, String locator) {
        String content = String.format(
            Locale.ROOT,
            "{\n"
                + "\"format_version\":\n"
                + "  2,\n"
                + "\"last_segmentid\":\n"
                + "  1,\n"
                + "\"level_info\":\n"
                + "  {\n"
                + "  \"level_metas\":\n"
                + "    [\n"
                + "      {\n"
                + "      \"cursor\":\n"
                + "        0,\n"
                + "      \"level_idx\":\n"
                + "        0,\n"
                + "      \"segments\":\n"
                + "        [\n"
                + "          1\n"
                + "        ],\n"
                + "      \"topology\":\n"
                + "        \"sequence\"\n"
                + "      }\n"
                + "    ]\n"
                + "  },\n"
                + "\"locator\":\n"
                + "  \"%s\",\n"
                + "\"schema_version\":\n"
                + "  0,\n"
                + "\"segments\":\n"
                + "  [\n"
                + "    1\n"
                + "  ],\n"
                + "\"timestamp\":\n"
                + "  0,\n"
                + "\"versionid\":\n"
                + "  1\n"
                + "}",
            locator
        );

        try {
            Files.write(path.resolve(fileName), content.getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            logger.error("write file [{}] error", fileName);
        }
    }

    // create directory for certain index
    private Path mkIndexDir(String indexName) {
        Path path = configPath.resolve(indexName).resolve("generation_0").resolve("partition_0_65535");

        try {
            Files.createDirectories(path);
        } catch (IOException e) {
            logger.error("mkdir [{}] error", path);
        }
        return path;
    }

    // test get index checkpoint in the case of complex file names
    public void testGetIndexCheckpointComplexFileNames() {
        String testIndex = "in0";
        Path indexPath = configPath.resolve(testIndex).resolve("generation_0").resolve("partition_0_65535");
        Path dirPath = mkIndexDir(testIndex);

        writeTestFile(dirPath, "version.1", prefix + "0100000000000000" + suffix);
        writeTestFile(dirPath, "version.7", prefix + "0200000000000000" + suffix);
        writeTestFile(dirPath, "version.3", prefix + "0300000000000000" + suffix);
        writeTestFile(dirPath, "ssversion.18", prefix + "0400000000000000" + suffix);
        writeTestFile(dirPath, "other", prefix + "0500000000000000" + suffix);
        writeTestFile(dirPath, "version.8a", prefix + "0600000000000000" + suffix);
        writeTestFile(dirPath, "version.a9", prefix + "0700000000000000" + suffix);

        long version = Utils.getVersionAndIndexCheckpoint(indexPath).v1();
        long timeStamp = Utils.getVersionAndIndexCheckpoint(indexPath).v2();
        assertEquals(7, version);
        assertEquals(2, timeStamp);
    }

    // test get index checkpoint in the case of version number is big
    public void testGetIndexCheckpointBigVersionNum() {
        String testIndex = "in1";
        Path indexPath = configPath.resolve(testIndex).resolve("generation_0").resolve("partition_0_65535");
        Path dirPath = mkIndexDir(testIndex);

        writeTestFile(dirPath, "version.1", prefix + "0100000000000000" + suffix);
        writeTestFile(dirPath, "version.11", prefix + "1100000000000000" + suffix);
        writeTestFile(dirPath, "version.111", prefix + "ff00000000000000" + suffix);
        // 9223372036854775807 is the max value of long type
        writeTestFile(dirPath, "version.9223372036854775807", prefix + "ffff000000000000" + suffix);

        long version = Utils.getVersionAndIndexCheckpoint(indexPath).v1();
        long timeStamp = Utils.getVersionAndIndexCheckpoint(indexPath).v2();
        assertEquals(9223372036854775807L, version);
        assertEquals(65535, timeStamp);
    }

    // test get index checkpoint in the case of multi index, and some index number is negative
    public void testGetIndexCheckpointMultiIndex() {
        String testIndex2 = "in2";
        Path indexPath2 = configPath.resolve(testIndex2).resolve("generation_0").resolve("partition_0_65535");
        Path dirPath_in2 = mkIndexDir(testIndex2);

        writeTestFile(dirPath_in2, "version.-1", prefix + "0100000000000000" + suffix);
        writeTestFile(dirPath_in2, "version.0", prefix + "0200000000000000" + suffix);
        writeTestFile(dirPath_in2, "version.1", prefix + "gg00000000000000" + suffix);

        String testIndex3 = "in3";
        Path indexPath3 = configPath.resolve(testIndex3).resolve("generation_0").resolve("partition_0_65535");
        Path dirPath_in3 = mkIndexDir(testIndex3);

        writeTestFile(dirPath_in3, "version.-1", prefix + "ffff000000000000" + suffix);

        long version2 = Utils.getVersionAndIndexCheckpoint(indexPath2).v1();
        Long timeStamp2 = Utils.getVersionAndIndexCheckpoint(indexPath2).v2();
        assertEquals(1, version2);
        assertNull(timeStamp2);

        Tuple<Long, Long> tuple = Utils.getVersionAndIndexCheckpoint(indexPath3);
        assertNull(tuple);
    }

    // test get index checkpoint in the case of no index directory
    public void testGetIndexCheckpointNoDir() {
        String testIndex = "in4";
        Path indexPath = configPath.resolve(testIndex).resolve("generation_0").resolve("partition_0_65535");

        Tuple<Long, Long> tuple = Utils.getVersionAndIndexCheckpoint(indexPath);
        assertNull(tuple);
    }

    // test get index checkpoint in the case of no version file
    public void testGetIndexCheckpointNoFile() {
        String testIndex = "in5";
        Path indexPath = configPath.resolve(testIndex).resolve("generation_0").resolve("partition_0_65535");

        mkIndexDir(testIndex);

        Tuple<Long, Long> tuple = Utils.getVersionAndIndexCheckpoint(indexPath);
        assertNull(tuple);
    }

    // test getLocatorCheckpoint
    public void testGetLocatorCheckpoint() {
        assertEquals(-1, Utils.getLocatorCheckpoint(prefix + "ffffffffffffffff" + suffix).longValue());
        assertEquals(65535, Utils.getLocatorCheckpoint(prefix + "ffff000000000000" + suffix).longValue());
        assertEquals(65295, Utils.getLocatorCheckpoint(prefix + "0fff000000000000" + suffix).longValue());
        assertEquals(65280, Utils.getLocatorCheckpoint(prefix + "00ff000000000000" + suffix).longValue());
        assertEquals(256, Utils.getLocatorCheckpoint(prefix + "0001000000000000" + suffix).longValue());
        assertEquals(3840, Utils.getLocatorCheckpoint(prefix + "000f000000000000" + suffix).longValue());
    }

    // test getLongLittleEndian
    public void testGetLongLittleEndian() {
        assertEquals(-1, Utils.getLongLittleEndian("ffffffffffffffff", 0, 16).longValue());
        assertEquals(65535, Utils.getLongLittleEndian("ffff000000000000", 0, 16).longValue());
        assertEquals(65295, Utils.getLongLittleEndian("0fff000000000000", 0, 16).longValue());
        assertEquals(65280, Utils.getLongLittleEndian("00ff000000000000", 0, 16).longValue());
        assertEquals(3840, Utils.getLongLittleEndian("000f000000000000", 0, 16).longValue());
        assertEquals(256, Utils.getLongLittleEndian("0001000000000000", 0, 16).longValue());
        assertThrows(NumberFormatException.class, () -> Utils.getLongLittleEndian("g000000000000000", 0, 16));
    }

    // test hexCharToInt
    public void testHexCharToInt() {
        assertEquals(0, Utils.hexCharToInt('0'));
        assertEquals(3, Utils.hexCharToInt('3'));
        assertEquals(9, Utils.hexCharToInt('9'));
        assertEquals(10, Utils.hexCharToInt('a'));
        assertEquals(12, Utils.hexCharToInt('c'));
        assertEquals(15, Utils.hexCharToInt('f'));
        assertEquals(10, Utils.hexCharToInt('A'));
        assertEquals(12, Utils.hexCharToInt('C'));
        assertEquals(15, Utils.hexCharToInt('F'));
        assertThrows(NumberFormatException.class, () -> Utils.hexCharToInt('g'));
    }

    // test cleanVersionPublishFiles
    public void testCleanVersionPublishFiles() {
        String testIndex = "in6";
        Path indexPath = configPath.resolve(testIndex).resolve("generation_0").resolve("partition_0_65535");
        Path dirPath = mkIndexDir(testIndex);

        writeTestFile(dirPath, "version.publish.1", "some content");
        writeTestFile(dirPath, "version.publish.7", "some content");
        writeTestFile(dirPath, "version.publish.3", "some content");
        writeTestFile(dirPath, "ssversion.publish.18", "some content");
        writeTestFile(dirPath, "other", "some content");

        Utils.cleanVersionPublishFiles(indexPath);

        try (Stream<Path> stream = Files.list(dirPath)) {
            assertEquals(2, stream.count());
        } catch (IOException e) {
            logger.error("list directory [{}] error", dirPath);
        }
    }
}

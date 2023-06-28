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

import org.havenask.test.HavenaskTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;

import static org.havenask.engine.util.Utils.INDEX_SUB_PATH;

public class UtilsTests extends HavenaskTestCase {
    private final Path configPath;
    public UtilsTests() {
        configPath = createTempDir();
    }

    // write file containing certain timestamp
    private void writeTestFile(Path path, String fileName, String timeStamp) {
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
                + "  \"c544a5eb3063fb610500000000000000\",\n"
                + "\"schema_version\":\n"
                + "  0,\n"
                + "\"segments\":\n"
                + "  [\n"
                + "    1\n"
                + "  ],\n"
                + "\"timestamp\":\n"
                + "  %s,\n"
                + "\"versionid\":\n"
                + "  1\n"
                + "}",
            timeStamp
        );

        try {
            Files.write(path.resolve(fileName), content.getBytes("UTF-8"));
        } catch (IOException e) {
            logger.error("write file [{}] error", fileName);
        }
    }

    // create directory for certain index
    private Path mkIndexDir(String indexName) {
        Path path = configPath.resolve(indexName).resolve(INDEX_SUB_PATH);

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
        Path indexPath = configPath.resolve(testIndex);

        Path dirPath = mkIndexDir(testIndex);

        writeTestFile(dirPath, "version.1", "333");
        writeTestFile(dirPath, "version.7", "78641949317145");
        writeTestFile(dirPath, "version.3", "78641949");
        writeTestFile(dirPath, "ssversion.18", "786419");
        writeTestFile(dirPath, "other", "7864198888");
        writeTestFile(dirPath, "version.8a", "7864198888");
        writeTestFile(dirPath, "version.a9", "7864198887");

        String timeStamp = Utils.getIndexCheckpoint(indexPath);
        assertEquals("78641949317145", timeStamp);
    }

    // test get index checkpoint in the case of version number is big
    public void testGetIndexCheckpointBigVersionNum() {
        String testIndex = "in1";
        Path indexPath = configPath.resolve(testIndex);

        Path dirPath = mkIndexDir(testIndex);

        writeTestFile(dirPath, "version.1", "333");
        writeTestFile(dirPath, "version.11", "78641949317145");
        writeTestFile(dirPath, "version.111", "78641949");

        // 9223372036854775807 is the max value of long type
        writeTestFile(dirPath, "version.9223372036854775807", "9876578889901");

        String timeStamp = Utils.getIndexCheckpoint(indexPath);
        assertEquals("9876578889901", timeStamp);
    }

    // test get index checkpoint in the case of multi index, and some index number is negative
    public void testGetIndexCheckpointMultiIndex() {
        String testIndex2 = "in2";
        Path indexPath2 = configPath.resolve(testIndex2);
        Path dirPath_in2 = mkIndexDir(testIndex2);

        writeTestFile(dirPath_in2, "version.-1", "333");
        writeTestFile(dirPath_in2, "version.0", "666");
        writeTestFile(dirPath_in2, "version.1", "999");

        String testIndex3 = "in3";
        Path indexPath3 = configPath.resolve(testIndex3);
        Path dirPath_in3 = mkIndexDir(testIndex3);

        writeTestFile(dirPath_in3, "version.-1", "333");

        String timeStamp2 = Utils.getIndexCheckpoint(indexPath2);
        String timeStamp3 = Utils.getIndexCheckpoint(indexPath3);
        assertEquals("999", timeStamp2);
        assertNull(timeStamp3);
    }

    // test get index checkpoint in the case of no index directory
    public void testGetIndexCheckpointNoDir() {
        String testIndex = "in4";
        Path indexPath = configPath.resolve(testIndex);

        String timeStamp = Utils.getIndexCheckpoint(indexPath);
        assertNull(timeStamp);
    }

    // test get index checkpoint in the case of no version file
    public void testGetIndexCheckpointNoFile() {
        String testIndex = "in5";
        Path indexPath = configPath.resolve(testIndex);

        mkIndexDir(testIndex);

        String timeStamp = Utils.getIndexCheckpoint(indexPath);
        assertNull(timeStamp);
    }
}

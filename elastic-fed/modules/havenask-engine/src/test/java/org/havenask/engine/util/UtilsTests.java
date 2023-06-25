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

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;

import static org.havenask.engine.util.Utils.DEFAULT_INDEX_UP_PATH;
import static org.havenask.engine.util.Utils.DEFAULT_INDEX_SUB_PATH;

public class UtilsTests extends HavenaskTestCase {

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
            FileWriter writer = new FileWriter(path.resolve(fileName).toString());
            writer.write(content);
            writer.flush();
            writer.close();
            System.out.printf("write file success : %s\n", fileName);
        } catch(IOException e) {
            e.printStackTrace();
        }
    }

    // create directory for certain index
    private Path mkIndexDir(String indexName) {
        Path path = Path.of(DEFAULT_INDEX_UP_PATH, indexName, DEFAULT_INDEX_SUB_PATH);
        try {
            Files.createDirectories(path);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return path;
    }

    // test get index checkpoint in the case of complex file names
    public void testGetIndexCheckpointComplexFileNames() {
        String testIndex = "in0";
        Path dirPath = mkIndexDir(testIndex);

        writeTestFile(dirPath, "version.1", "333");
        writeTestFile(dirPath, "version.7", "78641949317145");
        writeTestFile(dirPath, "version.3", "78641949");
        writeTestFile(dirPath, "ssversion.18", "786419");
        writeTestFile(dirPath, "other", "7864198888");
        writeTestFile(dirPath, "version.8a", "7864198888");
        writeTestFile(dirPath, "version.a9", "7864198887");

        String timeStamp = Utils.getIndexCheckpoint(testIndex);
        assertEquals("78641949317145", timeStamp);
    }

    // test get index checkpoint in the case of version number is big
    public void testGetIndexCheckpointBigVersionNum() {
        String testIndex = "in1";
        Path dirPath = mkIndexDir(testIndex);

        writeTestFile(dirPath, "version.1", "333");
        writeTestFile(dirPath, "version.11", "78641949317145");
        writeTestFile(dirPath, "version.111", "78641949");

        // 9223372036854775807 is the max value of long type
        writeTestFile(dirPath, "version.9223372036854775807", "9876578889901");

        String timeStamp = Utils.getIndexCheckpoint(testIndex);
        assertEquals("9876578889901", timeStamp);
    }

    // test get index checkpoint in the case of multi index, and some index number is negative
    public void testGetIndexCheckpointMultiIndex() {
        String testIndex = "in2";
        Path dirPath_in2 = mkIndexDir(testIndex);

        writeTestFile(dirPath_in2, "version.-1", "333");
        writeTestFile(dirPath_in2, "version.0", "666");
        writeTestFile(dirPath_in2, "version.1", "999");

        String testIndex2 = "in3";
        Path dirPath_in3 = mkIndexDir(testIndex2);

        writeTestFile(dirPath_in3, "version.-1", "333");


        String timeStamp = Utils.getIndexCheckpoint(testIndex);
        String timeStamp2 = Utils.getIndexCheckpoint(testIndex2);
        assertEquals("999", timeStamp);
        assertNull(timeStamp2);
    }

    // test get index checkpoint in the case of no index directory
    public void testGetIndexCheckpointNoDir() {
        String testIndex = "in4";

        String timeStamp = Utils.getIndexCheckpoint(testIndex);
        assertNull(timeStamp);
    }

    // test get index checkpoint in the case of no version file
    public void testGetIndexCheckpointNoFile() {
        String testIndex = "in5";
        mkIndexDir(testIndex);

        String timeStamp = Utils.getIndexCheckpoint(testIndex);
        assertNull(timeStamp);
    }
}

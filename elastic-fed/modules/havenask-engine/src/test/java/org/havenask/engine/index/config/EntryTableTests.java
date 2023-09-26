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

package org.havenask.engine.index.config;

import org.havenask.test.HavenaskTestCase;

public class EntryTableTests extends HavenaskTestCase {
    // test for EntryTable
    public void testEntryTable() {
        EntryTable entryTable = new EntryTable();
        assertEquals(entryTable.toString(), "{\"files\":{},\"package_files\":{}}");
    }

    // test for EntryTable.parse
    public void testEntryTableParse() {
        EntryTable entryTable = EntryTable.parse("{}");
        assertEquals(entryTable.toString(), "{\"files\":{},\"package_files\":{}}");
    }

    // test for EntryTable with files
    public void testEntryTableWithFiles() {
        String content = "{\"files\":{\"\":{\"index_format_version\":{\"length\":82},"
            + "\"index_partition_meta\":{\"length\":28},\"schema.json\":{\"length\":2335},"
            + "\"segment_0_level_0\":{\"length\":-2},\"segment_0_level_0\\/attribute\":{\"length\":-2},"
            + "\"segment_0_level_0\\/attribute\\/_id\":{\"length\":-2},"
            + "\"segment_0_level_0\\/attribute\\/_id\\/data\":{\"length\":1050},"
            + "\"segment_0_level_0\\/counter\":{\"length\":0},\"segment_0_level_0\\/deletionmap\":{\"length\":-2},"
            + "\"segment_0_level_0\\/segment_info\":{\"length\":347},"
            + "\"segment_0_level_0\\/segment_metrics\":{\"length\":52},"
            + "\"segment_0_level_0\\/summary\":{\"length\":-2},"
            + "\"segment_0_level_0\\/summary\\/data\":{\"length\":1400},"
            + "\"segment_0_level_0\\/summary\\/offset\":{\"length\":400},\"truncate_meta\":{\"length\":-2},"
            + "\"truncate_meta\\/index.mapper\":{\"length\":41},\"version.1\":{\"length\":1371}}},\"package_files\":{}}";
        EntryTable entryTable = EntryTable.parse(content);
        assertEquals(
            entryTable.toString(),
            "{\"files\":{\"\":{\"index_format_version\":{\"length\":82},\"index_partition_meta\":{\"length\":28},"
                + "\"schema.json\":{\"length\":2335},\"segment_0_level_0\":{\"length\":-2},"
                + "\"segment_0_level_0/attribute\":{\"length\":-2},"
                + "\"segment_0_level_0/attribute/_id\":{\"length\":-2},"
                + "\"segment_0_level_0/attribute/_id/data\":{\"length\":1050},"
                + "\"segment_0_level_0/counter\":{\"length\":0},"
                + "\"segment_0_level_0/deletionmap\":{\"length\":-2},"
                + "\"segment_0_level_0/segment_info\":{\"length\":347},"
                + "\"segment_0_level_0/segment_metrics\":{\"length\":52},"
                + "\"segment_0_level_0/summary\":{\"length\":-2},"
                + "\"segment_0_level_0/summary/data\":{\"length\":1400},"
                + "\"segment_0_level_0/summary/offset\":{\"length\":400},\"truncate_meta\":{\"length\":-2},"
                + "\"truncate_meta/index.mapper\":{\"length\":41},\"version.1\":{\"length\":1371}}},"
                + "\"package_files\":{}}"
        );
    }
}

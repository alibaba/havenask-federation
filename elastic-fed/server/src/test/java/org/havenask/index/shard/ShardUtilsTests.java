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

package org.havenask.index.shard;


import org.apache.lucene.document.Document;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.CompositeReaderContext;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.BaseDirectoryWrapper;
import org.havenask.common.lucene.index.HavenaskDirectoryReader;
import org.havenask.core.internal.io.IOUtils;
import org.havenask.test.HavenaskTestCase;

import java.io.IOException;

public class ShardUtilsTests extends HavenaskTestCase {

    public void testExtractShardId() throws IOException {
        BaseDirectoryWrapper dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig());
        writer.commit();
        ShardId id = new ShardId("foo", "_na_", random().nextInt());
        try (DirectoryReader reader = DirectoryReader.open(writer)) {
            HavenaskDirectoryReader wrap = HavenaskDirectoryReader.wrap(reader, id);
            assertEquals(id, ShardUtils.extractShardId(wrap));
        }
        final int numDocs = 1 + random().nextInt(5);
        for (int i = 0; i < numDocs; i++) {
            Document d = new Document();
            d.add(newField("name", "foobar", StringField.TYPE_STORED));
            writer.addDocument(d);
            if (random().nextBoolean()) {
                writer.commit();
            }
        }

        try (DirectoryReader reader = DirectoryReader.open(writer)) {
            HavenaskDirectoryReader wrap = HavenaskDirectoryReader.wrap(reader, id);
            assertEquals(id, ShardUtils.extractShardId(wrap));
            CompositeReaderContext context = wrap.getContext();
            for (LeafReaderContext leaf : context.leaves()) {
                assertEquals(id, ShardUtils.extractShardId(leaf.reader()));
            }
        }
        IOUtils.close(writer, dir);
    }
}

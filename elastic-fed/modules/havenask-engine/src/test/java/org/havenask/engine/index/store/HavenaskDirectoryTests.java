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

package org.havenask.engine.index.store;

import java.io.IOException;
import java.nio.file.Path;

import org.apache.lucene.util.LuceneTestCase;
import org.havenask.core.internal.io.IOUtils;
import org.havenask.test.HavenaskTestCase;
import org.junit.After;
import org.junit.Before;

@LuceneTestCase.SuppressFileSystems("ExtrasFS")
public class HavenaskDirectoryTests extends HavenaskTestCase {

    private HavenaskDirectory directory;
    private Path dataPath = createTempDir();

    @Before
    public void setup() throws IOException {
        directory = new HavenaskDirectory(newFSDirectory(createTempDir()), dataPath);
    }

    @After
    public void cleanup() throws IOException {
        IOUtils.close(directory);
    }
}

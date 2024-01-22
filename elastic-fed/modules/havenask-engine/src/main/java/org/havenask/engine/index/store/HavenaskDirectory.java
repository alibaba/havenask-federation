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
import java.util.Collection;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;

public class HavenaskDirectory extends FilterDirectory {
    private final Path shardPath;

    protected HavenaskDirectory(Directory in, Path shardPath) {
        super(in);
        this.shardPath = shardPath;
    }

    @Override
    public void sync(Collection<String> names) throws IOException {

    }
}

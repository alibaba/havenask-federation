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

package org.havenask.analysis.common;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.path.PathHierarchyTokenizer;
import org.apache.lucene.analysis.path.ReversePathHierarchyTokenizer;
import org.havenask.common.settings.Settings;
import org.havenask.env.Environment;
import org.havenask.index.IndexSettings;
import org.havenask.index.analysis.AbstractTokenizerFactory;

public class PathHierarchyTokenizerFactory extends AbstractTokenizerFactory {

    private final int bufferSize;

    private final char delimiter;
    private final char replacement;
    private final int skip;
    private final boolean reverse;

    PathHierarchyTokenizerFactory(IndexSettings indexSettings, Environment environment, String name, Settings settings) {
        super(indexSettings, settings, name);
        bufferSize = settings.getAsInt("buffer_size", 1024);
        String delimiter = settings.get("delimiter");
        if (delimiter == null) {
            this.delimiter = PathHierarchyTokenizer.DEFAULT_DELIMITER;
        } else if (delimiter.length() != 1) {
            throw new IllegalArgumentException("delimiter must be a one char value");
        } else {
            this.delimiter = delimiter.charAt(0);
        }

        String replacement = settings.get("replacement");
        if (replacement == null) {
            this.replacement = this.delimiter;
        } else if (replacement.length() != 1) {
            throw new IllegalArgumentException("replacement must be a one char value");
        } else {
            this.replacement = replacement.charAt(0);
        }
        this.skip = settings.getAsInt("skip", PathHierarchyTokenizer.DEFAULT_SKIP);
        this.reverse = settings.getAsBoolean("reverse", false);
    }

    @Override
    public Tokenizer create() {
        if (reverse) {
            return new ReversePathHierarchyTokenizer(bufferSize, delimiter, replacement, skip);
        }
        return new PathHierarchyTokenizer(bufferSize, delimiter, replacement, skip);
    }

}

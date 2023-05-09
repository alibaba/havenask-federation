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

package org.havenask.snapshots;

import org.havenask.cluster.ClusterModule;
import org.havenask.cluster.Diff;
import org.havenask.cluster.metadata.Metadata.Custom;
import org.havenask.cluster.metadata.RepositoriesMetadata;
import org.havenask.cluster.metadata.RepositoryMetadata;
import org.havenask.common.io.stream.NamedWriteableRegistry;
import org.havenask.common.io.stream.Writeable;
import org.havenask.common.settings.Settings;
import org.havenask.common.xcontent.XContentParser;
import org.havenask.test.AbstractDiffableSerializationTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class RepositoriesMetadataSerializationTests extends AbstractDiffableSerializationTestCase<Custom> {

    @Override
    protected Custom createTestInstance() {
        int numberOfRepositories = randomInt(10);
        List<RepositoryMetadata> entries = new ArrayList<>();
        for (int i = 0; i < numberOfRepositories; i++) {
            // divide by 2 to not overflow when adding to this number for the pending generation below
            final long generation = randomNonNegativeLong() / 2L;
            entries.add(new RepositoryMetadata(randomAlphaOfLength(10), randomAlphaOfLength(10), randomSettings(), generation,
                generation + randomLongBetween(0, generation)));
        }
        entries.sort(Comparator.comparing(RepositoryMetadata::name));
        return new RepositoriesMetadata(entries);
    }

    @Override
    protected Writeable.Reader<Custom> instanceReader() {
        return RepositoriesMetadata::new;
    }

    @Override
    protected Custom mutateInstance(Custom instance) {
        List<RepositoryMetadata> entries = new ArrayList<>(((RepositoriesMetadata) instance).repositories());
        boolean addEntry = entries.isEmpty() ? true : randomBoolean();
        if (addEntry) {
            entries.add(new RepositoryMetadata(randomAlphaOfLength(10), randomAlphaOfLength(10), randomSettings()));
        } else {
            entries.remove(randomIntBetween(0, entries.size() - 1));
        }
        return new RepositoriesMetadata(entries);
    }

    public Settings randomSettings() {
        if (randomBoolean()) {
            return Settings.EMPTY;
        } else {
            int numberOfSettings = randomInt(10);
            Settings.Builder builder = Settings.builder();
            for (int i = 0; i < numberOfSettings; i++) {
                builder.put(randomAlphaOfLength(10), randomAlphaOfLength(20));
            }
            return builder.build();
        }
    }

    @Override
    protected Custom makeTestChanges(Custom testInstance) {
        RepositoriesMetadata repositoriesMetadata = (RepositoriesMetadata) testInstance;
        List<RepositoryMetadata> repos = new ArrayList<>(repositoriesMetadata.repositories());
        if (randomBoolean() && repos.size() > 1) {
            // remove some elements
            int leaveElements = randomIntBetween(0, repositoriesMetadata.repositories().size() - 1);
            repos = randomSubsetOf(leaveElements, repos.toArray(new RepositoryMetadata[leaveElements]));
        }
        if (randomBoolean()) {
            // add some elements
            int addElements = randomInt(10);
            for (int i = 0; i < addElements; i++) {
                repos.add(new RepositoryMetadata(randomAlphaOfLength(10), randomAlphaOfLength(10), randomSettings()));
            }
        }
        return new RepositoriesMetadata(repos);
    }

    @Override
    protected Writeable.Reader<Diff<Custom>> diffReader() {
        return RepositoriesMetadata::readDiffFrom;
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(ClusterModule.getNamedWriteables());
    }

    @Override
    protected Custom doParseInstance(XContentParser parser) throws IOException {
        assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
        RepositoriesMetadata repositoriesMetadata = RepositoriesMetadata.fromXContent(parser);
        assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
        List<RepositoryMetadata> repos = new ArrayList<>(repositoriesMetadata.repositories());
        repos.sort(Comparator.comparing(RepositoryMetadata::name));
        return new RepositoriesMetadata(repos);
    }

}

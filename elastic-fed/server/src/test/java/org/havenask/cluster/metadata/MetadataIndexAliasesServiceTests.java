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

package org.havenask.cluster.metadata;

import org.havenask.Version;
import org.havenask.cluster.ClusterName;
import org.havenask.cluster.ClusterState;
import org.havenask.common.settings.Settings;
import org.havenask.common.util.set.Sets;
import org.havenask.index.Index;
import org.havenask.index.IndexNotFoundException;
import org.havenask.test.HavenaskTestCase;
import org.havenask.test.VersionUtils;
import org.havenask.cluster.metadata.AliasAction;
import org.havenask.cluster.metadata.AliasMetadata;
import org.havenask.cluster.metadata.AliasValidator;
import org.havenask.cluster.metadata.DataStream;
import org.havenask.cluster.metadata.IndexAbstraction;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.cluster.metadata.Metadata;
import org.havenask.cluster.metadata.MetadataDeleteIndexService;
import org.havenask.cluster.metadata.MetadataIndexAliasesService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.util.Collections.singletonList;
import static org.havenask.cluster.DataStreamTestHelper.createTimestampField;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anySetOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MetadataIndexAliasesServiceTests extends HavenaskTestCase {
    private final AliasValidator aliasValidator = new AliasValidator();
    private final MetadataDeleteIndexService deleteIndexService = mock(MetadataDeleteIndexService.class);
    private final MetadataIndexAliasesService service = new MetadataIndexAliasesService(null, null, aliasValidator,
            deleteIndexService, xContentRegistry());

    public MetadataIndexAliasesServiceTests() {
        // Mock any deletes so we don't need to worry about how MetadataDeleteIndexService does its job
        when(deleteIndexService.deleteIndices(any(ClusterState.class), anySetOf(Index.class))).then(i -> {
            ClusterState state = (ClusterState) i.getArguments()[0];
            @SuppressWarnings("unchecked")
            Collection<Index> indices = (Collection<Index>) i.getArguments()[1];
            Metadata.Builder meta = Metadata.builder(state.metadata());
            for (Index index : indices) {
                assertTrue("index now found", state.metadata().hasConcreteIndex(index.getName()));
                meta.remove(index.getName()); // We only think about metadata for this test. Not routing or any other fun stuff.
            }
            return ClusterState.builder(state).metadata(meta).build();
        });
    }

    public void testAddAndRemove() {
        // Create a state with a single index
        String index = randomAlphaOfLength(5);
        ClusterState before = createIndex(ClusterState.builder(ClusterName.DEFAULT).build(), index);

        // Add an alias to it
        ClusterState after = service.applyAliasActions(before, singletonList(new AliasAction.Add(index, "test", null, null, null, null,
            null)));
        IndexAbstraction alias = after.metadata().getIndicesLookup().get("test");
        assertNotNull(alias);
        assertThat(alias.getType(), equalTo(IndexAbstraction.Type.ALIAS));
        assertThat(alias.getIndices(), contains(after.metadata().index(index)));
        assertAliasesVersionIncreased(index, before, after);

        // Remove the alias from it while adding another one
        before = after;
        after = service.applyAliasActions(before, Arrays.asList(
                new AliasAction.Remove(index, "test", null),
                new AliasAction.Add(index, "test_2", null, null, null, null, null)));
        assertNull(after.metadata().getIndicesLookup().get("test"));
        alias = after.metadata().getIndicesLookup().get("test_2");
        assertNotNull(alias);
        assertThat(alias.getType(), equalTo(IndexAbstraction.Type.ALIAS));
        assertThat(alias.getIndices(), contains(after.metadata().index(index)));
        assertAliasesVersionIncreased(index, before, after);

        // Now just remove on its own
        before = after;
        after = service.applyAliasActions(before, singletonList(new AliasAction.Remove(index, "test_2", randomBoolean())));
        assertNull(after.metadata().getIndicesLookup().get("test"));
        assertNull(after.metadata().getIndicesLookup().get("test_2"));
        assertAliasesVersionIncreased(index, before, after);
    }

    public void testMustExist() {
        // Create a state with a single index
        String index = randomAlphaOfLength(5);
        ClusterState before = createIndex(ClusterState.builder(ClusterName.DEFAULT).build(), index);

        // Add an alias to it
        ClusterState after = service.applyAliasActions(before, singletonList(new AliasAction.Add(index, "test", null, null, null, null,
            null)));
        IndexAbstraction alias = after.metadata().getIndicesLookup().get("test");
        assertNotNull(alias);
        assertThat(alias.getType(), equalTo(IndexAbstraction.Type.ALIAS));
        assertThat(alias.getIndices(), contains(after.metadata().index(index)));
        assertAliasesVersionIncreased(index, before, after);

        // Remove the alias from it with mustExist == true while adding another one
        before = after;
        after = service.applyAliasActions(before, Arrays.asList(
            new AliasAction.Remove(index, "test", true),
            new AliasAction.Add(index, "test_2", null, null, null, null, null)));
        assertNull(after.metadata().getIndicesLookup().get("test"));
        alias = after.metadata().getIndicesLookup().get("test_2");
        assertNotNull(alias);
        assertThat(alias.getType(), equalTo(IndexAbstraction.Type.ALIAS));
        assertThat(alias.getIndices(), contains(after.metadata().index(index)));
        assertAliasesVersionIncreased(index, before, after);

        // Now just remove on its own
        before = after;
        after = service.applyAliasActions(before, singletonList(new AliasAction.Remove(index, "test_2", randomBoolean())));
        assertNull(after.metadata().getIndicesLookup().get("test"));
        assertNull(after.metadata().getIndicesLookup().get("test_2"));
        assertAliasesVersionIncreased(index, before, after);

        // Show that removing non-existing alias with mustExist == true fails
        final ClusterState finalCS = after;
        final IllegalArgumentException iae = expectThrows(IllegalArgumentException.class,
            () -> service.applyAliasActions(finalCS, singletonList(new AliasAction.Remove(index, "test_2", true))));
        assertThat(iae.getMessage(), containsString("required alias [test_2] does not exist"));
    }

    public void testMultipleIndices() {
        final int length = randomIntBetween(2, 8);
        final Set<String> indices = new HashSet<>(length);
        ClusterState before = ClusterState.builder(ClusterName.DEFAULT).build();
        final List<AliasAction> addActions = new ArrayList<>(length);
        for (int i = 0; i < length; i++) {
            final String index = randomValueOtherThanMany(v -> indices.add(v) == false, () -> randomAlphaOfLength(8));
            before = createIndex(before, index);
            addActions.add(new AliasAction.Add(index, "alias-" + index, null, null, null, null, null));
        }
        final ClusterState afterAddingAliasesToAll = service.applyAliasActions(before, addActions);
        assertAliasesVersionIncreased(indices.toArray(new String[0]), before, afterAddingAliasesToAll);

        // now add some aliases randomly
        final Set<String> randomIndices = new HashSet<>(length);
        final List<AliasAction> randomAddActions = new ArrayList<>(length);
        for (String index : indices) {
            if (randomBoolean()) {
                randomAddActions.add(new AliasAction.Add(index, "random-alias-" + index, null, null, null, null, null));
                randomIndices.add(index);
            }
        }
        final ClusterState afterAddingRandomAliases = service.applyAliasActions(afterAddingAliasesToAll, randomAddActions);
        assertAliasesVersionIncreased(randomIndices.toArray(new String[0]), afterAddingAliasesToAll, afterAddingRandomAliases);
        assertAliasesVersionUnchanged(
                Sets.difference(indices, randomIndices).toArray(new String[0]),
                afterAddingAliasesToAll,
                afterAddingRandomAliases);
    }

    public void testChangingWriteAliasStateIncreasesAliasesVersion() {
        final String index = randomAlphaOfLength(8);
        final ClusterState before = createIndex(ClusterState.builder(ClusterName.DEFAULT).build(), index);

        final ClusterState afterAddWriteAlias =
                service.applyAliasActions(before, singletonList(new AliasAction.Add(index, "test", null, null, null, true, null)));
        assertAliasesVersionIncreased(index, before, afterAddWriteAlias);

        final ClusterState afterChangeWriteAliasToNonWriteAlias =
                service.applyAliasActions(afterAddWriteAlias, singletonList(new AliasAction.Add(index, "test", null, null, null, false,
                    null)));
        assertAliasesVersionIncreased(index, afterAddWriteAlias, afterChangeWriteAliasToNonWriteAlias);

        final ClusterState afterChangeNonWriteAliasToWriteAlias =
                service.applyAliasActions(
                        afterChangeWriteAliasToNonWriteAlias,
                        singletonList(new AliasAction.Add(index, "test", null, null, null, true, null)));
        assertAliasesVersionIncreased(index, afterChangeWriteAliasToNonWriteAlias, afterChangeNonWriteAliasToWriteAlias);
    }

    public void testAddingAliasMoreThanOnceShouldOnlyIncreaseAliasesVersionByOne() {
        final String index = randomAlphaOfLength(8);
        final ClusterState before = createIndex(ClusterState.builder(ClusterName.DEFAULT).build(), index);

        // add an alias to the index multiple times
        final int length = randomIntBetween(2, 8);
        final List<AliasAction> addActions = new ArrayList<>(length);
        for (int i = 0; i < length; i++) {
            addActions.add(new AliasAction.Add(index, "test", null, null, null, null, null));
        }
        final ClusterState afterAddingAliases = service.applyAliasActions(before, addActions);

        assertAliasesVersionIncreased(index, before, afterAddingAliases);
    }

    public void testAliasesVersionUnchangedWhenActionsAreIdempotent() {
        final String index = randomAlphaOfLength(8);
        final ClusterState before = createIndex(ClusterState.builder(ClusterName.DEFAULT).build(), index);

        // add some aliases to the index
        final int length = randomIntBetween(1, 8);
        final Set<String> aliasNames = new HashSet<>();
        final List<AliasAction> addActions = new ArrayList<>(length);
        for (int i = 0; i < length; i++) {
            final String aliasName = randomValueOtherThanMany(v -> aliasNames.add(v) == false, () -> randomAlphaOfLength(8));
            addActions.add(new AliasAction.Add(index, aliasName, null, null, null, null, null));
        }
        final ClusterState afterAddingAlias = service.applyAliasActions(before, addActions);

        // now perform a remove and add for each alias which is idempotent, the resulting aliases are unchanged
        final List<AliasAction> removeAndAddActions = new ArrayList<>(2 * length);
        for (final String aliasName : aliasNames) {
            removeAndAddActions.add(new AliasAction.Remove(index, aliasName, null));
            removeAndAddActions.add(new AliasAction.Add(index, aliasName, null, null, null, null, null));
        }
        final ClusterState afterRemoveAndAddAlias = service.applyAliasActions(afterAddingAlias, removeAndAddActions);
        assertAliasesVersionUnchanged(index, afterAddingAlias, afterRemoveAndAddAlias);
    }

    public void testSwapIndexWithAlias() {
        // Create "test" and "test_2"
        ClusterState before = createIndex(ClusterState.builder(ClusterName.DEFAULT).build(), "test");
        before = createIndex(before, "test_2");

        // Now remove "test" and add an alias to "test" to "test_2" in one go
        ClusterState after = service.applyAliasActions(before, Arrays.asList(
                new AliasAction.Add("test_2", "test", null, null, null, null, null),
                new AliasAction.RemoveIndex("test")));
        IndexAbstraction alias = after.metadata().getIndicesLookup().get("test");
        assertNotNull(alias);
        assertThat(alias.getType(), equalTo(IndexAbstraction.Type.ALIAS));
        assertThat(alias.getIndices(), contains(after.metadata().index("test_2")));
        assertAliasesVersionIncreased("test_2", before, after);
    }

    public void testAddAliasToRemovedIndex() {
        // Create "test"
        ClusterState before = createIndex(ClusterState.builder(ClusterName.DEFAULT).build(), "test");

        // Attempt to add an alias to "test" at the same time as we remove it
        IndexNotFoundException e = expectThrows(IndexNotFoundException.class, () -> service.applyAliasActions(before, Arrays.asList(
                new AliasAction.Add("test", "alias", null, null, null, null, null),
                new AliasAction.RemoveIndex("test"))));
        assertEquals("test", e.getIndex().getName());
    }

    public void testRemoveIndexTwice() {
        // Create "test"
        ClusterState before = createIndex(ClusterState.builder(ClusterName.DEFAULT).build(), "test");

        // Try to remove an index twice. This should just remove the index once....
        ClusterState after = service.applyAliasActions(before, Arrays.asList(
                new AliasAction.RemoveIndex("test"),
                new AliasAction.RemoveIndex("test")));
        assertNull(after.metadata().getIndicesLookup().get("test"));
    }

    public void testAddWriteOnlyWithNoExistingAliases() {
        ClusterState before = createIndex(ClusterState.builder(ClusterName.DEFAULT).build(), "test");

        ClusterState after = service.applyAliasActions(before, Arrays.asList(
            new AliasAction.Add("test", "alias", null, null, null, false, null)));
        assertFalse(after.metadata().index("test").getAliases().get("alias").writeIndex());
        assertNull(after.metadata().getIndicesLookup().get("alias").getWriteIndex());
        assertAliasesVersionIncreased("test", before, after);

        after = service.applyAliasActions(before, Arrays.asList(
            new AliasAction.Add("test", "alias", null, null, null, null, null)));
        assertNull(after.metadata().index("test").getAliases().get("alias").writeIndex());
        assertThat(after.metadata().getIndicesLookup().get("alias").getWriteIndex(),
            equalTo(after.metadata().index("test")));
        assertAliasesVersionIncreased("test", before, after);

        after = service.applyAliasActions(before, Arrays.asList(
            new AliasAction.Add("test", "alias", null, null, null, true, null)));
        assertTrue(after.metadata().index("test").getAliases().get("alias").writeIndex());
        assertThat(after.metadata().getIndicesLookup().get("alias").getWriteIndex(),
            equalTo(after.metadata().index("test")));
        assertAliasesVersionIncreased("test", before, after);
    }

    public void testAddWriteOnlyWithExistingWriteIndex() {
        IndexMetadata.Builder indexMetadata = IndexMetadata.builder("test")
            .settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1);
        IndexMetadata.Builder indexMetadata2 = IndexMetadata.builder("test2")
            .putAlias(AliasMetadata.builder("alias").writeIndex(true).build())
            .settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1);
        ClusterState before = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(indexMetadata).put(indexMetadata2)).build();

        ClusterState after = service.applyAliasActions(before, Arrays.asList(
            new AliasAction.Add("test", "alias", null, null, null, null, null)));
        assertNull(after.metadata().index("test").getAliases().get("alias").writeIndex());
        assertThat(after.metadata().getIndicesLookup().get("alias").getWriteIndex(),
            equalTo(after.metadata().index("test2")));
        assertAliasesVersionIncreased("test", before, after);
        assertAliasesVersionUnchanged("test2", before, after);

        Exception exception = expectThrows(IllegalStateException.class, () -> service.applyAliasActions(before, Arrays.asList(
            new AliasAction.Add("test", "alias", null, null, null, true, null))));
        assertThat(exception.getMessage(), startsWith("alias [alias] has more than one write index ["));
    }

    public void testSwapWriteOnlyIndex() {
        IndexMetadata.Builder indexMetadata = IndexMetadata.builder("test")
            .putAlias(AliasMetadata.builder("alias").writeIndex(true).build())
            .settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1);
        IndexMetadata.Builder indexMetadata2 = IndexMetadata.builder("test2")
            .settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1);
        ClusterState before = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(indexMetadata).put(indexMetadata2)).build();

        Boolean unsetValue = randomBoolean() ? null : false;
        List<AliasAction> swapActions = Arrays.asList(
            new AliasAction.Add("test", "alias", null, null, null, unsetValue, null),
            new AliasAction.Add("test2", "alias", null, null, null, true, null)
        );
        Collections.shuffle(swapActions, random());
        ClusterState after = service.applyAliasActions(before, swapActions);
        assertThat(after.metadata().index("test").getAliases().get("alias").writeIndex(), equalTo(unsetValue));
        assertTrue(after.metadata().index("test2").getAliases().get("alias").writeIndex());
        assertThat(after.metadata().getIndicesLookup().get("alias").getWriteIndex(),
            equalTo(after.metadata().index("test2")));
        assertAliasesVersionIncreased("test", before, after);
        assertAliasesVersionIncreased("test2", before, after);
    }

    public void testAddWriteOnlyWithExistingNonWriteIndices() {
        IndexMetadata.Builder indexMetadata = IndexMetadata.builder("test")
            .putAlias(AliasMetadata.builder("alias").writeIndex(randomBoolean() ? null : false).build())
            .settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1);
        IndexMetadata.Builder indexMetadata2 = IndexMetadata.builder("test2")
            .putAlias(AliasMetadata.builder("alias").writeIndex(randomBoolean() ? null : false).build())
            .settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1);
        IndexMetadata.Builder indexMetadata3 = IndexMetadata.builder("test3")
            .settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1);
        ClusterState before = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(indexMetadata).put(indexMetadata2).put(indexMetadata3)).build();

        assertNull(before.metadata().getIndicesLookup().get("alias").getWriteIndex());

        ClusterState after = service.applyAliasActions(before, Arrays.asList(
            new AliasAction.Add("test3", "alias", null, null, null, true, null)));
        assertTrue(after.metadata().index("test3").getAliases().get("alias").writeIndex());
        assertThat(after.metadata().getIndicesLookup().get("alias").getWriteIndex(),
            equalTo(after.metadata().index("test3")));
        assertAliasesVersionUnchanged("test", before, after);
        assertAliasesVersionUnchanged("test2", before, after);
        assertAliasesVersionIncreased("test3", before, after);
    }

    public void testAddWriteOnlyWithIndexRemoved() {
        IndexMetadata.Builder indexMetadata = IndexMetadata.builder("test")
            .putAlias(AliasMetadata.builder("alias").build())
            .settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1);
        IndexMetadata.Builder indexMetadata2 = IndexMetadata.builder("test2")
            .putAlias(AliasMetadata.builder("alias").build())
            .settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1);
        ClusterState before = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(indexMetadata).put(indexMetadata2)).build();

        assertNull(before.metadata().index("test").getAliases().get("alias").writeIndex());
        assertNull(before.metadata().index("test2").getAliases().get("alias").writeIndex());
        assertNull(before.metadata().getIndicesLookup().get("alias").getWriteIndex());

        ClusterState after = service.applyAliasActions(before, Collections.singletonList(new AliasAction.RemoveIndex("test")));
        assertNull(after.metadata().index("test2").getAliases().get("alias").writeIndex());
        assertThat(after.metadata().getIndicesLookup().get("alias").getWriteIndex(),
            equalTo(after.metadata().index("test2")));
        assertAliasesVersionUnchanged("test2", before, after);
    }

    public void testAddWriteOnlyValidatesAgainstMetadataBuilder() {
        IndexMetadata.Builder indexMetadata = IndexMetadata.builder("test")
            .settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1);
        IndexMetadata.Builder indexMetadata2 = IndexMetadata.builder("test2")
            .settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1);
        ClusterState before = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(indexMetadata).put(indexMetadata2)).build();

        Exception exception = expectThrows(IllegalStateException.class, () -> service.applyAliasActions(before, Arrays.asList(
            new AliasAction.Add("test", "alias", null, null, null, true, null),
            new AliasAction.Add("test2", "alias", null, null, null, true, null)
        )));
        assertThat(exception.getMessage(), startsWith("alias [alias] has more than one write index ["));
    }

    public void testHiddenPropertyValidation() {
        ClusterState originalState = ClusterState.EMPTY_STATE;
        originalState = createIndex(originalState, "test1");
        originalState = createIndex(originalState, "test2");

        {
            // Add a non-hidden alias to one index
            ClusterState testState = service.applyAliasActions(originalState, Collections.singletonList(
                new AliasAction.Add("test1", "alias", null, null, null, null, randomFrom(false, null))
            ));

            // Adding the same alias as hidden to another index should throw
            Exception ex = expectThrows(IllegalStateException.class, () -> // Add a non-hidden alias to one index
                service.applyAliasActions(testState, Collections.singletonList(
                    new AliasAction.Add("test2", "alias", null, null, null, null, true)
                )));
            assertThat(ex.getMessage(), containsString("alias [alias] has is_hidden set to true on indices"));
        }

        {
            // Add a hidden alias to one index
            ClusterState testState = service.applyAliasActions(originalState, Collections.singletonList(
                new AliasAction.Add("test1", "alias", null, null, null, null, true)
            ));

            // Adding the same alias as non-hidden to another index should throw
            Exception ex = expectThrows(IllegalStateException.class, () -> // Add a non-hidden alias to one index
                service.applyAliasActions(testState, Collections.singletonList(
                    new AliasAction.Add("test2", "alias", null, null, null, null, randomFrom(false, null))
                )));
            assertThat(ex.getMessage(), containsString("alias [alias] has is_hidden set to true on indices"));
        }

        {
            // Add a non-hidden alias to one index
            ClusterState testState = service.applyAliasActions(originalState, Collections.singletonList(
                new AliasAction.Add("test1", "alias", null, null, null, null, randomFrom(false, null))
            ));

            // Adding the same alias as non-hidden should be OK
            service.applyAliasActions(testState, Collections.singletonList(
                    new AliasAction.Add("test2", "alias", null, null, null, null, randomFrom(false, null))
            ));
        }

        {
            // Add a hidden alias to one index
            ClusterState testState = service.applyAliasActions(originalState, Collections.singletonList(
                new AliasAction.Add("test1", "alias", null, null, null, null, true)
            ));

            // Adding the same alias as hidden should be OK
            service.applyAliasActions(testState, Collections.singletonList(
                new AliasAction.Add("test2", "alias", null, null, null, null, true)
            ));
        }
    }

    public void testSimultaneousHiddenPropertyValidation() {
        IndexMetadata.Builder indexMetadata = IndexMetadata.builder("test")
            .settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1);
        IndexMetadata.Builder indexMetadata2 = IndexMetadata.builder("test2")
            .settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1);
        ClusterState before = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(indexMetadata).put(indexMetadata2)).build();

        {
            // These should all be fine
            applyHiddenAliasMix(before, null, null);
            applyHiddenAliasMix(before, false, false);
            applyHiddenAliasMix(before, false, null);
            applyHiddenAliasMix(before, null, false);

            applyHiddenAliasMix(before, true, true);
        }

        {
            Exception exception = expectThrows(IllegalStateException.class,
                () -> applyHiddenAliasMix(before, true, randomFrom(false, null)));
            assertThat(exception.getMessage(), startsWith("alias [alias] has is_hidden set to true on indices ["));
        }

        {
            Exception exception = expectThrows(IllegalStateException.class,
                () -> applyHiddenAliasMix(before, randomFrom(false, null), true));
            assertThat(exception.getMessage(), startsWith("alias [alias] has is_hidden set to true on indices ["));
        }
    }

    public void testAliasesForDataStreamBackingIndicesNotSupported() {
        String dataStreamName = "foo-stream";
        String backingIndexName = DataStream.getDefaultBackingIndexName(dataStreamName, 1);
        IndexMetadata indexMetadata = IndexMetadata.builder(backingIndexName)
            .settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1).build();
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(
                Metadata.builder()
                    .put(indexMetadata, true)
                    .put(new DataStream(dataStreamName, createTimestampField("@timestamp"), singletonList(indexMetadata.getIndex()))))
            .build();

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> service.applyAliasActions(state,
            singletonList(new AliasAction.Add(backingIndexName, "test", null, null, null, null, null))));
        assertThat(exception.getMessage(), is("The provided index [ .ds-foo-stream-000001] is a backing index belonging to data stream " +
            "[foo-stream]. Data streams and their backing indices don't support alias operations."));
    }

    private ClusterState applyHiddenAliasMix(ClusterState before, Boolean isHidden1, Boolean isHidden2) {
        return service.applyAliasActions(before, Arrays.asList(
            new AliasAction.Add("test", "alias", null, null, null, null, isHidden1),
            new AliasAction.Add("test2", "alias", null, null, null, null, isHidden2)
        ));
    }

    private ClusterState createIndex(ClusterState state, String index) {
        IndexMetadata indexMetadata = IndexMetadata.builder(index)
                .settings(Settings.builder().put("index.version.created", VersionUtils.randomVersion(random())))
                .numberOfShards(1)
                .numberOfReplicas(1)
                .build();
        return ClusterState.builder(state)
                .metadata(Metadata.builder(state.metadata()).put(indexMetadata, false))
                .build();
    }

    private void assertAliasesVersionUnchanged(final String index, final ClusterState before, final ClusterState after) {
        assertAliasesVersionUnchanged(new String[]{index}, before, after);
    }

    private void assertAliasesVersionUnchanged(final String[] indices, final ClusterState before, final ClusterState after) {
        for (final String index : indices) {
            final long expected = before.metadata().index(index).getAliasesVersion();
            final long actual = after.metadata().index(index).getAliasesVersion();
            assertThat("index metadata aliases version mismatch", actual, equalTo(expected));
        }
    }

    private void assertAliasesVersionIncreased(final String index, final ClusterState before, final ClusterState after) {
        assertAliasesVersionIncreased(new String[]{index}, before, after);
    }

    private void assertAliasesVersionIncreased(final String[] indices, final ClusterState before, final ClusterState after) {
        for (final String index : indices) {
            final long expected = 1 + before.metadata().index(index).getAliasesVersion();
            final long actual = after.metadata().index(index).getAliasesVersion();
            assertThat("index metadata aliases version mismatch", actual, equalTo(expected));
        }
    }

}

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

package org.havenask.action.support;

import org.havenask.LegacyESVersion;
import org.havenask.Version;
import org.havenask.action.support.IndicesOptions.Option;
import org.havenask.action.support.IndicesOptions.WildcardStates;
import org.havenask.common.bytes.BytesReference;
import org.havenask.common.io.stream.BytesStreamOutput;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.xcontent.NamedXContentRegistry;
import org.havenask.common.xcontent.ToXContent.MapParams;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.common.xcontent.XContentFactory;
import org.havenask.common.xcontent.XContentParser;
import org.havenask.common.xcontent.XContentType;
import org.havenask.test.HavenaskTestCase;
import org.havenask.test.EqualsHashCodeTestUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;

import static org.havenask.test.VersionUtils.randomVersionBetween;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.is;

public class IndicesOptionsTests extends HavenaskTestCase {

    public void testSerialization() throws Exception {
        int iterations = randomIntBetween(5, 20);
        for (int i = 0; i < iterations; i++) {
            Version version = randomVersionBetween(random(), LegacyESVersion.V_7_0_0, null);
            IndicesOptions indicesOptions = IndicesOptions.fromOptions(
                randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean(),
                randomBoolean(), randomBoolean());

            BytesStreamOutput output = new BytesStreamOutput();
            output.setVersion(version);
            indicesOptions.writeIndicesOptions(output);

            StreamInput streamInput = output.bytes().streamInput();
            streamInput.setVersion(version);
            IndicesOptions indicesOptions2 = IndicesOptions.readIndicesOptions(streamInput);

            assertThat(indicesOptions2.ignoreUnavailable(), equalTo(indicesOptions.ignoreUnavailable()));
            assertThat(indicesOptions2.allowNoIndices(), equalTo(indicesOptions.allowNoIndices()));
            assertThat(indicesOptions2.expandWildcardsOpen(), equalTo(indicesOptions.expandWildcardsOpen()));
            assertThat(indicesOptions2.expandWildcardsClosed(), equalTo(indicesOptions.expandWildcardsClosed()));
            if (version.before(LegacyESVersion.V_7_7_0)) {
                assertThat(indicesOptions2.expandWildcardsHidden(), is(true));
            } else {
                assertThat(indicesOptions2.expandWildcardsHidden(), equalTo(indicesOptions.expandWildcardsHidden()));
            }

            assertThat(indicesOptions2.forbidClosedIndices(), equalTo(indicesOptions.forbidClosedIndices()));
            assertThat(indicesOptions2.allowAliasesToMultipleIndices(), equalTo(indicesOptions.allowAliasesToMultipleIndices()));

            assertEquals(indicesOptions2.ignoreAliases(), indicesOptions.ignoreAliases());
        }
    }

    public void testSerializationPre70() throws Exception {
        int iterations = randomIntBetween(5, 20);
        List<Version> declaredVersions = Version.getDeclaredVersions(LegacyESVersion.class);
        OptionalInt maxV6Id = declaredVersions.stream().filter(v -> v.major == 6).mapToInt(v -> v.id).max();
        assertTrue(maxV6Id.isPresent());
        final Version maxVersion = Version.fromId(maxV6Id.getAsInt());
        for (int i = 0; i < iterations; i++) {
            Version version = randomVersionBetween(random(), Version.CURRENT.minimumCompatibilityVersion(), maxVersion);
            IndicesOptions indicesOptions = IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean(),
                    randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean());

            BytesStreamOutput output = new BytesStreamOutput();
            output.setVersion(version);
            indicesOptions.writeIndicesOptions(output);

            StreamInput streamInput = output.bytes().streamInput();
            streamInput.setVersion(version);
            IndicesOptions indicesOptions2 = IndicesOptions.readIndicesOptions(streamInput);

            assertThat(indicesOptions2.ignoreUnavailable(), equalTo(indicesOptions.ignoreUnavailable()));
            assertThat(indicesOptions2.allowNoIndices(), equalTo(indicesOptions.allowNoIndices()));
            assertThat(indicesOptions2.expandWildcardsOpen(), equalTo(indicesOptions.expandWildcardsOpen()));
            assertThat(indicesOptions2.expandWildcardsClosed(), equalTo(indicesOptions.expandWildcardsClosed()));

            assertThat(indicesOptions2.forbidClosedIndices(), equalTo(indicesOptions.forbidClosedIndices()));
            assertThat(indicesOptions2.allowAliasesToMultipleIndices(), equalTo(indicesOptions.allowAliasesToMultipleIndices()));

            assertEquals(indicesOptions2.ignoreAliases(), indicesOptions.ignoreAliases());
            assertEquals(indicesOptions2.ignoreThrottled(), indicesOptions.ignoreThrottled());
        }
    }

    public void testFromOptions() {
        final boolean ignoreUnavailable = randomBoolean();
        final boolean allowNoIndices = randomBoolean();
        final boolean expandToOpenIndices = randomBoolean();
        final boolean expandToClosedIndices = randomBoolean();
        final boolean expandToHiddenIndices = randomBoolean();
        final boolean allowAliasesToMultipleIndices = randomBoolean();
        final boolean forbidClosedIndices = randomBoolean();
        final boolean ignoreAliases = randomBoolean();
        final boolean ignoreThrottled = randomBoolean();

        IndicesOptions indicesOptions = IndicesOptions.fromOptions(ignoreUnavailable, allowNoIndices, expandToOpenIndices,
            expandToClosedIndices, expandToHiddenIndices, allowAliasesToMultipleIndices, forbidClosedIndices, ignoreAliases,
            ignoreThrottled);

        assertThat(indicesOptions.ignoreUnavailable(), equalTo(ignoreUnavailable));
        assertThat(indicesOptions.allowNoIndices(), equalTo(allowNoIndices));
        assertThat(indicesOptions.expandWildcardsOpen(), equalTo(expandToOpenIndices));
        assertThat(indicesOptions.expandWildcardsClosed(), equalTo(expandToClosedIndices));
        assertThat(indicesOptions.expandWildcardsHidden(), equalTo(expandToHiddenIndices));
        assertThat(indicesOptions.allowAliasesToMultipleIndices(), equalTo(allowAliasesToMultipleIndices));
        assertThat(indicesOptions.allowAliasesToMultipleIndices(), equalTo(allowAliasesToMultipleIndices));
        assertThat(indicesOptions.forbidClosedIndices(), equalTo(forbidClosedIndices));
        assertEquals(ignoreAliases, indicesOptions.ignoreAliases());
        assertEquals(ignoreThrottled, indicesOptions.ignoreThrottled());
    }

    public void testFromOptionsWithDefaultOptions() {
        boolean ignoreUnavailable = randomBoolean();
        boolean allowNoIndices = randomBoolean();
        boolean expandToOpenIndices = randomBoolean();
        boolean expandToClosedIndices = randomBoolean();

        IndicesOptions defaultOptions = IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean(),
                randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean());

        IndicesOptions indicesOptions = IndicesOptions.fromOptions(ignoreUnavailable, allowNoIndices, expandToOpenIndices,
                expandToClosedIndices, defaultOptions);

        assertEquals(ignoreUnavailable, indicesOptions.ignoreUnavailable());
        assertEquals(allowNoIndices, indicesOptions.allowNoIndices());
        assertEquals(expandToOpenIndices, indicesOptions.expandWildcardsOpen());
        assertEquals(expandToClosedIndices, indicesOptions.expandWildcardsClosed());
        assertEquals(defaultOptions.expandWildcardsHidden(), indicesOptions.expandWildcardsHidden());
        assertEquals(defaultOptions.allowAliasesToMultipleIndices(), indicesOptions.allowAliasesToMultipleIndices());
        assertEquals(defaultOptions.forbidClosedIndices(), indicesOptions.forbidClosedIndices());
        assertEquals(defaultOptions.ignoreAliases(), indicesOptions.ignoreAliases());
    }

    public void testFromParameters() {
        final boolean expandWildcardsOpen = randomBoolean();
        final boolean expandWildcardsClosed = randomBoolean();
        final boolean expandWildcardsHidden = randomBoolean();
        final String expandWildcardsString;
        if (expandWildcardsOpen && expandWildcardsClosed && expandWildcardsHidden) {
            if (randomBoolean()) {
                expandWildcardsString = "open,closed,hidden";
            } else {
                expandWildcardsString = "all";
            }
        } else {
            List<String> values = new ArrayList<>();
            if (expandWildcardsOpen) {
                values.add("open");
            }
            if (expandWildcardsClosed) {
                values.add("closed");
            }
            if (expandWildcardsHidden) {
                values.add("hidden");
            }
            if (values.isEmpty() && randomBoolean()) {
                values.add("none");
            }
            expandWildcardsString = String.join(",", values);
        }
        boolean ignoreUnavailable = randomBoolean();
        String ignoreUnavailableString = Boolean.toString(ignoreUnavailable);
        boolean ignoreThrottled = randomBoolean();
        boolean allowNoIndices = randomBoolean();
        String allowNoIndicesString = Boolean.toString(allowNoIndices);

        IndicesOptions defaultOptions = IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean(),
                randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean());

        IndicesOptions updatedOptions = IndicesOptions.fromParameters(expandWildcardsString, ignoreUnavailableString,
                allowNoIndicesString, ignoreThrottled, defaultOptions);

        assertEquals(expandWildcardsOpen, updatedOptions.expandWildcardsOpen());
        assertEquals(expandWildcardsClosed, updatedOptions.expandWildcardsClosed());
        assertEquals(expandWildcardsHidden, updatedOptions.expandWildcardsHidden());
        assertEquals(ignoreUnavailable, updatedOptions.ignoreUnavailable());
        assertEquals(allowNoIndices, updatedOptions.allowNoIndices());
        assertEquals(defaultOptions.allowAliasesToMultipleIndices(), updatedOptions.allowAliasesToMultipleIndices());
        assertEquals(defaultOptions.forbidClosedIndices(), updatedOptions.forbidClosedIndices());
        assertEquals(defaultOptions.ignoreAliases(), updatedOptions.ignoreAliases());
    }

    public void testEqualityAndHashCode() {
        IndicesOptions indicesOptions = IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean(),
            randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean());

        EqualsHashCodeTestUtils.checkEqualsAndHashCode(indicesOptions, opts -> {
            return IndicesOptions.fromOptions(opts.ignoreUnavailable(), opts.allowNoIndices(), opts.expandWildcardsOpen(),
                opts.expandWildcardsClosed(), opts.expandWildcardsHidden(), opts.allowAliasesToMultipleIndices(),
                opts.forbidClosedIndices(), opts.ignoreAliases(), opts.ignoreThrottled());
        }, opts -> {
            boolean mutated = false;
            boolean ignoreUnavailable = opts.ignoreUnavailable();
            boolean allowNoIndices = opts.allowNoIndices();
            boolean expandOpen = opts.expandWildcardsOpen();
            boolean expandClosed = opts.expandWildcardsClosed();
            boolean expandHidden = opts.expandWildcardsHidden();
            boolean allowAliasesToMulti = opts.allowAliasesToMultipleIndices();
            boolean forbidClosed = opts.forbidClosedIndices();
            boolean ignoreAliases = opts.ignoreAliases();
            boolean ignoreThrottled = opts.ignoreThrottled();
            while (mutated == false) {
                if (randomBoolean()) {
                    ignoreUnavailable = !ignoreUnavailable;
                    mutated = true;
                }
                if (randomBoolean()) {
                    allowNoIndices = !allowNoIndices;
                    mutated = true;
                }
                if (randomBoolean()) {
                    expandOpen = !expandOpen;
                    mutated = true;
                }
                if (randomBoolean()) {
                    expandClosed = !expandClosed;
                    mutated = true;
                }
                if (randomBoolean()) {
                    expandHidden = !expandHidden;
                    mutated = true;
                }
                if (randomBoolean()) {
                    allowAliasesToMulti = !allowAliasesToMulti;
                    mutated = true;
                }
                if (randomBoolean()) {
                    forbidClosed = !forbidClosed;
                    mutated = true;
                }
                if (randomBoolean()) {
                    ignoreAliases = !ignoreAliases;
                    mutated = true;
                }
                if (randomBoolean()) {
                    ignoreThrottled = !ignoreThrottled;
                    mutated = true;
                }
            }
            return IndicesOptions.fromOptions(ignoreUnavailable, allowNoIndices, expandOpen, expandClosed, expandHidden,
                allowAliasesToMulti, forbidClosed, ignoreAliases, ignoreThrottled);
        });
    }

    public void testFromMap() {
        IndicesOptions defaults = IndicesOptions.strictExpandOpen();
        Collection<String> wildcardStates = randomBoolean() ?
                null : randomSubsetOf(Arrays.asList("open", "closed", "hidden"));
        Boolean ignoreUnavailable = randomBoolean() ? null : randomBoolean();
        Boolean allowNoIndices = randomBoolean() ? null : randomBoolean();
        Boolean ignoreThrottled = randomBoolean() ? null : randomBoolean();

        Map<String, Object> settings = new HashMap<>();

        if (wildcardStates != null) {
            settings.put("expand_wildcards", wildcardStates);
        }

        if (ignoreUnavailable != null) {
            settings.put("ignore_unavailable", ignoreUnavailable);
        }

        if (allowNoIndices != null) {
            settings.put("allow_no_indices", allowNoIndices);
        }

        if (ignoreThrottled != null) {
            settings.put("ignore_throttled", ignoreThrottled);
        }

        IndicesOptions fromMap = IndicesOptions.fromMap(settings, defaults);

        boolean open = wildcardStates != null ? wildcardStates.contains("open") : defaults.expandWildcardsOpen();
        assertEquals(open, fromMap.expandWildcardsOpen());
        boolean closed = wildcardStates != null ? wildcardStates.contains("closed") : defaults.expandWildcardsClosed();
        assertEquals(closed, fromMap.expandWildcardsClosed());
        boolean hidden = wildcardStates != null ? wildcardStates.contains("hidden") : defaults.expandWildcardsHidden();
        assertEquals(hidden, fromMap.expandWildcardsHidden());

        assertEquals(ignoreUnavailable == null ? defaults.ignoreUnavailable() : ignoreUnavailable, fromMap.ignoreUnavailable());
        assertEquals(allowNoIndices == null ? defaults.allowNoIndices() : allowNoIndices, fromMap.allowNoIndices());
        assertEquals(ignoreThrottled == null ? defaults.ignoreThrottled() : ignoreThrottled, fromMap.ignoreThrottled());
    }

    public void testToXContent() throws IOException {
        Collection<WildcardStates> wildcardStates = randomSubsetOf(Arrays.asList(WildcardStates.values()));
        Collection<Option> options = randomSubsetOf(Arrays.asList(Option.values()));

        IndicesOptions indicesOptions = new IndicesOptions(
                options.isEmpty() ? Option.NONE : EnumSet.copyOf(options),
                wildcardStates.isEmpty() ? WildcardStates.NONE : EnumSet.copyOf(wildcardStates));

        XContentType type = randomFrom(XContentType.values());
        BytesReference xContentBytes = toXContentBytes(indicesOptions, type);
        Map<String, Object> map;
        try (XContentParser parser = type.xContent().createParser(
            NamedXContentRegistry.EMPTY, null, xContentBytes.streamInput())) {
            map = parser.mapOrdered();
        }

        boolean open = wildcardStates.contains(WildcardStates.OPEN);
        if (open) {
            assertTrue(((List<?>) map.get("expand_wildcards")).contains("open"));
        } else {
            assertFalse(((List<?>) map.get("expand_wildcards")).contains("open"));
        }
        boolean closed = wildcardStates.contains(WildcardStates.CLOSED);
        if (closed) {
            assertTrue(((List<?>) map.get("expand_wildcards")).contains("closed"));
        } else {
            assertFalse(((List<?>) map.get("expand_wildcards")).contains("closed"));
        }
        assertEquals(wildcardStates.contains(WildcardStates.HIDDEN), ((List<?>) map.get("expand_wildcards")).contains("hidden"));
        assertEquals(map.get("ignore_unavailable"), options.contains(Option.IGNORE_UNAVAILABLE));
        assertEquals(map.get("allow_no_indices"), options.contains(Option.ALLOW_NO_INDICES));
        assertEquals(map.get("ignore_throttled"), options.contains(Option.IGNORE_THROTTLED));
    }

    public void testFromXContent() throws IOException {
        Collection<WildcardStates> wildcardStates = randomSubsetOf(Arrays.asList(WildcardStates.values()));
        Collection<Option> options = randomSubsetOf(Arrays.asList(Option.values()));

        IndicesOptions indicesOptions = new IndicesOptions(
            options.isEmpty() ? Option.NONE : EnumSet.copyOf(options),
            wildcardStates.isEmpty() ? WildcardStates.NONE : EnumSet.copyOf(wildcardStates));

        XContentType type = randomFrom(XContentType.values());
        BytesReference xContentBytes = toXContentBytes(indicesOptions, type);
        IndicesOptions fromXContentOptions;
        try (XContentParser parser = type.xContent().createParser(
            NamedXContentRegistry.EMPTY, null, xContentBytes.streamInput())) {
            fromXContentOptions = IndicesOptions.fromXContent(parser);
        }

        assertEquals(indicesOptions.expandWildcardsClosed(), fromXContentOptions.expandWildcardsClosed());
        assertEquals(indicesOptions.expandWildcardsHidden(), fromXContentOptions.expandWildcardsHidden());
        assertEquals(indicesOptions.expandWildcardsOpen(), fromXContentOptions.expandWildcardsOpen());
        assertEquals(indicesOptions.ignoreUnavailable(), fromXContentOptions.ignoreUnavailable());
        assertEquals(indicesOptions.allowNoIndices(), fromXContentOptions.allowNoIndices());
        assertEquals(indicesOptions.ignoreThrottled(), fromXContentOptions.ignoreThrottled());
    }

    public void testFromXContentWithWildcardSpecialValues() throws IOException {
        XContentType type = randomFrom(XContentType.values());
        final boolean ignoreUnavailable = randomBoolean();
        final boolean allowNoIndices = randomBoolean();

        BytesReference xContentBytes;
        try (XContentBuilder builder = XContentFactory.contentBuilder(type)) {
            builder.startObject();
            builder.field("expand_wildcards", "all");
            builder.field("ignore_unavailable", ignoreUnavailable);
            builder.field("allow_no_indices", allowNoIndices);
            builder.endObject();
            xContentBytes = BytesReference.bytes(builder);
        }

        IndicesOptions fromXContentOptions;
        try (XContentParser parser = type.xContent().createParser(
            NamedXContentRegistry.EMPTY, null, xContentBytes.streamInput())) {
            fromXContentOptions = IndicesOptions.fromXContent(parser);
        }
        assertEquals(ignoreUnavailable, fromXContentOptions.ignoreUnavailable());
        assertEquals(allowNoIndices, fromXContentOptions.allowNoIndices());
        assertTrue(fromXContentOptions.expandWildcardsClosed());
        assertTrue(fromXContentOptions.expandWildcardsHidden());
        assertTrue(fromXContentOptions.expandWildcardsOpen());

        try (XContentBuilder builder = XContentFactory.contentBuilder(type)) {
            builder.startObject();
            builder.field("expand_wildcards", "none");
            builder.field("ignore_unavailable", ignoreUnavailable);
            builder.field("allow_no_indices", allowNoIndices);
            builder.endObject();
            xContentBytes = BytesReference.bytes(builder);
        }

        try (XContentParser parser = type.xContent().createParser(
            NamedXContentRegistry.EMPTY, null, xContentBytes.streamInput())) {
            fromXContentOptions = IndicesOptions.fromXContent(parser);
        }
        assertEquals(ignoreUnavailable, fromXContentOptions.ignoreUnavailable());
        assertEquals(allowNoIndices, fromXContentOptions.allowNoIndices());
        assertFalse(fromXContentOptions.expandWildcardsClosed());
        assertFalse(fromXContentOptions.expandWildcardsHidden());
        assertFalse(fromXContentOptions.expandWildcardsOpen());
    }

    private BytesReference toXContentBytes(IndicesOptions indicesOptions, XContentType type) throws IOException {
        try (XContentBuilder builder = XContentFactory.contentBuilder(type)) {
            builder.startObject();
            indicesOptions.toXContent(builder, new MapParams(Collections.emptyMap()));
            builder.endObject();
            return BytesReference.bytes(builder);
        }
    }
}

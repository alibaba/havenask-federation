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

package org.havenask.plugins.spi;

import org.havenask.common.ParseField;
import org.havenask.common.io.Streams;
import org.havenask.common.xcontent.NamedXContentRegistry;
import org.havenask.search.aggregations.Aggregation;
import org.havenask.search.aggregations.pipeline.ParsedSimpleValue;
import org.havenask.search.suggest.Suggest;
import org.havenask.search.suggest.term.TermSuggestion;
import org.havenask.test.HavenaskTestCase;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.ServiceLoader;
import java.util.function.Predicate;

public class NamedXContentProviderTests extends HavenaskTestCase {

    public void testSpiFileExists() throws IOException {
        String serviceFile = "/META-INF/services/" + NamedXContentProvider.class.getName();
        List<String> implementations = new ArrayList<>();
        try (InputStream input = NamedXContentProviderTests.class.getResourceAsStream(serviceFile)) {
            Streams.readAllLines(input, implementations::add);
        }

        assertEquals(1, implementations.size());
        assertEquals(TestNamedXContentProvider.class.getName(), implementations.get(0));
    }

    public void testNamedXContents() {
        final List<NamedXContentRegistry.Entry> namedXContents = new ArrayList<>();
        for (NamedXContentProvider service : ServiceLoader.load(NamedXContentProvider.class)) {
            namedXContents.addAll(service.getNamedXContentParsers());
        }

        assertEquals(2, namedXContents.size());

        List<Predicate<NamedXContentRegistry.Entry>> predicates = new ArrayList<>(2);
        predicates.add(e -> Aggregation.class.equals(e.categoryClass) && "test_aggregation".equals(e.name.getPreferredName()));
        predicates.add(e -> Suggest.Suggestion.class.equals(e.categoryClass) && "test_suggestion".equals(e.name.getPreferredName()));
        predicates.forEach(predicate -> assertEquals(1, namedXContents.stream().filter(predicate).count()));
    }

    public static class TestNamedXContentProvider implements NamedXContentProvider {

        public TestNamedXContentProvider() {
        }

        @Override
        public List<NamedXContentRegistry.Entry> getNamedXContentParsers() {
            return Arrays.asList(
                    new NamedXContentRegistry.Entry(Aggregation.class, new ParseField("test_aggregation"),
                            (parser, context) -> ParsedSimpleValue.fromXContent(parser, (String) context)),
                    new NamedXContentRegistry.Entry(Suggest.Suggestion.class, new ParseField("test_suggestion"),
                            (parser, context) -> TermSuggestion.fromXContent(parser, (String) context))
            );
        }
    }
}

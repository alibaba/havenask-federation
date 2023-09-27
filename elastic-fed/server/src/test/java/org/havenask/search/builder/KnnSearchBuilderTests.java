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

package org.havenask.search.builder;

import java.io.IOException;

import org.havenask.common.io.stream.NamedWriteableRegistry;
import org.havenask.common.io.stream.Writeable;
import org.havenask.common.settings.Settings;
import org.havenask.common.xcontent.NamedXContentRegistry;
import org.havenask.common.xcontent.XContentParser;
import org.havenask.index.query.QueryBuilders;
import org.havenask.search.SearchModule;
import org.havenask.test.AbstractSerializingTestCase;
import org.havenask.test.HavenaskTestCase;
import org.junit.Before;

import static java.util.Collections.emptyList;
import static org.hamcrest.Matchers.containsString;

public class KnnSearchBuilderTests extends AbstractSerializingTestCase<KnnSearchBuilder> {
    private NamedWriteableRegistry namedWriteableRegistry;
    private NamedXContentRegistry namedXContentRegistry;

    public static KnnSearchBuilder randomTestInstance() {
        String field = randomAlphaOfLength(6);
        int dim = randomIntBetween(2, 30);
        float[] vector = randomVector(dim);
        int k = randomIntBetween(1, 100);
        int numCands = randomIntBetween(k + 20, 1000);

        KnnSearchBuilder builder = new KnnSearchBuilder(field, vector, k, numCands, randomBoolean() ? null : randomFloat());
        if (randomBoolean()) {
            builder.boost(randomFloat());
        }

        int numFilters = randomIntBetween(0, 3);
        for (int i = 0; i < numFilters; i++) {
            builder.addFilterQuery(QueryBuilders.termQuery(randomAlphaOfLength(5), randomAlphaOfLength(10)));
        }

        return builder;
    }

    @Before
    public void registerNamedXContents() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, false, emptyList());
        namedXContentRegistry = new NamedXContentRegistry(searchModule.getNamedXContents());
        namedWriteableRegistry = new NamedWriteableRegistry(searchModule.getNamedWriteables());
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return namedXContentRegistry;
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return namedWriteableRegistry;
    }

    @Override
    protected KnnSearchBuilder doParseInstance(XContentParser parser) throws IOException {
        return KnnSearchBuilder.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<KnnSearchBuilder> instanceReader() {
        return KnnSearchBuilder::new;
    }

    @Override
    protected KnnSearchBuilder createTestInstance() {
        return randomTestInstance();
    }

    @Override
    protected KnnSearchBuilder mutateInstance(KnnSearchBuilder instance) {
        switch (random().nextInt(7)) {
            case 0:
                String newField = randomValueOtherThan(instance.field, () -> randomAlphaOfLength(5));
                return new KnnSearchBuilder(newField, instance.queryVector, instance.k, instance.numCands + 3, instance.similarity).boost(
                    instance.boost
                );
            case 1:
                float[] newVector = randomValueOtherThan(instance.queryVector, () -> randomVector(5));
                return new KnnSearchBuilder(instance.field, newVector, instance.k + 3, instance.numCands, instance.similarity).boost(
                    instance.boost
                );
            case 2:
                return new KnnSearchBuilder(instance.field, instance.queryVector, instance.k + 3, instance.numCands, instance.similarity)
                    .boost(instance.boost);
            case 3:
                return new KnnSearchBuilder(instance.field, instance.queryVector, instance.k, instance.numCands + 3, instance.similarity)
                    .boost(instance.boost);
            case 4:
                return new KnnSearchBuilder(instance.field, instance.queryVector, instance.k, instance.numCands, instance.similarity)
                    .addFilterQueries(instance.filterQueries)
                    .addFilterQuery(QueryBuilders.termQuery("new_field", "new-value"))
                    .boost(instance.boost);
            case 5:
                float newBoost = randomValueOtherThan(instance.boost, HavenaskTestCase::randomFloat);
                return new KnnSearchBuilder(instance.field, instance.queryVector, instance.k, instance.numCands, instance.similarity)
                    .addFilterQueries(instance.filterQueries)
                    .boost(newBoost);
            case 6:
                return new KnnSearchBuilder(
                    instance.field,
                    instance.queryVector,
                    instance.k,
                    instance.numCands,
                    randomValueOtherThan(instance.similarity, HavenaskTestCase::randomFloat)
                ).addFilterQueries(instance.filterQueries).boost(instance.boost);
            default:
                throw new IllegalStateException();
        }
    }

    public void testNumCandsLessThanK() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new KnnSearchBuilder("field", randomVector(3), 50, 10, null)
        );
        assertThat(e.getMessage(), containsString("[num_candidates] cannot be less than [k]"));
    }

    public void testNumCandsExceedsLimit() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new KnnSearchBuilder("field", randomVector(3), 100, 10002, null)
        );
        assertThat(e.getMessage(), containsString("[num_candidates] cannot exceed [10000]"));
    }

    public void testInvalidK() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new KnnSearchBuilder("field", randomVector(3), 0, 100, null)
        );
        assertThat(e.getMessage(), containsString("[k] must be greater than 0"));
    }

    static float[] randomVector(int dim) {
        float[] vector = new float[dim];
        for (int i = 0; i < vector.length; i++) {
            vector[i] = randomFloat();
        }
        return vector;
    }
}
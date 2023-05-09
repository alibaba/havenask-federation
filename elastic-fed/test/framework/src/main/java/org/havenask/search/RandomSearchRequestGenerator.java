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

package org.havenask.search;

import org.havenask.action.search.SearchRequest;
import org.havenask.action.search.SearchType;
import org.havenask.action.support.IndicesOptions;
import org.havenask.common.bytes.BytesReference;
import org.havenask.common.text.Text;
import org.havenask.common.unit.TimeValue;
import org.havenask.common.xcontent.DeprecationHandler;
import org.havenask.common.xcontent.NamedXContentRegistry;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.common.xcontent.XContentFactory;
import org.havenask.common.xcontent.XContentParser;
import org.havenask.common.xcontent.XContentType;
import org.havenask.index.query.QueryBuilders;
import org.havenask.script.Script;
import org.havenask.script.ScriptType;
import org.havenask.search.aggregations.AggregationBuilders;
import org.havenask.search.builder.PointInTimeBuilder;
import org.havenask.search.builder.SearchSourceBuilder;
import org.havenask.search.collapse.CollapseBuilder;
import org.havenask.search.fetch.subphase.FetchSourceContext;
import org.havenask.search.fetch.subphase.highlight.HighlightBuilder;
import org.havenask.search.internal.SearchContext;
import org.havenask.search.rescore.RescorerBuilder;
import org.havenask.search.searchafter.SearchAfterBuilder;
import org.havenask.search.slice.SliceBuilder;
import org.havenask.search.sort.ScriptSortBuilder;
import org.havenask.search.sort.SortBuilders;
import org.havenask.search.sort.SortOrder;
import org.havenask.search.suggest.SuggestBuilder;
import org.havenask.test.AbstractQueryTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static java.util.Collections.emptyMap;
import static org.havenask.test.HavenaskTestCase.between;
import static org.havenask.test.HavenaskTestCase.generateRandomStringArray;
import static org.havenask.test.HavenaskTestCase.mockScript;
import static org.havenask.test.HavenaskTestCase.randomAlphaOfLengthBetween;
import static org.havenask.test.HavenaskTestCase.randomBoolean;
import static org.havenask.test.HavenaskTestCase.randomByte;
import static org.havenask.test.HavenaskTestCase.randomDouble;
import static org.havenask.test.HavenaskTestCase.randomFloat;
import static org.havenask.test.HavenaskTestCase.randomFrom;
import static org.havenask.test.HavenaskTestCase.randomInt;
import static org.havenask.test.HavenaskTestCase.randomIntBetween;
import static org.havenask.test.HavenaskTestCase.randomLong;
import static org.havenask.test.HavenaskTestCase.randomPositiveTimeValue;
import static org.havenask.test.HavenaskTestCase.randomShort;
import static org.havenask.test.HavenaskTestCase.randomTimeValue;

/**
 * Builds random search requests.
 */
public class RandomSearchRequestGenerator {
    private RandomSearchRequestGenerator() {}

    /**
     * Build a random search request.
     *
     * @param randomSearchSourceBuilder builds a random {@link SearchSourceBuilder}. You can use
     *        {@link #randomSearchSourceBuilder(Supplier, Supplier, Supplier, Supplier, Supplier)}.
     */
    public static SearchRequest randomSearchRequest(Supplier<SearchSourceBuilder> randomSearchSourceBuilder) {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.allowPartialSearchResults(true);
        if (randomBoolean()) {
            searchRequest.setCcsMinimizeRoundtrips(randomBoolean());
        }
        if (randomBoolean()) {
            searchRequest.indices(generateRandomStringArray(10, 10, false, false));
        }
        if (randomBoolean()) {
            searchRequest.indicesOptions(IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean()));
        }
        if (randomBoolean()) {
            searchRequest.types(generateRandomStringArray(10, 10, false, false));
        }
        if (randomBoolean()) {
            searchRequest.preference(randomAlphaOfLengthBetween(3, 10));
        }
        if (randomBoolean()) {
            searchRequest.requestCache(randomBoolean());
        }
        if (randomBoolean()) {
            searchRequest.routing(randomAlphaOfLengthBetween(3, 10));
        }
        if (randomBoolean()) {
            searchRequest.scroll(randomPositiveTimeValue());
        }
        if (randomBoolean()) {
            searchRequest.searchType(randomFrom(SearchType.DFS_QUERY_THEN_FETCH, SearchType.QUERY_THEN_FETCH));
        }
        if (randomBoolean()) {
            searchRequest.source(randomSearchSourceBuilder.get());
        }
        return searchRequest;
    }

    public static SearchSourceBuilder randomSearchSourceBuilder(
            Supplier<HighlightBuilder> randomHighlightBuilder,
            Supplier<SuggestBuilder> randomSuggestBuilder,
            Supplier<RescorerBuilder<?>> randomRescoreBuilder,
            Supplier<List<SearchExtBuilder>> randomExtBuilders,
            Supplier<CollapseBuilder> randomCollapseBuilder) {
        SearchSourceBuilder builder = new SearchSourceBuilder();
        if (randomBoolean()) {
            builder.from(randomIntBetween(0, 10000));
        }
        if (randomBoolean()) {
            builder.size(randomIntBetween(0, 10000));
        }
        if (randomBoolean()) {
            builder.explain(randomBoolean());
        }
        if (randomBoolean()) {
            builder.version(randomBoolean());
        }
        if (randomBoolean()) {
            builder.seqNoAndPrimaryTerm(randomBoolean());
        }
        if (randomBoolean()) {
            builder.trackScores(randomBoolean());
        }
        if (randomBoolean()) {
            builder.minScore(randomFloat() * 1000);
        }
        if (randomBoolean()) {
            builder.timeout(TimeValue.parseTimeValue(randomTimeValue(), null, "timeout"));
        }
        if (randomBoolean()) {
            builder.terminateAfter(randomIntBetween(1, 100000));
        }
        if (randomBoolean()) {
            if (randomBoolean()) {
                builder.trackTotalHits(randomBoolean());
            } else {
                builder.trackTotalHitsUpTo(
                    randomIntBetween(SearchContext.TRACK_TOTAL_HITS_DISABLED, SearchContext.TRACK_TOTAL_HITS_ACCURATE)
                );
            }
        }

        switch(randomInt(2)) {
            case 0:
                builder.storedFields();
                break;
            case 1:
                builder.storedField("_none_");
                break;
            case 2:
                int fieldsSize = randomInt(25);
                List<String> fields = new ArrayList<>(fieldsSize);
                for (int i = 0; i < fieldsSize; i++) {
                    fields.add(randomAlphaOfLengthBetween(5, 50));
                }
                builder.storedFields(fields);
                break;
            default:
                throw new IllegalStateException();
        }

        if (randomBoolean()) {
            int numFields = randomInt(5);
            for (int i = 0; i < numFields; i++) {
                builder.fetchField(randomAlphaOfLengthBetween(5, 10));
            }
        }

        if (randomBoolean()) {
            int scriptFieldsSize = randomInt(25);
            for (int i = 0; i < scriptFieldsSize; i++) {
                if (randomBoolean()) {
                    builder.scriptField(randomAlphaOfLengthBetween(5, 50), mockScript("foo"), randomBoolean());
                } else {
                    builder.scriptField(randomAlphaOfLengthBetween(5, 50), mockScript("foo"));
                }
            }
        }
        if (randomBoolean()) {
            FetchSourceContext fetchSourceContext;
            int branch = randomInt(5);
            String[] includes = new String[randomIntBetween(0, 20)];
            for (int i = 0; i < includes.length; i++) {
                includes[i] = randomAlphaOfLengthBetween(5, 20);
            }
            String[] excludes = new String[randomIntBetween(0, 20)];
            for (int i = 0; i < excludes.length; i++) {
                excludes[i] = randomAlphaOfLengthBetween(5, 20);
            }
            switch (branch) {
                case 0:
                    fetchSourceContext = new FetchSourceContext(randomBoolean());
                    break;
                case 1:
                    fetchSourceContext = new FetchSourceContext(true, includes, excludes);
                    break;
                case 2:
                    fetchSourceContext = new FetchSourceContext(true, new String[]{randomAlphaOfLengthBetween(5, 20)},
                        new String[]{randomAlphaOfLengthBetween(5, 20)});
                    break;
                case 3:
                    fetchSourceContext = new FetchSourceContext(true, includes, excludes);
                    break;
                case 4:
                    fetchSourceContext = new FetchSourceContext(true, includes, null);
                    break;
                case 5:
                    fetchSourceContext = new FetchSourceContext(true, new String[] {randomAlphaOfLengthBetween(5, 20)}, null);
                    break;
                default:
                    throw new IllegalStateException();
            }
            builder.fetchSource(fetchSourceContext);
        }
        if (randomBoolean()) {
            int size = randomIntBetween(0, 20);
            List<String> statsGroups = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                statsGroups.add(randomAlphaOfLengthBetween(5, 20));
            }
            builder.stats(statsGroups);
        }
        if (randomBoolean()) {
            int indexBoostSize = randomIntBetween(1, 10);
            for (int i = 0; i < indexBoostSize; i++) {
                builder.indexBoost(randomAlphaOfLengthBetween(5, 20), randomFloat() * 10);
            }
        }
        if (randomBoolean()) {
            builder.query(QueryBuilders.termQuery(randomAlphaOfLengthBetween(5, 20), randomAlphaOfLengthBetween(5, 20)));
        }
        if (randomBoolean()) {
            builder.postFilter(QueryBuilders.termQuery(randomAlphaOfLengthBetween(5, 20), randomAlphaOfLengthBetween(5, 20)));
        }
        if (randomBoolean()) {
            int numSorts = randomIntBetween(1, 5);
            for (int i = 0; i < numSorts; i++) {
                int branch = randomInt(5);
                switch (branch) {
                    case 0:
                        builder.sort(SortBuilders.fieldSort(randomAlphaOfLengthBetween(5, 20)).order(randomFrom(SortOrder.values())));
                        break;
                    case 1:
                        builder.sort(SortBuilders.geoDistanceSort(randomAlphaOfLengthBetween(5, 20),
                                AbstractQueryTestCase.randomGeohash(1, 12)).order(randomFrom(SortOrder.values())));
                        break;
                    case 2:
                        builder.sort(SortBuilders.scoreSort().order(randomFrom(SortOrder.values())));
                        break;
                    case 3:
                        builder.sort(SortBuilders
                                .scriptSort(
                                        new Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG, "foo", emptyMap()),
                                        ScriptSortBuilder.ScriptSortType.NUMBER)
                                .order(randomFrom(SortOrder.values())));
                        break;
                    case 4:
                        builder.sort(randomAlphaOfLengthBetween(5, 20));
                        break;
                    case 5:
                        builder.sort(randomAlphaOfLengthBetween(5, 20), randomFrom(SortOrder.values()));
                        break;
                }
            }
        }

        if (randomBoolean()) {
            int numSearchFrom = randomIntBetween(1, 5);
            try {
                // We build a json version of the search_from first in order to
                // ensure that every number type remain the same before/after xcontent (de)serialization.
                // This is not a problem because the final type of each field value is extracted from associated sort field.
                // This little trick ensure that equals and hashcode are the same when using the xcontent serialization.
                XContentBuilder jsonBuilder = XContentFactory.jsonBuilder();
                jsonBuilder.startObject();
                jsonBuilder.startArray("search_from");
                for (int i = 0; i < numSearchFrom; i++) {
                    int branch = randomInt(8);
                    switch (branch) {
                        case 0:
                            jsonBuilder.value(randomInt());
                            break;
                        case 1:
                            jsonBuilder.value(randomFloat());
                            break;
                        case 2:
                            jsonBuilder.value(randomLong());
                            break;
                        case 3:
                            jsonBuilder.value(randomDouble());
                            break;
                        case 4:
                            jsonBuilder.value(randomAlphaOfLengthBetween(5, 20));
                            break;
                        case 5:
                            jsonBuilder.value(randomBoolean());
                            break;
                        case 6:
                            jsonBuilder.value(randomByte());
                            break;
                        case 7:
                            jsonBuilder.value(randomShort());
                            break;
                        case 8:
                            jsonBuilder.value(new Text(randomAlphaOfLengthBetween(5, 20)));
                            break;
                    }
                }
                jsonBuilder.endArray();
                jsonBuilder.endObject();
                XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                    .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                        BytesReference.bytes(jsonBuilder).streamInput());
                parser.nextToken();
                parser.nextToken();
                parser.nextToken();
                builder.searchAfter(SearchAfterBuilder.fromXContent(parser).getSortValues());
            } catch (IOException e) {
                throw new RuntimeException("Error building search_from", e);
            }
        }
        if (randomBoolean()) {
            builder.highlighter(randomHighlightBuilder.get());
        }
        if (randomBoolean()) {
            builder.suggest(randomSuggestBuilder.get());
        }
        if (randomBoolean()) {
            int numRescores = randomIntBetween(1, 5);
            for (int i = 0; i < numRescores; i++) {
                builder.addRescorer(randomRescoreBuilder.get());
            }
        }
        if (randomBoolean()) {
            builder.aggregation(AggregationBuilders.avg(randomAlphaOfLengthBetween(5, 20)).field("foo"));
        }
        if (randomBoolean()) {
            builder.ext(randomExtBuilders.get());
        }
        if (randomBoolean()) {
            String field = randomBoolean() ? null : randomAlphaOfLengthBetween(5, 20);
            int max = between(2, 1000);
            int id = randomInt(max-1);
            if (field == null) {
                builder.slice(new SliceBuilder(id, max));
            } else {
                builder.slice(new SliceBuilder(field, id, max));
            }
        }
        if (randomBoolean()) {
            builder.collapse(randomCollapseBuilder.get());
        }
        if (randomBoolean()) {
            PointInTimeBuilder pit = new PointInTimeBuilder(randomAlphaOfLengthBetween(3, 10));
            if (randomBoolean()) {
                pit.setKeepAlive(TimeValue.timeValueMinutes(randomIntBetween(1, 60)));
            }
            builder.pointInTimeBuilder(pit);
        }
        return builder;
    }
}

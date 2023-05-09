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

package org.havenask.search.suggest;

import org.apache.lucene.util.CharsRefBuilder;
import org.havenask.HavenaskException;
import org.havenask.search.internal.SearchContext;
import org.havenask.search.suggest.Suggest.Suggestion;
import org.havenask.search.suggest.Suggest.Suggestion.Entry;
import org.havenask.search.suggest.Suggest.Suggestion.Entry.Option;
import org.havenask.search.suggest.SuggestionSearchContext.SuggestionContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Suggest phase of a search request, used to collect suggestions
 */
public class SuggestPhase {

    public void execute(SearchContext context) {
        final SuggestionSearchContext suggest = context.suggest();
        if (suggest == null) {
            return;
        }
        try {
            CharsRefBuilder spare = new CharsRefBuilder();
            final List<Suggestion<? extends Entry<? extends Option>>> suggestions = new ArrayList<>(suggest.suggestions().size());

            for (Map.Entry<String, SuggestionSearchContext.SuggestionContext> entry : suggest.suggestions().entrySet()) {
                SuggestionSearchContext.SuggestionContext suggestion = entry.getValue();
                Suggester<SuggestionContext> suggester = suggestion.getSuggester();
                Suggestion<? extends Entry<? extends Option>> result =
                    suggester.execute(entry.getKey(), suggestion, context.searcher(), spare);
                if (result != null) {
                    assert entry.getKey().equals(result.name);
                    suggestions.add(result);
                }
            }
            context.queryResult().suggest(new Suggest(suggestions));
        } catch (IOException e) {
            throw new HavenaskException("I/O exception during suggest phase", e);
        }
    }

}


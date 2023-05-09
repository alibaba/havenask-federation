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

package org.havenask.search.suggest.term;

import org.havenask.LegacyESVersion;
import org.havenask.HavenaskException;
import org.havenask.common.ParseField;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.io.stream.StreamOutput;
import org.havenask.common.text.Text;
import org.havenask.common.xcontent.ConstructingObjectParser;
import org.havenask.common.xcontent.ObjectParser;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.common.xcontent.XContentParser;
import org.havenask.search.suggest.SortBy;
import org.havenask.search.suggest.Suggest;
import org.havenask.search.suggest.Suggest.Suggestion;
import org.havenask.search.suggest.Suggest.Suggestion.Entry.Option;

import java.io.IOException;
import java.util.Comparator;
import java.util.Objects;

import static org.havenask.common.xcontent.ConstructingObjectParser.constructorArg;

/**
 * The suggestion responses corresponding with the suggestions in the request.
 */
public class TermSuggestion extends Suggestion<TermSuggestion.Entry> {

    @Deprecated
    public static final int TYPE = 1;

    public static final Comparator<Suggestion.Entry.Option> SCORE = new Score();
    public static final Comparator<Suggestion.Entry.Option> FREQUENCY = new Frequency();

    private SortBy sort;

    public TermSuggestion(String name, int size, SortBy sort) {
        super(name, size);
        this.sort = sort;
    }

    public TermSuggestion(StreamInput in) throws IOException {
        super(in);
        if (in.getVersion().onOrAfter(LegacyESVersion.V_7_0_0)) {
            sort = SortBy.readFromStream(in);
        }
    }

    // Same behaviour as comparators in suggest module, but for SuggestedWord
    // Highest score first, then highest freq first, then lowest term first
    public static class Score implements Comparator<Suggestion.Entry.Option> {

        @Override
        public int compare(Suggestion.Entry.Option first, Suggestion.Entry.Option second) {
            // first criteria: the distance
            int cmp = Float.compare(second.getScore(), first.getScore());
            if (cmp != 0) {
                return cmp;
            }
            return FREQUENCY.compare(first, second);
        }
    }

    // Same behaviour as comparators in suggest module, but for SuggestedWord
    // Highest freq first, then highest score first, then lowest term first
    public static class Frequency implements Comparator<Suggestion.Entry.Option> {

        @Override
        public int compare(Suggestion.Entry.Option first, Suggestion.Entry.Option second) {

            // first criteria: the popularity
            int cmp = ((TermSuggestion.Entry.Option) second).getFreq() - ((TermSuggestion.Entry.Option) first).getFreq();
            if (cmp != 0) {
                return cmp;
            }

            // second criteria (if first criteria is equal): the distance
            cmp = Float.compare(second.getScore(), first.getScore());
            if (cmp != 0) {
                return cmp;
            }

            // third criteria: term text
            return first.getText().compareTo(second.getText());
        }
    }

    @Override
    public int getWriteableType() {
        return TYPE;
    }

    public void setSort(SortBy sort) {
        this.sort = sort;
    }

    public SortBy getSort() {
        return sort;
    }

    @Override
    protected Comparator<Option> sortComparator() {
        switch (sort) {
        case SCORE:
            return SCORE;
        case FREQUENCY:
            return FREQUENCY;
        default:
            throw new HavenaskException("Could not resolve comparator for sort key: [" + sort + "]");
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);

        if (out.getVersion().onOrAfter(LegacyESVersion.V_7_0_0)) {
            sort.writeTo(out);
        }
    }

    @Override
    public String getWriteableName() {
        return TermSuggestionBuilder.SUGGESTION_NAME;
    }

    public static TermSuggestion fromXContent(XContentParser parser, String name) throws IOException {
        // the "size" parameter and the SortBy for TermSuggestion cannot be parsed from the response, use default values
        TermSuggestion suggestion = new TermSuggestion(name, -1, SortBy.SCORE);
        parseEntries(parser, suggestion, TermSuggestion.Entry::fromXContent);
        return suggestion;
    }

    @Override
    protected Entry newEntry(StreamInput in) throws IOException {
        return new Entry(in);
    }

    @Override
    public boolean equals(Object other) {
        return super.equals(other)
            && Objects.equals(sort, ((TermSuggestion) other).sort);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), sort);
    }

    /**
     * Represents a part from the suggest text with suggested options.
     */
    public static class Entry extends Suggest.Suggestion.Entry<TermSuggestion.Entry.Option> {

        public Entry(Text text, int offset, int length) {
            super(text, offset, length);
        }

        private Entry() {}

        public Entry(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        protected Option newOption(StreamInput in) throws IOException {
            return new Option(in);
        }

        private static final ObjectParser<Entry, Void> PARSER = new ObjectParser<>("TermSuggestionEntryParser", true, Entry::new);
        static {
            declareCommonFields(PARSER);
            /*
             * The use of a lambda expression instead of the method reference Entry::addOptions is a workaround for a JDK 14 compiler bug.
             * The bug is: https://bugs.java.com/bugdatabase/view_bug.do?bug_id=JDK-8242214
             */
            PARSER.declareObjectArray((e, o) -> e.addOptions(o), (p, c) -> Option.fromXContent(p), new ParseField(OPTIONS));
        }

        public static Entry fromXContent(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        /**
         * Contains the suggested text with its document frequency and score.
         */
        public static class Option extends Suggest.Suggestion.Entry.Option {

            public static final ParseField FREQ = new ParseField("freq");

            private int freq;

            public Option(Text text, int freq, float score) {
                super(text, score);
                this.freq = freq;
            }

            public Option(StreamInput in) throws IOException {
                super(in);
                freq = in.readVInt();
            }

            @Override
            protected void mergeInto(Suggestion.Entry.Option otherOption) {
                super.mergeInto(otherOption);
                freq += ((Option) otherOption).freq;
            }

            public void setFreq(int freq) {
                this.freq = freq;
            }

            /**
             * @return How often this suggested text appears in the index.
             */
            public int getFreq() {
                return freq;
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                super.writeTo(out);
                out.writeVInt(freq);
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                builder = super.toXContent(builder, params);
                builder.field(FREQ.getPreferredName(), freq);
                return builder;
            }

            private static final ConstructingObjectParser<Option, Void> PARSER = new ConstructingObjectParser<>(
                    "TermSuggestionOptionParser", true,
                    args -> {
                        Text text = new Text((String) args[0]);
                        int freq = (Integer) args[1];
                        float score = (Float) args[2];
                        return new Option(text, freq, score);
                    });

            static {
                PARSER.declareString(constructorArg(), Suggestion.Entry.Option.TEXT);
                PARSER.declareInt(constructorArg(), FREQ);
                PARSER.declareFloat(constructorArg(), Suggestion.Entry.Option.SCORE);
            }

            public static Option fromXContent(XContentParser parser) {
                return PARSER.apply(parser, null);
            }
        }
    }
}

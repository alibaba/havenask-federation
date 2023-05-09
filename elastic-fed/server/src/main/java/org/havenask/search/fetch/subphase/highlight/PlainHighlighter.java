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

package org.havenask.search.fetch.subphase.highlight;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.search.highlight.Encoder;
import org.apache.lucene.search.highlight.Formatter;
import org.apache.lucene.search.highlight.Fragmenter;
import org.apache.lucene.search.highlight.InvalidTokenOffsetsException;
import org.apache.lucene.search.highlight.NullFragmenter;
import org.apache.lucene.search.highlight.QueryScorer;
import org.apache.lucene.search.highlight.SimpleFragmenter;
import org.apache.lucene.search.highlight.SimpleHTMLFormatter;
import org.apache.lucene.search.highlight.SimpleSpanFragmenter;
import org.apache.lucene.search.highlight.TextFragment;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.CollectionUtil;
import org.havenask.common.text.Text;
import org.havenask.index.IndexSettings;
import org.havenask.index.mapper.KeywordFieldMapper;
import org.havenask.index.mapper.MappedFieldType;
import org.havenask.search.fetch.FetchContext;
import org.havenask.search.fetch.FetchSubPhase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.havenask.search.fetch.subphase.highlight.UnifiedHighlighter.convertFieldValue;

public class PlainHighlighter implements Highlighter {
    private static final String CACHE_KEY = "highlight-plain";

    @Override
    public HighlightField highlight(FieldHighlightContext fieldContext) throws IOException {
        SearchHighlightContext.Field field = fieldContext.field;
        FetchContext context = fieldContext.context;
        FetchSubPhase.HitContext hitContext = fieldContext.hitContext;
        MappedFieldType fieldType = fieldContext.fieldType;

        Encoder encoder = field.fieldOptions().encoder().equals("html") ? HighlightUtils.Encoders.HTML : HighlightUtils.Encoders.DEFAULT;

        if (!fieldContext.cache.containsKey(CACHE_KEY)) {
            fieldContext.cache.put(CACHE_KEY, new HashMap<>());
        }
        @SuppressWarnings("unchecked")
        Map<MappedFieldType, org.apache.lucene.search.highlight.Highlighter> cache =
            (Map<MappedFieldType, org.apache.lucene.search.highlight.Highlighter>) fieldContext.cache.get(CACHE_KEY);

        org.apache.lucene.search.highlight.Highlighter entry = cache.get(fieldType);
        if (entry == null) {
            QueryScorer queryScorer = new CustomQueryScorer(fieldContext.query,
                    field.fieldOptions().requireFieldMatch() ? fieldType.name() : null);
            queryScorer.setExpandMultiTermQuery(true);
            Fragmenter fragmenter;
            if (field.fieldOptions().numberOfFragments() == 0) {
                fragmenter = new NullFragmenter();
            } else if (field.fieldOptions().fragmenter() == null) {
                fragmenter = new SimpleSpanFragmenter(queryScorer, field.fieldOptions().fragmentCharSize());
            } else if ("simple".equals(field.fieldOptions().fragmenter())) {
                fragmenter = new SimpleFragmenter(field.fieldOptions().fragmentCharSize());
            } else if ("span".equals(field.fieldOptions().fragmenter())) {
                fragmenter = new SimpleSpanFragmenter(queryScorer, field.fieldOptions().fragmentCharSize());
            } else {
                throw new IllegalArgumentException("unknown fragmenter option [" + field.fieldOptions().fragmenter()
                        + "] for the field [" + fieldContext.fieldName + "]");
            }
            Formatter formatter = new SimpleHTMLFormatter(field.fieldOptions().preTags()[0], field.fieldOptions().postTags()[0]);

            entry = new org.apache.lucene.search.highlight.Highlighter(formatter, encoder, queryScorer);
            entry.setTextFragmenter(fragmenter);
            // always highlight across all data
            entry.setMaxDocCharsToAnalyze(Integer.MAX_VALUE);

            cache.put(fieldType, entry);
        }

        // a HACK to make highlighter do highlighting, even though its using the single frag list builder
        int numberOfFragments = field.fieldOptions().numberOfFragments() == 0 ? 1 : field.fieldOptions().numberOfFragments();
        ArrayList<TextFragment> fragsList = new ArrayList<>();
        List<Object> textsToHighlight;
        Analyzer analyzer = context.mapperService().documentMapper(hitContext.hit().getType()).mappers().indexAnalyzer();
        Integer keywordIgnoreAbove = null;
        if (fieldType instanceof KeywordFieldMapper.KeywordFieldType) {
            KeywordFieldMapper mapper = (KeywordFieldMapper) context.mapperService().documentMapper()
                .mappers().getMapper(fieldContext.fieldName);
            keywordIgnoreAbove = mapper.ignoreAbove();
        };
        final int maxAnalyzedOffset = context.getIndexSettings().getHighlightMaxAnalyzedOffset();

        textsToHighlight = HighlightUtils.loadFieldValues(fieldType, hitContext, fieldContext.forceSource);

        for (Object textToHighlight : textsToHighlight) {
            String text = convertFieldValue(fieldType, textToHighlight);
            int textLength = text.length();
            if (keywordIgnoreAbove != null && textLength > keywordIgnoreAbove) {
                continue; // skip highlighting keyword terms that were ignored during indexing
            }
            if (textLength > maxAnalyzedOffset) {
                throw new IllegalArgumentException(
                    "The length of [" + fieldContext.fieldName + "] field of [" + hitContext.hit().getId() +
                        "] doc of [" + context.getIndexName() + "] index " +
                        "has exceeded [" + maxAnalyzedOffset + "] - maximum allowed to be analyzed for highlighting. " +
                        "This maximum can be set by changing the [" + IndexSettings.MAX_ANALYZED_OFFSET_SETTING.getKey() +
                        "] index level setting. " + "For large texts, indexing with offsets or term vectors, and highlighting " +
                        "with unified or fvh highlighter is recommended!");
            }

            try (TokenStream tokenStream = analyzer.tokenStream(fieldType.name(), text)) {
                if (!tokenStream.hasAttribute(CharTermAttribute.class) || !tokenStream.hasAttribute(OffsetAttribute.class)) {
                    // can't perform highlighting if the stream has no terms (binary token stream) or no offsets
                    continue;
                }
                TextFragment[] bestTextFragments = entry.getBestTextFragments(tokenStream, text, false, numberOfFragments);
                for (TextFragment bestTextFragment : bestTextFragments) {
                    if (bestTextFragment != null && bestTextFragment.getScore() > 0) {
                        fragsList.add(bestTextFragment);
                    }
                }
            } catch (BytesRefHash.MaxBytesLengthExceededException e) {
                // this can happen if for example a field is not_analyzed and ignore_above option is set.
                // the field will be ignored when indexing but the huge term is still in the source and
                // the plain highlighter will parse the source and try to analyze it.
                // ignore and continue to the next value
            } catch (InvalidTokenOffsetsException e) {
                throw new IllegalArgumentException(e);
            }
        }

        if (field.fieldOptions().scoreOrdered()) {
            CollectionUtil.introSort(fragsList, (o1, o2) -> Math.round(o2.getScore() - o1.getScore()));
        }
        String[] fragments;
        // number_of_fragments is set to 0 but we have a multivalued field
        if (field.fieldOptions().numberOfFragments() == 0 && textsToHighlight.size() > 1 && fragsList.size() > 0) {
            fragments = new String[fragsList.size()];
            for (int i = 0; i < fragsList.size(); i++) {
                fragments[i] = fragsList.get(i).toString();
            }
        } else {
            // refine numberOfFragments if needed
            numberOfFragments = Math.min(fragsList.size(), numberOfFragments);
            fragments = new String[numberOfFragments];
            for (int i = 0; i < fragments.length; i++) {
                fragments[i] = fragsList.get(i).toString();
            }
        }

        if (fragments.length > 0) {
            return new HighlightField(fieldContext.fieldName, Text.convertFromStringArray(fragments));
        }

        int noMatchSize = fieldContext.field.fieldOptions().noMatchSize();
        if (noMatchSize > 0 && textsToHighlight.size() > 0) {
            // Pull an excerpt from the beginning of the string but make sure to split the string on a term boundary.
            String fieldContents = textsToHighlight.get(0).toString();
            int end = findGoodEndForNoHighlightExcerpt(noMatchSize, analyzer, fieldType.name(), fieldContents);
            if (end > 0) {
                return new HighlightField(fieldContext.fieldName, new Text[] { new Text(fieldContents.substring(0, end)) });
            }
        }
        return null;
    }

    @Override
    public boolean canHighlight(MappedFieldType fieldType) {
        return true;
    }

    private static int findGoodEndForNoHighlightExcerpt(int noMatchSize, Analyzer analyzer, String fieldName, String contents)
            throws IOException {
        try (TokenStream tokenStream = analyzer.tokenStream(fieldName, contents)) {
            if (!tokenStream.hasAttribute(OffsetAttribute.class)) {
                // Can't split on term boundaries without offsets
                return -1;
            }
            int end = -1;
            tokenStream.reset();
            while (tokenStream.incrementToken()) {
                OffsetAttribute attr = tokenStream.getAttribute(OffsetAttribute.class);
                if (attr.endOffset() >= noMatchSize) {
                    // Jump to the end of this token if it wouldn't put us past the boundary
                    if (attr.endOffset() == noMatchSize) {
                        end = noMatchSize;
                    }
                    return end;
                }
                end = attr.endOffset();
            }
            tokenStream.end();
            // We've exhausted the token stream so we should just highlight everything.
            return end;
        }
    }
}

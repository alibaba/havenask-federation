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

package org.havenask.engine.search.dsl.expression;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.havenask.engine.search.dsl.expression.aggregation.AvgExpression;
import org.havenask.engine.search.dsl.expression.aggregation.BucketExpression;
import org.havenask.engine.search.dsl.expression.aggregation.CountExpression;
import org.havenask.engine.search.dsl.expression.aggregation.GroupByExpression;
import org.havenask.engine.search.dsl.expression.aggregation.MetricExpression;
import org.havenask.engine.search.dsl.expression.aggregation.SumExpression;
import org.havenask.engine.search.dsl.expression.aggregation.TermsExpression;
import org.havenask.engine.search.dsl.expression.query.BoolExpression;
import org.havenask.engine.search.dsl.expression.query.MatchAllExpression;
import org.havenask.engine.search.dsl.expression.query.TermExpression;
import org.havenask.index.query.BoolQueryBuilder;
import org.havenask.index.query.MatchAllQueryBuilder;
import org.havenask.index.query.QueryBuilder;
import org.havenask.index.query.TermQueryBuilder;
import org.havenask.search.aggregations.AggregationBuilder;
import org.havenask.search.aggregations.AggregatorFactories;
import org.havenask.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.havenask.search.aggregations.metrics.AvgAggregationBuilder;
import org.havenask.search.aggregations.metrics.SumAggregationBuilder;
import org.havenask.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.havenask.search.builder.SearchSourceBuilder;

public class SourceExpression extends Expression {
    private static final int DEFAULT_SEARCH_SIZE = 10;
    private final SearchSourceBuilder searchSourceBuilder;

    private final WhereExpression where;
    private final OrderByExpression orderBy;
    private QuerySQLExpression querySQLExpression;
    private List<AggregationSQLExpression> aggregationSQLExpressions = new ArrayList<>();
    private final int size;
    private final int from;

    public SourceExpression(SearchSourceBuilder searchSourceBuilder) {
        this.searchSourceBuilder = searchSourceBuilder;
        this.where = new WhereExpression(visitQuery(searchSourceBuilder.query()));
        this.orderBy = new OrderByExpression(searchSourceBuilder.sorts());
        this.size = searchSourceBuilder.size() >= 0 ? searchSourceBuilder.size() : DEFAULT_SEARCH_SIZE;
        this.from = searchSourceBuilder.from() >= 0 ? searchSourceBuilder.from() : 0;
    }

    public int size() {
        return size;
    }

    public int from() {
        return from;
    }

    public QuerySQLExpression getQuerySQLExpression(String index) {
        if (querySQLExpression == null) {
            querySQLExpression = new QuerySQLExpression(List.of("_id"), index, where, orderBy, size, from);
        }

        return querySQLExpression;
    }

    public List<AggregationSQLExpression> getAggregationSQLExpressions(String index) {
        if (aggregationSQLExpressions.size() > 0) {
            return aggregationSQLExpressions;
        }

        AggregatorFactories.Builder aggBuilder = searchSourceBuilder.aggregations();
        if (aggBuilder == null) {
            return aggregationSQLExpressions;
        }

        if (aggBuilder.getPipelineAggregatorFactories().size() > 0) {
            throw new IllegalArgumentException("Pipeline aggregation is not supported");
        }

        List<MetricExpression> metrics = new ArrayList<>();
        for (AggregationBuilder aggregationBuilder : aggBuilder.getAggregatorFactories()) {
            if (aggregationBuilder instanceof ValuesSourceAggregationBuilder.LeafOnly) {
                metrics.add(visitMetricExpression(aggregationBuilder));
            } else {
                List<GroupByExpression> groupByExpressions = visitAggregation(aggregationBuilder);
                for (GroupByExpression groupByExpression : groupByExpressions) {
                    List<BucketExpression> groupBy = new ArrayList<>();
                    AtomicInteger limit = new AtomicInteger(1);
                    groupByExpression.getAggregationBuilders().forEach(agg -> {
                        if (agg instanceof TermsAggregationBuilder) {
                            groupBy.add(new TermsExpression(((TermsAggregationBuilder) agg)));
                            limit.updateAndGet(v -> v * ((TermsAggregationBuilder) agg).size());
                        } else {
                            throw new IllegalArgumentException("Unsupported aggregation type: " + agg.getClass().getName());
                        }
                    });

                    List<MetricExpression> inMetrics = new ArrayList<>();
                    groupByExpression.getLastAggregationBuilder().getSubAggregations().forEach(subAgg -> {
                        if (false == subAgg instanceof ValuesSourceAggregationBuilder.LeafOnly) {
                            return;
                        }

                        inMetrics.add(visitMetricExpression(subAgg));
                    });
                    if (inMetrics.size() == 0) {
                        inMetrics.add(new CountExpression());
                    }

                    aggregationSQLExpressions.add(new AggregationSQLExpression(groupBy, inMetrics, index, where, limit.get()));
                }
            }
        }

        if (metrics.size() > 0) {
            aggregationSQLExpressions.add(new AggregationSQLExpression(List.of(), metrics, index, where, 1));
        }

        return aggregationSQLExpressions;
    }

    private MetricExpression visitMetricExpression(AggregationBuilder aggregationBuilder) {
        if (aggregationBuilder instanceof SumAggregationBuilder) {
            return new SumExpression((SumAggregationBuilder) aggregationBuilder);
        } else if (aggregationBuilder instanceof AvgAggregationBuilder) {
            return new AvgExpression((AvgAggregationBuilder) aggregationBuilder);
        } else {
            throw new IllegalArgumentException("Unsupported aggregation type: " + aggregationBuilder.getClass().getName());
        }
    }

    public static List<GroupByExpression> visitAggregation(AggregationBuilder aggregationBuilder) {
        if (aggregationBuilder instanceof ValuesSourceAggregationBuilder.LeafOnly) {
            return List.of();
        } else {
            List<GroupByExpression> groupByExpressions = new ArrayList<>();
            groupByExpressions.add(new GroupByExpression(List.of(aggregationBuilder)));
            for (AggregationBuilder sub : aggregationBuilder.getSubAggregations()) {
                if (false == sub instanceof ValuesSourceAggregationBuilder.LeafOnly) {
                    List<GroupByExpression> subGroupBy = visitAggregation(sub);
                    for (GroupByExpression groupByExpression : subGroupBy) {
                        List<AggregationBuilder> aggregations = new ArrayList<>();
                        aggregations.add(aggregationBuilder);
                        aggregations.addAll(groupByExpression.getAggregationBuilders());
                        groupByExpressions.add(new GroupByExpression(aggregations));
                    }
                }
            }
            return groupByExpressions;
        }
    }

    public static Expression visitQuery(QueryBuilder query) {
        if (query == null) {
            return new MatchAllExpression();
        }

        if (query instanceof BoolQueryBuilder) {
            return visitBoolQuery((BoolQueryBuilder) query);
        } else if (query instanceof TermQueryBuilder) {
            return new TermExpression((TermQueryBuilder) query);
        } else if (query instanceof MatchAllQueryBuilder) {
            return new MatchAllExpression();
        } else {
            throw new IllegalArgumentException("Unsupported query type: " + query.getClass().getName());
        }
    }

    private static Expression visitBoolQuery(BoolQueryBuilder query) {
        List<Expression> must = query.must().stream().map(SourceExpression::visitQuery).collect(Collectors.toList());
        List<Expression> mustNot = query.mustNot().stream().map(SourceExpression::visitQuery).collect(Collectors.toList());
        List<Expression> should = query.should().stream().map(SourceExpression::visitQuery).collect(Collectors.toList());
        List<Expression> filter = query.filter().stream().map(SourceExpression::visitQuery).collect(Collectors.toList());
        return new BoolExpression(must, should, mustNot, filter, query.minimumShouldMatch());
    }

    @Override
    public String translate() {
        return "";
    }
}

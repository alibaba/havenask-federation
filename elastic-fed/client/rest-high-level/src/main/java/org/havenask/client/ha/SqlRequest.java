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

package org.havenask.client.ha;

import org.havenask.action.ActionRequest;
import org.havenask.action.ActionRequestValidationException;

public class SqlRequest extends ActionRequest {
    private final String sql;
    private String trace;
    private Long timeout;
    private Boolean searchInfo;
    private Boolean sqlPlan;
    private Boolean forbitMergeSearchInfo;
    private Boolean resultReadable;
    private Integer parallel;
    private String parallelTables;
    private Boolean lackResultEnable;
    private Boolean optimizerDebug;
    private Boolean sortLimitTogether;
    private Boolean forceLimit;
    private Boolean joinConditionCheck;
    private Boolean forceJoinHask;
    private Boolean planLevel;
    private Boolean cacheEnable;

    public SqlRequest(String sql) {
        this.sql = sql;
    }

    public String getSql() {
        return sql;
    }


    public String getTrace() {
        return trace;
    }

    public void setTrace(String trace) {
        this.trace = trace;
    }

    public Long getTimeout() {
        return timeout;
    }

    public void setTimeout(Long timeout) {
        this.timeout = timeout;
    }

    public Boolean getSearchInfo() {
        return searchInfo;
    }

    public void setSearchInfo(Boolean searchInfo) {
        this.searchInfo = searchInfo;
    }

    public Boolean getSqlPlan() {
        return sqlPlan;
    }

    public void setSqlPlan(Boolean sqlPlan) {
        this.sqlPlan = sqlPlan;
    }

    public Boolean getForbitMergeSearchInfo() {
        return forbitMergeSearchInfo;
    }

    public void setForbitMergeSearchInfo(Boolean forbitMergeSearchInfo) {
        this.forbitMergeSearchInfo = forbitMergeSearchInfo;
    }

    public Boolean getResultReadable() {
        return resultReadable;
    }

    public void setResultReadable(Boolean resultReadable) {
        this.resultReadable = resultReadable;
    }

    public Integer getParallel() {
        return parallel;
    }

    public void setParallel(Integer parallel) {
        this.parallel = parallel;
    }

    public String getParallelTables() {
        return parallelTables;
    }

    public void setParallelTables(String parallelTables) {
        this.parallelTables = parallelTables;
    }

    public Boolean getLackResultEnable() {
        return lackResultEnable;
    }

    public void setLackResultEnable(Boolean lackResultEnable) {
        this.lackResultEnable = lackResultEnable;
    }

    public Boolean getOptimizerDebug() {
        return optimizerDebug;
    }

    public void setOptimizerDebug(Boolean optimizerDebug) {
        this.optimizerDebug = optimizerDebug;
    }

    public Boolean getSortLimitTogether() {
        return sortLimitTogether;
    }

    public void setSortLimitTogether(Boolean sortLimitTogether) {
        this.sortLimitTogether = sortLimitTogether;
    }

    public Boolean getForceLimit() {
        return forceLimit;
    }

    public void setForceLimit(Boolean forceLimit) {
        this.forceLimit = forceLimit;
    }

    public Boolean getJoinConditionCheck() {
        return joinConditionCheck;
    }

    public void setJoinConditionCheck(Boolean joinConditionCheck) {
        this.joinConditionCheck = joinConditionCheck;
    }

    public Boolean getForceJoinHask() {
        return forceJoinHask;
    }

    public void setForceJoinHask(Boolean forceJoinHask) {
        this.forceJoinHask = forceJoinHask;
    }

    public Boolean getPlanLevel() {
        return planLevel;
    }

    public void setPlanLevel(Boolean planLevel) {
        this.planLevel = planLevel;
    }

    public Boolean getCacheEnable() {
        return cacheEnable;
    }

    public void setCacheEnable(Boolean cacheEnable) {
        this.cacheEnable = cacheEnable;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}

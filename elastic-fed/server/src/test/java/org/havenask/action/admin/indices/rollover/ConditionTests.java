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

package org.havenask.action.admin.indices.rollover;

import org.havenask.common.unit.ByteSizeUnit;
import org.havenask.common.unit.ByteSizeValue;
import org.havenask.common.unit.TimeValue;
import org.havenask.test.HavenaskTestCase;
import org.havenask.test.EqualsHashCodeTestUtils;
import org.havenask.action.admin.indices.rollover.Condition;
import org.havenask.action.admin.indices.rollover.MaxAgeCondition;
import org.havenask.action.admin.indices.rollover.MaxDocsCondition;
import org.havenask.action.admin.indices.rollover.MaxSizeCondition;

import static org.hamcrest.Matchers.equalTo;

public class ConditionTests extends HavenaskTestCase {

    public void testMaxAge() {
        final MaxAgeCondition maxAgeCondition = new MaxAgeCondition(TimeValue.timeValueHours(1));

        long indexCreatedMatch = System.currentTimeMillis() - TimeValue.timeValueMinutes(61).getMillis();
        Condition.Result evaluate = maxAgeCondition.evaluate(new Condition.Stats(0, indexCreatedMatch, randomByteSize()));
        assertThat(evaluate.condition, equalTo(maxAgeCondition));
        assertThat(evaluate.matched, equalTo(true));

        long indexCreatedNotMatch = System.currentTimeMillis() - TimeValue.timeValueMinutes(59).getMillis();
        evaluate = maxAgeCondition.evaluate(new Condition.Stats(0, indexCreatedNotMatch, randomByteSize()));
        assertThat(evaluate.condition, equalTo(maxAgeCondition));
        assertThat(evaluate.matched, equalTo(false));
    }

    public void testMaxDocs() {
        final MaxDocsCondition maxDocsCondition = new MaxDocsCondition(100L);

        long maxDocsMatch = randomIntBetween(100, 1000);
        Condition.Result evaluate = maxDocsCondition.evaluate(new Condition.Stats(maxDocsMatch, 0, randomByteSize()));
        assertThat(evaluate.condition, equalTo(maxDocsCondition));
        assertThat(evaluate.matched, equalTo(true));

        long maxDocsNotMatch = randomIntBetween(0, 99);
        evaluate = maxDocsCondition.evaluate(new Condition.Stats(0, maxDocsNotMatch, randomByteSize()));
        assertThat(evaluate.condition, equalTo(maxDocsCondition));
        assertThat(evaluate.matched, equalTo(false));
    }

    public void testMaxSize() {
        MaxSizeCondition maxSizeCondition = new MaxSizeCondition(new ByteSizeValue(randomIntBetween(10, 20), ByteSizeUnit.MB));

        Condition.Result result = maxSizeCondition.evaluate(new Condition.Stats(randomNonNegativeLong(), randomNonNegativeLong(),
            new ByteSizeValue(0, ByteSizeUnit.MB)));
        assertThat(result.matched, equalTo(false));

        result = maxSizeCondition.evaluate(new Condition.Stats(randomNonNegativeLong(), randomNonNegativeLong(),
            new ByteSizeValue(randomIntBetween(0, 9), ByteSizeUnit.MB)));
        assertThat(result.matched, equalTo(false));

        result = maxSizeCondition.evaluate(new Condition.Stats(randomNonNegativeLong(), randomNonNegativeLong(),
            new ByteSizeValue(randomIntBetween(20, 1000), ByteSizeUnit.MB)));
        assertThat(result.matched, equalTo(true));
    }

    public void testEqualsAndHashCode() {
        MaxDocsCondition maxDocsCondition = new MaxDocsCondition(randomLong());
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(maxDocsCondition, condition -> new MaxDocsCondition(condition.value),
                condition -> new MaxDocsCondition(randomLong()));

        MaxSizeCondition maxSizeCondition = new MaxSizeCondition(randomByteSize());
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(maxSizeCondition, condition -> new MaxSizeCondition(condition.value),
                condition -> new MaxSizeCondition(randomByteSize()));

        MaxAgeCondition maxAgeCondition = new MaxAgeCondition(new TimeValue(randomNonNegativeLong()));
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(maxAgeCondition, condition -> new MaxAgeCondition(condition.value),
                condition -> new MaxAgeCondition(new TimeValue(randomNonNegativeLong())));
    }

    private static ByteSizeValue randomByteSize() {
        return new ByteSizeValue(randomNonNegativeLong(), ByteSizeUnit.BYTES);
    }
}

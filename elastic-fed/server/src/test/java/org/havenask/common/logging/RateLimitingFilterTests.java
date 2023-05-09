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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.havenask.common.logging;

import org.apache.logging.log4j.message.Message;
import org.apache.logging.log4j.message.SimpleMessage;
import org.havenask.test.HavenaskTestCase;
import org.junit.After;
import org.junit.Before;

import static org.apache.logging.log4j.core.Filter.Result;
import static org.hamcrest.Matchers.equalTo;

public class RateLimitingFilterTests extends HavenaskTestCase {

    private RateLimitingFilter filter;

    @Before
    public void setup() {
        this.filter = new RateLimitingFilter();
        filter.start();
    }

    @After
    public void cleanup() {
        this.filter.stop();
    }

    /**
     * Check that messages are rate-limited by their key.
     */
    public void testMessagesAreRateLimitedByKey() {
        // Fill up the cache
        for (int i = 0; i < 128; i++) {
            Message message = new DeprecatedMessage("key " + i, "", "msg " + i);
            assertThat("Expected key" + i + " to be accepted", filter.filter(message), equalTo(Result.ACCEPT));
        }

        // Should be rate-limited because it's still in the cache
        Message message = new DeprecatedMessage("key 0", "", "msg " + 0);
        assertThat(filter.filter(message), equalTo(Result.DENY));

        // Filter a message with a previously unseen key, in order to evict key0 as it's the oldest
        message = new DeprecatedMessage("key 129", "", "msg " + 129);
        assertThat(filter.filter(message), equalTo(Result.ACCEPT));

        // Should be allowed because key0 was evicted from the cache
        message = new DeprecatedMessage("key 0", "", "msg " + 0);
        assertThat(filter.filter(message), equalTo(Result.ACCEPT));
    }

    /**
     * Check that messages are rate-limited by their x-opaque-id value
     */
    public void testMessagesAreRateLimitedByXOpaqueId() {
        // Fill up the cache
        for (int i = 0; i < 128; i++) {
            Message message = new DeprecatedMessage("", "id " + i, "msg " + i);
            assertThat("Expected key" + i + " to be accepted", filter.filter(message), equalTo(Result.ACCEPT));
        }

        // Should be rate-limited because it's still in the cache
        Message message = new DeprecatedMessage("", "id 0", "msg 0");
        assertThat(filter.filter(message), equalTo(Result.DENY));

        // Filter a message with a previously unseen key, in order to evict key0 as it's the oldest
        message = new DeprecatedMessage("", "id 129", "msg 129");
        assertThat(filter.filter(message), equalTo(Result.ACCEPT));

        // Should be allowed because key0 was evicted from the cache
        message = new DeprecatedMessage("", "id 0", "msg 0");
        assertThat(filter.filter(message), equalTo(Result.ACCEPT));
    }

    /**
     * Check that messages are rate-limited by their key and x-opaque-id value
     */
    public void testMessagesAreRateLimitedByKeyAndXOpaqueId() {
        // Fill up the cache
        for (int i = 0; i < 128; i++) {
            Message message = new DeprecatedMessage("key " + i, "opaque-id " + i, "msg " + i);
            assertThat("Expected key" + i + " to be accepted", filter.filter(message), equalTo(Result.ACCEPT));
        }

        // Should be rate-limited because it's still in the cache
        Message message = new DeprecatedMessage("key 0", "opaque-id 0", "msg 0");
        assertThat(filter.filter(message), equalTo(Result.DENY));

        // Filter a message with a previously unseen key, in order to evict key0 as it's the oldest
        message = new DeprecatedMessage("key 129", "opaque-id 129", "msg 129");
        assertThat(filter.filter(message), equalTo(Result.ACCEPT));

        // Should be allowed because key 0 was evicted from the cache
        message = new DeprecatedMessage("key 0", "opaque-id 0", "msg 0");
        assertThat(filter.filter(message), equalTo(Result.ACCEPT));
    }

    /**
     * Check that it is the combination of key and x-opaque-id that rate-limits messages, by varying each
     * independently and checking that a message is not filtered.
     */
    public void testVariationsInKeyAndXOpaqueId() {
        Message message = new DeprecatedMessage("key 0", "opaque-id 0", "msg 0");
        assertThat(filter.filter(message), equalTo(Result.ACCEPT));

        message = new DeprecatedMessage("key 0", "opaque-id 0", "msg 0");
        // Rejected because the "x-opaque-id" and "key" values are the same as above
        assertThat(filter.filter(message), equalTo(Result.DENY));

        message = new DeprecatedMessage("key 1", "opaque-id 0", "msg 0");
        // Accepted because the "key" value is different
        assertThat(filter.filter(message), equalTo(Result.ACCEPT));

        message = new DeprecatedMessage("key 0", "opaque-id 1", "msg 0");
        // Accepted because the "x-opaque-id" value is different
        assertThat(filter.filter(message), equalTo(Result.ACCEPT));
    }

    /**
     * Check that rate-limiting is not applied to messages if they are not an EsLogMessage.
     */
    public void testOnlyEsMessagesAreFiltered() {
        Message message = new SimpleMessage("a message");
        assertThat(filter.filter(message), equalTo(Result.NEUTRAL));
    }

    /**
     * Check that the filter can be reset, so that previously-seen keys are treated as new keys.
     */
    public void testFilterCanBeReset() {
        final Message message = new DeprecatedMessage("key", "", "msg");

        // First time, the message is a allowed
        assertThat(filter.filter(message), equalTo(Result.ACCEPT));

        // Second time, it is filtered out
        assertThat(filter.filter(message), equalTo(Result.DENY));

        filter.reset();

        // Third time, it is allowed again
        assertThat(filter.filter(message), equalTo(Result.ACCEPT));
    }
}

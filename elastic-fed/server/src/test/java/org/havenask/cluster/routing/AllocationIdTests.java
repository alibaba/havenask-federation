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

package org.havenask.cluster.routing;

import org.havenask.cluster.routing.AllocationId;
import org.havenask.cluster.routing.RecoverySource.ExistingStoreRecoverySource;
import org.havenask.common.bytes.BytesReference;
import org.havenask.common.xcontent.ToXContent;
import org.havenask.common.xcontent.XContentFactory;
import org.havenask.common.xcontent.json.JsonXContent;
import org.havenask.index.shard.ShardId;
import org.havenask.test.HavenaskTestCase;
import org.havenask.cluster.routing.ShardRouting;
import org.havenask.cluster.routing.UnassignedInfo;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class AllocationIdTests extends HavenaskTestCase {
    public void testShardToStarted() {
        logger.info("-- create unassigned shard");
        ShardRouting shard = ShardRouting.newUnassigned(new ShardId("test","_na_", 0), true,
            ExistingStoreRecoverySource.INSTANCE, new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null));
        assertThat(shard.allocationId(), nullValue());

        logger.info("-- initialize the shard");
        shard = shard.initialize("node1", null, -1);
        AllocationId allocationId = shard.allocationId();
        assertThat(allocationId, notNullValue());
        assertThat(allocationId.getId(), notNullValue());
        assertThat(allocationId.getRelocationId(), nullValue());

        logger.info("-- start the shard");
        shard = shard.moveToStarted();
        assertThat(shard.allocationId().getId(), equalTo(allocationId.getId()));
        allocationId = shard.allocationId();
        assertThat(allocationId.getId(), notNullValue());
        assertThat(allocationId.getRelocationId(), nullValue());
    }

    public void testSuccessfulRelocation() {
        logger.info("-- build started shard");
        ShardRouting shard = ShardRouting.newUnassigned(new ShardId("test","_na_", 0), true,
            ExistingStoreRecoverySource.INSTANCE, new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null));
        shard = shard.initialize("node1", null, -1);
        shard = shard.moveToStarted();

        AllocationId allocationId = shard.allocationId();
        logger.info("-- relocate the shard");
        shard = shard.relocate("node2", -1);
        assertThat(shard.allocationId(), not(equalTo(allocationId)));
        assertThat(shard.allocationId().getId(), equalTo(allocationId.getId()));
        assertThat(shard.allocationId().getRelocationId(), notNullValue());

        ShardRouting target = shard.getTargetRelocatingShard();
        assertThat(target.allocationId().getId(), equalTo(shard.allocationId().getRelocationId()));
        assertThat(target.allocationId().getRelocationId(), equalTo(shard.allocationId().getId()));

        logger.info("-- finalize the relocation");
        target = target.moveToStarted();
        assertThat(target.allocationId().getId(), equalTo(shard.allocationId().getRelocationId()));
        assertThat(target.allocationId().getRelocationId(), nullValue());
    }

    public void testCancelRelocation() {
        logger.info("-- build started shard");
        ShardRouting shard = ShardRouting.newUnassigned(new ShardId("test","_na_", 0), true,
            ExistingStoreRecoverySource.INSTANCE, new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null));
        shard = shard.initialize("node1", null, -1);
        shard = shard.moveToStarted();

        AllocationId allocationId = shard.allocationId();
        logger.info("-- relocate the shard");
        shard = shard.relocate("node2", -1);
        assertThat(shard.allocationId(), not(equalTo(allocationId)));
        assertThat(shard.allocationId().getId(), equalTo(allocationId.getId()));
        assertThat(shard.allocationId().getRelocationId(), notNullValue());
        allocationId = shard.allocationId();

        logger.info("-- cancel relocation");
        shard = shard.cancelRelocation();
        assertThat(shard.allocationId().getId(), equalTo(allocationId.getId()));
        assertThat(shard.allocationId().getRelocationId(), nullValue());
    }

    public void testMoveToUnassigned() {
        logger.info("-- build started shard");
        ShardRouting shard = ShardRouting.newUnassigned(new ShardId("test","_na_", 0), true,
            ExistingStoreRecoverySource.INSTANCE, new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null));
        shard = shard.initialize("node1", null, -1);
        shard = shard.moveToStarted();

        logger.info("-- move to unassigned");
        shard = shard.moveToUnassigned(new UnassignedInfo(UnassignedInfo.Reason.NODE_LEFT, null));
        assertThat(shard.allocationId(), nullValue());
    }

    public void testSerialization() throws IOException {
        AllocationId allocationId = AllocationId.newInitializing();
        if (randomBoolean()) {
            allocationId = AllocationId.newRelocation(allocationId);
        }
        BytesReference bytes = BytesReference.bytes(allocationId.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS));
        AllocationId parsedAllocationId = AllocationId.fromXContent(createParser(JsonXContent.jsonXContent, bytes));
        assertEquals(allocationId, parsedAllocationId);
    }

    public void testEquals() {
        AllocationId allocationId1 = AllocationId.newInitializing();
        AllocationId allocationId2 = AllocationId.newInitializing(allocationId1.getId());
        AllocationId allocationId3 = AllocationId.newInitializing("not a UUID");
        String s = "Some random other object";
        assertEquals(allocationId1, allocationId1);
        assertEquals(allocationId1, allocationId2);
        assertNotEquals(allocationId1, s);
        assertNotEquals(allocationId1, null);
        assertNotEquals(allocationId1, allocationId3);

        allocationId2 = AllocationId.newRelocation(allocationId1);
        assertNotEquals(allocationId1, allocationId2);
    }
}

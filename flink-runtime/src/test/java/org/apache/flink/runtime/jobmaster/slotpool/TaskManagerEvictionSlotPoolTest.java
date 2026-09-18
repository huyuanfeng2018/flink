/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.slots.ResourceRequirement;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.util.ResourceCounter;
import org.apache.flink.util.FlinkException;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Exercises the real resource ledger, not a mocked resource counter. */
class TaskManagerEvictionSlotPoolTest {
    private static final ResourceProfile PROFILE =
            ResourceProfile.newBuilder().setCpuCores(1).build();
    private final SimpleAckingTaskManagerGateway gateway = new SimpleAckingTaskManagerGateway();
    private final TaskManagerLocation oldLocation = new LocalTaskManagerLocation();
    private final TaskManagerLocation healthyLocation = new LocalTaskManagerLocation();
    private final List<Collection<ResourceRequirement>> declarations = new ArrayList<>();
    private final DefaultDeclarativeSlotPool pool =
            DefaultDeclarativeSlotPoolBuilder.builder()
                    .setSlotRequestMaxInterval(Duration.ZERO)
                    .setIdleSlotTimeout(Duration.ofMillis(1))
                    .setNotifyNewResourceRequirements(declarations::add)
                    .build();

    @Test
    void retainsReplacementCapacityWhileDefaultSchedulerRequirementsTemporarilyDisappear() {
        pool.increaseResourceRequirementsBy(count(2));
        final SlotOffer old = offer(oldLocation);
        final SlotOffer healthy = offer(healthyLocation);
        pool.reserveFreeSlot(old.getAllocationId(), PROFILE);
        pool.reserveFreeSlot(healthy.getAllocationId(), PROFILE);
        pool.markTaskManagersForEviction(Collections.singleton(oldLocation.getResourceID()));
        pool.beginTaskManagerEviction();
        assertDemand(3);
        assertThat(pool.hasSufficientResourcesForTaskManagerEviction()).isFalse();
        final SlotOffer replacement = offer(new LocalTaskManagerLocation());
        assertThat(pool.hasSufficientResourcesForTaskManagerEviction()).isTrue();

        pool.decreaseResourceRequirementsBy(pool.freeReservedSlot(old.getAllocationId(), null, 1));
        assertDemand(2);
        pool.decreaseResourceRequirementsBy(
                pool.freeReservedSlot(healthy.getAllocationId(), null, 1));
        assertDemand(2);
        pool.releaseIdleSlots(1000);
        assertThat(pool.getAllSlotsInformation()).hasSize(2);
        assertThat(pool.containsFreeSlot(replacement.getAllocationId())).isTrue();

        pool.increaseResourceRequirementsBy(count(2));
        pool.reserveFreeSlot(healthy.getAllocationId(), PROFILE);
        pool.reserveFreeSlot(replacement.getAllocationId(), PROFILE);
        pool.finishTaskManagerEviction();
        assertDemand(2);
        assertThat(pool.containsSlots(oldLocation.getResourceID())).isFalse();
    }

    @Test
    void keepsRunningSlotOnDuplicateOffersButRejectsNewAndIdleSlotsOnDrainingWorker() {
        pool.increaseResourceRequirementsBy(count(2));
        final SlotOffer running = offer(oldLocation);
        final SlotOffer idle = offer(oldLocation);
        final PhysicalSlot slot = pool.reserveFreeSlot(running.getAllocationId(), PROFILE);
        assertThat(
                        pool.markTaskManagersForEviction(
                                Collections.singleton(oldLocation.getResourceID())))
                .containsExactly(oldLocation.getResourceID());
        pool.beginTaskManagerEviction();
        final int notifications = declarations.size();
        assertThat(
                        pool.markTaskManagersForEviction(
                                Collections.singleton(oldLocation.getResourceID())))
                .isEmpty();
        assertThat(declarations).hasSize(notifications);
        assertThat(pool.containsFreeSlot(idle.getAllocationId())).isFalse();
        assertThat(slot.getAllocationId()).isEqualTo(running.getAllocationId());
        assertThat(pool.offerSlots(Collections.singleton(running), oldLocation, gateway, 1))
                .containsExactly(running);
        final SlotOffer newOffer = new SlotOffer(new AllocationID(), 3, PROFILE);
        assertThat(pool.offerSlots(Collections.singleton(newOffer), oldLocation, gateway, 1))
                .isEmpty();
        assertThat(pool.registerSlots(Collections.singleton(newOffer), oldLocation, gateway, 1))
                .isEmpty();
        assertThat(pool.getSlotsInformationForScheduling()).isEmpty();
        pool.finishTaskManagerEviction();
        assertThat(pool.isTaskManagerPendingEviction(oldLocation.getResourceID())).isTrue();
    }

    @Test
    void replacementLossReopensTheDeficitWithoutIncreasingDeclaredDemand() {
        pool.increaseResourceRequirementsBy(count(1));
        final SlotOffer old = offer(oldLocation);
        pool.reserveFreeSlot(old.getAllocationId(), PROFILE);
        pool.markTaskManagersForEviction(Collections.singleton(oldLocation.getResourceID()));
        pool.beginTaskManagerEviction();
        final SlotOffer replacement = offer(healthyLocation);
        assertThat(pool.hasSufficientResourcesForTaskManagerEviction()).isTrue();
        pool.releaseSlot(replacement.getAllocationId(), new FlinkException("replacement lost"));
        assertDemand(2);
        assertThat(pool.hasSufficientResourcesForTaskManagerEviction()).isFalse();
        assertThat(pool.calculateUnfulfilledResources()).isEqualTo(count(1));
    }

    @Test
    void unrelatedExcessSlotsStillExpireDuringEviction() {
        pool.increaseResourceRequirementsBy(count(1));
        final SlotOffer old = offer(oldLocation);
        pool.reserveFreeSlot(old.getAllocationId(), PROFILE);
        pool.markTaskManagersForEviction(Collections.singleton(oldLocation.getResourceID()));
        pool.beginTaskManagerEviction();
        offer(healthyLocation);
        final SlotOffer surplus = new SlotOffer(new AllocationID(), 0, PROFILE);
        pool.registerSlots(
                Collections.singleton(surplus), new LocalTaskManagerLocation(), gateway, 0);
        assertThat(pool.getAllSlotsInformation()).hasSize(3);
        pool.releaseIdleSlots(1000);
        assertThat(pool.getAllSlotsInformation()).hasSize(2);
        assertThat(pool.hasSufficientResourcesForTaskManagerEviction()).isTrue();
        assertThat(pool.containsSlots(oldLocation.getResourceID())).isTrue();
    }

    private SlotOffer offer(TaskManagerLocation location) {
        final SlotOffer offer =
                new SlotOffer(new AllocationID(), pool.getAllSlotsInformation().size(), PROFILE);
        assertThat(pool.offerSlots(Collections.singleton(offer), location, gateway, 0))
                .containsExactly(offer);
        return offer;
    }

    private static ResourceCounter count(int count) {
        return ResourceCounter.withResource(PROFILE, count);
    }

    private void assertDemand(int count) {
        assertThat(pool.getResourceRequirements())
                .containsExactly(ResourceRequirement.create(PROFILE, count));
    }
}

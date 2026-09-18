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

package org.apache.flink.runtime.resourcemanager;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.jobmaster.utils.TestingJobMasterGateway;
import org.apache.flink.runtime.jobmaster.utils.TestingJobMasterGatewayBuilder;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.util.concurrent.ManuallyTriggeredScheduledExecutor;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

class PendingTaskManagerEvictionsTest {
    @Test
    void retriesCoalescesAndReplaysToReconnectedJobMaster() {
        final ManuallyTriggeredScheduledExecutor executor =
                new ManuallyTriggeredScheduledExecutor();
        final List<Collection<ResourceID>> notifications = new ArrayList<>();
        final List<CompletableFuture<Acknowledge>> responses = new ArrayList<>();
        final TestingJobMasterGateway gateway = new TestingJobMasterGatewayBuilder().build();
        gateway.setNotifyTaskManagersPendingEvictionFunction(
                taskManagers -> {
                    notifications.add(new ArrayList<>(taskManagers));
                    CompletableFuture<Acknowledge> response = new CompletableFuture<>();
                    responses.add(response);
                    return response;
                });
        final ResourceID first = ResourceID.generate();
        final ResourceID second = ResourceID.generate();
        try (PendingTaskManagerEvictions tracker =
                new PendingTaskManagerEvictions(
                        ResourceManagerId.generate(), executor, Duration.ofSeconds(10))) {
            tracker.register(gateway);
            assertThat(tracker.add(first)).isTrue();
            assertThat(tracker.add(first)).isFalse();
            tracker.add(second);
            assertThat(notifications).hasSize(1);
            responses.get(0).completeExceptionally(new RuntimeException("connection not ready"));
            executor.triggerAll();
            executor.triggerNonPeriodicScheduledTasks();
            assertThat(notifications).hasSize(2);
            assertThat(notifications.get(1)).containsExactlyInAnyOrder(first, second);
            responses.get(1).complete(Acknowledge.get());
            executor.triggerAll();
            tracker.register(gateway);
            assertThat(notifications).hasSize(2);
            tracker.unregister(gateway);
            tracker.register(gateway);
            assertThat(notifications).hasSize(3);
            assertThat(notifications.get(2)).containsExactlyInAnyOrder(first, second);
            tracker.remove(first);
            assertThat(tracker.contains(first)).isFalse();
        }
        responses.get(2).completeExceptionally(new RuntimeException("closed"));
        executor.triggerAll();
        executor.triggerNonPeriodicScheduledTasks();
        assertThat(notifications).hasSize(3);
    }

    @Test
    void acknowledgesOnlyTheRevisionActuallySent() {
        final ManuallyTriggeredScheduledExecutor executor =
                new ManuallyTriggeredScheduledExecutor();
        final TestingJobMasterGateway gateway = new TestingJobMasterGatewayBuilder().build();
        final List<Collection<ResourceID>> notifications = new ArrayList<>();
        final CompletableFuture<Acknowledge> firstResponse = new CompletableFuture<>();
        gateway.setNotifyTaskManagersPendingEvictionFunction(
                taskManagers -> {
                    notifications.add(new ArrayList<>(taskManagers));
                    return notifications.size() == 1
                            ? firstResponse
                            : CompletableFuture.completedFuture(Acknowledge.get());
                });
        try (PendingTaskManagerEvictions tracker =
                new PendingTaskManagerEvictions(
                        ResourceManagerId.generate(), executor, Duration.ofSeconds(10))) {
            tracker.register(gateway);
            final ResourceID first = ResourceID.generate();
            final ResourceID second = ResourceID.generate();
            tracker.add(first);
            tracker.add(second);
            firstResponse.complete(Acknowledge.get());
            executor.triggerAll();
            assertThat(notifications).hasSize(2);
            assertThat(notifications.get(1)).containsExactlyInAnyOrder(first, second);
        }
    }
}

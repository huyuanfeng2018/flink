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

package org.apache.flink.runtime.scheduler.adaptive;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.TaskManagerEvictionOptions;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobResourceRequirements;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmaster.slotpool.DefaultAllocatedSlotPool;
import org.apache.flink.runtime.jobmaster.slotpool.DefaultDeclarativeSlotPool;
import org.apache.flink.runtime.scheduler.TaskManagerEvictionCoordinator;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.util.ResourceCounter;
import org.apache.flink.util.clock.SystemClock;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createNoOpVertex;
import static org.apache.flink.runtime.jobgraph.JobGraphTestUtils.streamingJobGraph;
import static org.apache.flink.runtime.jobmaster.slotpool.SlotPoolTestUtils.createSlotOffersForResourceRequirements;
import static org.assertj.core.api.Assertions.assertThat;

/** Integrates the real scheduler, slot ledger and coordinator across a late cancellation intent. */
class TaskManagerEvictionAdaptiveSchedulerTest extends AdaptiveSchedulerTestBase {
    @Test
    @Timeout(30)
    void mergesIntentDuringCancellationAndDeploysOnlyToHealthySlots() throws Exception {
        final JobVertex vertex = createNoOpVertex(2);
        final JobGraph graph = streamingJobGraph(vertex);
        final Configuration configuration = new Configuration();
        configuration.set(
                JobManagerOptions.SCHEDULER_SUBMISSION_RESOURCE_STABILIZATION_TIMEOUT,
                Duration.ZERO);
        // Checkpoint gating has deterministic tests of its own. Exercise the internal planned
        // restart entry point explicitly here, while preventing an automatic checkpoint trigger.
        configuration.set(TaskManagerEvictionOptions.QUIET_PERIOD, Duration.ofHours(1));
        configuration.set(TaskManagerEvictionOptions.MAX_BATCH_WAIT, Duration.ofHours(1));
        final DefaultDeclarativeSlotPool pool =
                new DefaultDeclarativeSlotPool(
                        graph.getJobID(),
                        new DefaultAllocatedSlotPool(),
                        ignored -> {},
                        Duration.ofMinutes(10),
                        Duration.ofSeconds(10),
                        Duration.ZERO,
                        singleThreadMainThreadExecutor);
        scheduler =
                new AdaptiveSchedulerBuilder(
                                graph,
                                singleThreadMainThreadExecutor,
                                EXECUTOR_RESOURCE.getExecutor())
                        .setJobMasterConfiguration(configuration)
                        .setJobResourceRequirements(
                                JobResourceRequirements.newBuilder()
                                        .setParallelismForJobVertex(vertex.getID(), 2, 2)
                                        .build())
                        .setDeclarativeSlotPool(pool)
                        .build();
        final CompletableFuture<Void> finished = new CompletableFuture<>();
        final TaskManagerEvictionCoordinator coordinator =
                new TaskManagerEvictionCoordinator(
                        new TaskManagerEvictionCoordinator.Context() {
                            @Override
                            public JobStatus getJobStatus() {
                                return scheduler.requestJobStatus();
                            }

                            @Override
                            public Optional<ExecutionGraph> getExecutionGraph() {
                                return scheduler.getExecutionGraphForTaskManagerEviction();
                            }

                            @Override
                            public boolean restart() {
                                return scheduler.restartForTaskManagerEviction();
                            }

                            @Override
                            public void onFinished() {
                                scheduler.onTaskManagerEvictionFinished();
                                finished.complete(null);
                            }
                        },
                        pool,
                        configuration,
                        null,
                        singleThreadMainThreadExecutor,
                        SystemClock.getInstance(),
                        new UnregisteredMetricsGroup());
        final AdaptiveSchedulerTest.SubmissionBufferingTaskManagerGateway gateway =
                new AdaptiveSchedulerTest.SubmissionBufferingTaskManagerGateway(8);
        final List<ExecutionAttemptID> cancellations = new ArrayList<>();
        gateway.setCancelConsumer(cancellations::add);
        final TaskManagerLocation first = new LocalTaskManagerLocation();
        final TaskManagerLocation second = new LocalTaskManagerLocation();
        final TaskManagerLocation replacement = new LocalTaskManagerLocation();
        final TaskManagerLocation lateReplacement = new LocalTaskManagerLocation();
        try {
            runInMainThread(
                    () -> {
                        scheduler.setTaskManagerEvictionCoordinator(coordinator);
                        scheduler.startScheduling();
                        offer(pool, gateway, first);
                        offer(pool, gateway, second);
                    });
            final List<TaskDeploymentDescriptor> initial = gateway.waitForSubmissions(2);
            runInMainThread(
                    () -> {
                        initial.forEach(
                                task ->
                                        scheduler.updateTaskExecutionState(
                                                new TaskExecutionState(
                                                        task.getExecutionAttemptId(),
                                                        ExecutionState.RUNNING)));
                        coordinator.notifyTaskManagersPendingEviction(
                                Collections.singleton(first.getResourceID()));
                        assertThat(coordinator.isReadyToDeploy()).isFalse();
                        offer(pool, gateway, replacement);
                        assertThat(coordinator.isReadyToDeploy()).isTrue();
                        assertThat(scheduler.restartForTaskManagerEviction()).isTrue();
                        assertThat(cancellations).hasSize(2);
                        coordinator.notifyTaskManagersPendingEviction(
                                Collections.singleton(second.getResourceID()));
                        assertThat(coordinator.isReadyToDeploy()).isFalse();
                        cancellations.forEach(
                                attempt ->
                                        scheduler.updateTaskExecutionState(
                                                new TaskExecutionState(
                                                        attempt, ExecutionState.CANCELED)));
                    });
            runInMainThread(
                    () -> {
                        assertThat(gateway.submittedTasks).isEmpty();
                        assertThat(pool.hasSufficientResourcesForTaskManagerEviction()).isFalse();
                        offer(pool, gateway, lateReplacement);
                        assertThat(pool.hasSufficientResourcesForTaskManagerEviction()).isTrue();
                    });
            final List<TaskDeploymentDescriptor> recovered = gateway.waitForSubmissions(2);
            runInMainThread(
                    () -> {
                        assertThat(pool.containsSlots(first.getResourceID())).isFalse();
                        assertThat(pool.containsSlots(second.getResourceID())).isFalse();
                        final ExecutionGraph executionGraph =
                                scheduler.getExecutionGraphForTaskManagerEviction().get();
                        assertThat(executionGraph.getJobVertex(vertex.getID()).getParallelism())
                                .isEqualTo(2);
                        executionGraph
                                .getAllExecutionVertices()
                                .forEach(
                                        task ->
                                                assertThat(
                                                                task.getCurrentAssignedResourceLocation()
                                                                        .getResourceID())
                                                        .isIn(
                                                                replacement.getResourceID(),
                                                                lateReplacement.getResourceID()));
                        recovered.forEach(
                                task ->
                                        scheduler.updateTaskExecutionState(
                                                new TaskExecutionState(
                                                        task.getExecutionAttemptId(),
                                                        ExecutionState.RUNNING)));
                    });
            finished.get(10, TimeUnit.SECONDS);
            assertThat(supplyInMainThread(coordinator::isActive)).isFalse();
            assertThat(gateway.submittedTasks).isEmpty();
        } finally {
            runInMainThread(
                    () -> {
                        coordinator.close();
                        gateway.setCancelConsumer(
                                attempt ->
                                        singleThreadMainThreadExecutor.execute(
                                                () ->
                                                        scheduler.updateTaskExecutionState(
                                                                new TaskExecutionState(
                                                                        attempt,
                                                                        ExecutionState.CANCELED))));
                        for (ExecutionAttemptID attempt : new ArrayList<>(cancellations)) {
                            scheduler.updateTaskExecutionState(
                                    new TaskExecutionState(attempt, ExecutionState.CANCELED));
                        }
                    });
        }
    }

    private void offer(
            DefaultDeclarativeSlotPool pool,
            AdaptiveSchedulerTest.SubmissionBufferingTaskManagerGateway gateway,
            TaskManagerLocation location) {
        final java.util.Collection<SlotOffer> offers =
                createSlotOffersForResourceRequirements(
                        ResourceCounter.withResource(ResourceProfile.UNKNOWN, 1));
        assertThat(pool.offerSlots(offers, location, gateway, System.currentTimeMillis()))
                .containsExactlyElementsOf(offers);
    }
}

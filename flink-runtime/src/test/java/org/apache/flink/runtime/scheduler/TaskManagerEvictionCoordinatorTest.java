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

package org.apache.flink.runtime.scheduler;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.TaskManagerEvictionOptions;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.checkpoint.CheckpointStatsCounts;
import org.apache.flink.runtime.checkpoint.CheckpointStatsSnapshot;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.PendingCheckpoint;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobmaster.slotpool.DeclarativeSlotPool;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.clock.ManualClock;
import org.apache.flink.util.concurrent.ManuallyTriggeredScheduledExecutor;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Deterministic main-thread tests; time advances without sleeping or a live Kubernetes cluster. */
class TaskManagerEvictionCoordinatorTest {
    @Test
    void repeatedIntentDoesNotRestartTheQuietWindow() {
        try (Fixture f = new Fixture()) {
            f.mark(f.oldLocation.getResourceID());
            f.advance(4);
            f.mark(f.oldLocation.getResourceID());
            f.advance(1);
            assertThat(f.coordinator.getPhase())
                    .isEqualTo(TaskManagerEvictionCoordinator.Phase.WAITING_CHECKPOINT);
            verify(f.pool, times(1)).beginTaskManagerEviction();
            assertThat(f.restarts).isZero();
        }
    }

    @Test
    void continuousNewIntentsCannotExtendMaximumBatchWindow() {
        try (Fixture f = new Fixture()) {
            f.mark(f.oldLocation.getResourceID());
            for (int i = 0; i < 7; i++) {
                f.advance(4);
                f.mark(ResourceID.generate());
            }
            f.advance(2);
            assertThat(f.coordinator.getPhase())
                    .isEqualTo(TaskManagerEvictionCoordinator.Phase.WAITING_CHECKPOINT);
        }
    }

    @Test
    void timeoutNeverWaivesResourceReadiness() {
        try (Fixture f = new Fixture()) {
            f.ready = false;
            f.mark(f.oldLocation.getResourceID());
            f.advance(100);
            assertThat(f.restarts).isZero();
            assertThat(f.status).isEqualTo(JobStatus.RUNNING);
            f.ready = true;
            f.coordinator.reconcile();
            f.advance(10);
            assertThat(f.restarts).isEqualTo(1);
        }
    }

    @Test
    void checkpointStartedBeforeReadinessDoesNotCountAsFresh() {
        try (Fixture f = new Fixture()) {
            when(f.checkpoints.getPendingCheckpoints())
                    .thenReturn(Collections.singletonMap(2L, mock(PendingCheckpoint.class)));
            f.mark(f.oldLocation.getResourceID());
            f.advance(5);
            f.completed(2, 0);
            f.coordinator.reconcile();
            assertThat(f.restarts).isZero();
            f.completed(3, f.clock.absoluteTimeMillis());
            f.coordinator.reconcile();
            f.coordinator.reconcile();
            assertThat(f.restarts).isEqualTo(1);
        }
    }

    @Test
    void timeoutIsNotResetWhenAnotherIntentMakesResourcesInsufficient() {
        try (Fixture f = new Fixture()) {
            f.mark(f.oldLocation.getResourceID());
            f.advance(5);
            f.advance(8);
            f.ready = false;
            f.mark(ResourceID.generate());
            f.advance(2);
            assertThat(f.restarts).isZero();
            f.ready = true;
            f.advance(3);
            assertThat(f.restarts).isEqualTo(1);
        }
    }

    @Test
    void consecutiveCheckpointFailuresAllowFallbackToRetainedState() {
        try (Fixture f = new Fixture()) {
            final CheckpointStatsSnapshot stats = mock(CheckpointStatsSnapshot.class);
            final CheckpointStatsCounts counts = mock(CheckpointStatsCounts.class);
            when(stats.getCounts()).thenReturn(counts);
            when(f.graph.getCheckpointStatsSnapshot()).thenReturn(stats);
            f.mark(f.oldLocation.getResourceID());
            f.advance(5);
            when(counts.getNumberOfFailedCheckpoints()).thenReturn(2L);
            f.coordinator.reconcile();
            assertThat(f.restarts).isEqualTo(1);
        }
    }

    @Test
    void missingRetainedCheckpointLeavesExistingTasksRunning() {
        try (Fixture f = new Fixture()) {
            when(f.store.getLatestCheckpoint()).thenReturn(null);
            f.mark(f.oldLocation.getResourceID());
            f.advance(5);
            f.advance(10);
            assertThat(f.coordinator.getPhase())
                    .isEqualTo(TaskManagerEvictionCoordinator.Phase.BLOCKED_NO_CHECKPOINT);
            assertThat(f.status).isEqualTo(JobStatus.RUNNING);
            assertThat(f.restarts).isZero();
            f.completed(2, f.clock.absoluteTimeMillis());
            f.coordinator.reconcile();
            assertThat(f.restarts).isEqualTo(1);
        }
    }

    @Test
    void newIntentDuringRecoveryHoldsRedeploymentUntilHealthyCapacityReturns() {
        try (Fixture f = new Fixture()) {
            f.startRestart();
            f.ready = false;
            f.mark(ResourceID.generate());
            final AtomicInteger deployments = new AtomicInteger();
            f.coordinator.runWhenReadyToDeploy(deployments::incrementAndGet);
            f.coordinator.reconcile();
            assertThat(deployments).hasValue(0);
            f.ready = true;
            f.coordinator.reconcile();
            assertThat(deployments).hasValue(1);
            assertThat(f.restarts).isEqualTo(1);
            f.runningOn(new LocalTaskManagerLocation());
            f.coordinator.reconcile();
            assertThat(f.coordinator.isActive()).isFalse();
            verify(f.pool).finishTaskManagerEviction();
        }
    }

    @Test
    void intentForAlreadyRedeployedTaskIsPreservedForNextRound() {
        try (Fixture f = new Fixture()) {
            f.startRestart();
            final TaskManagerLocation next = new LocalTaskManagerLocation();
            f.mark(next.getResourceID());
            f.runningOn(next);
            f.coordinator.reconcile();
            assertThat(f.coordinator.getPhase())
                    .isEqualTo(TaskManagerEvictionCoordinator.Phase.PREPARING);
            f.advance(5);
            f.completed(3, f.clock.absoluteTimeMillis());
            f.coordinator.reconcile();
            assertThat(f.restarts).isEqualTo(2);
        }
    }

    @Test
    void naturalRecoveryConsumesPendingIntentsWithoutAnotherPlannedRestart() {
        try (Fixture f = new Fixture()) {
            f.mark(f.oldLocation.getResourceID());
            f.advance(5);
            f.status = JobStatus.RESTARTING;
            f.coordinator.reconcile();
            f.runningOn(new LocalTaskManagerLocation());
            f.coordinator.reconcile();
            assertThat(f.coordinator.isActive()).isFalse();
            assertThat(f.restarts).isZero();
        }
    }

    @Test
    void activeCheckpointHonorsMinPauseAndClosedEpochIgnoresCallback() {
        try (Fixture f = new Fixture()) {
            f.configuration.set(
                    JobManagerOptions.SCHEDULER_RESCALE_TRIGGER_ACTIVE_CHECKPOINT_ENABLED, true);
            f.recreateCoordinator();
            final CompletableFuture<CompletedCheckpoint> checkpoint = new CompletableFuture<>();
            when(f.checkpoints.triggerCheckpoint(false)).thenReturn(checkpoint);
            when(f.checkpoints.isPeriodicCheckpointingConfigured()).thenReturn(true);
            when(f.checkpoints.getActiveCheckpointTriggerDelay())
                    .thenReturn(Optional.of(Duration.ofSeconds(1)));
            f.mark(f.oldLocation.getResourceID());
            f.advance(5);
            verify(f.checkpoints, never()).triggerCheckpoint(false);
            when(f.checkpoints.getActiveCheckpointTriggerDelay())
                    .thenReturn(Optional.of(Duration.ZERO));
            f.coordinator.reconcile();
            f.coordinator.reconcile();
            verify(f.checkpoints, times(1)).triggerCheckpoint(false);
            f.coordinator.close();
            checkpoint.complete(mock(CompletedCheckpoint.class));
            f.executor.triggerAll();
            f.executor.triggerNonPeriodicScheduledTasks();
            assertThat(f.restarts).isZero();
            assertThat(f.coordinator.getPhase())
                    .isEqualTo(TaskManagerEvictionCoordinator.Phase.CLOSED);
        }
    }

    private static final class Fixture
            implements TaskManagerEvictionCoordinator.Context, AutoCloseable {
        private final Configuration configuration = new Configuration();
        private final ManualClock clock = new ManualClock();
        private final ManuallyTriggeredScheduledExecutor executor =
                new ManuallyTriggeredScheduledExecutor();
        private final DeclarativeSlotPool pool = mock(DeclarativeSlotPool.class);
        private final ExecutionGraph graph = mock(ExecutionGraph.class);
        private final ExecutionVertex vertex = mock(ExecutionVertex.class);
        private final CheckpointCoordinator checkpoints = mock(CheckpointCoordinator.class);
        private final CompletedCheckpointStore store = mock(CompletedCheckpointStore.class);
        private final TaskManagerLocation oldLocation = new LocalTaskManagerLocation();
        private final Set<ResourceID> marked = new HashSet<>();
        private TaskManagerEvictionCoordinator coordinator;
        private JobStatus status = JobStatus.RUNNING;
        private ExecutionState executionState = ExecutionState.RUNNING;
        private TaskManagerLocation location = oldLocation;
        private boolean ready = true;
        private int restarts;

        private Fixture() {
            configuration.set(
                    TaskManagerEvictionOptions.MAX_CHECKPOINT_WAIT, Duration.ofSeconds(10));
            configuration.set(
                    JobManagerOptions.SCHEDULER_RESCALE_TRIGGER_ACTIVE_CHECKPOINT_ENABLED, false);
            when(graph.getAllExecutionVertices()).thenReturn(Collections.singletonList(vertex));
            when(graph.getCheckpointCoordinator()).thenReturn(checkpoints);
            when(checkpoints.getCheckpointStore()).thenReturn(store);
            when(checkpoints.getPendingCheckpoints()).thenReturn(Collections.emptyMap());
            when(vertex.getExecutionState()).thenAnswer(ignored -> executionState);
            when(vertex.getCurrentAssignedResourceLocation()).thenAnswer(ignored -> location);
            when(pool.hasSufficientResourcesForTaskManagerEviction()).thenAnswer(ignored -> ready);
            when(pool.markTaskManagersForEviction(anyCollection()))
                    .thenAnswer(
                            invocation -> {
                                Set<ResourceID> added = new HashSet<>(invocation.getArgument(0));
                                added.removeAll(marked);
                                marked.addAll(added);
                                return added;
                            });
            when(pool.isTaskManagerPendingEviction(any()))
                    .thenAnswer(invocation -> marked.contains(invocation.getArgument(0)));
            completed(1, 0);
            recreateCoordinator();
        }

        private void recreateCoordinator() {
            if (coordinator != null) {
                coordinator.close();
            }
            coordinator =
                    new TaskManagerEvictionCoordinator(
                            this,
                            pool,
                            configuration,
                            null,
                            executor,
                            clock,
                            new UnregisteredMetricsGroup());
        }

        private void mark(ResourceID id) {
            coordinator.notifyTaskManagersPendingEviction(Collections.singleton(id));
            coordinator.reconcile();
        }

        private void advance(int seconds) {
            clock.advanceTime(Duration.ofSeconds(seconds));
            coordinator.reconcile();
        }

        private void completed(long id, long triggerTime) {
            final CompletedCheckpoint checkpoint = mock(CompletedCheckpoint.class);
            when(checkpoint.getCheckpointID()).thenReturn(id);
            when(checkpoint.getTimestamp()).thenReturn(triggerTime);
            when(store.getLatestCheckpoint()).thenReturn(checkpoint);
        }

        private void startRestart() {
            mark(oldLocation.getResourceID());
            advance(5);
            completed(2, clock.absoluteTimeMillis());
            coordinator.reconcile();
            assertThat(restarts).isEqualTo(1);
        }

        private void runningOn(TaskManagerLocation newLocation) {
            status = JobStatus.RUNNING;
            executionState = ExecutionState.RUNNING;
            location = newLocation;
        }

        @Override
        public JobStatus getJobStatus() {
            return status;
        }

        @Override
        public Optional<ExecutionGraph> getExecutionGraph() {
            return Optional.of(graph);
        }

        @Override
        public boolean restart() {
            restarts++;
            status = JobStatus.RESTARTING;
            executionState = ExecutionState.CANCELING;
            return true;
        }

        @Override
        public void close() {
            coordinator.close();
        }
    }
}

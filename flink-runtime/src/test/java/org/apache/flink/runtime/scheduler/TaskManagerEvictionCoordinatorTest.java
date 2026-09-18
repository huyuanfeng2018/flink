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
import org.apache.flink.runtime.checkpoint.CheckpointCoordinatorTestingUtils.CheckpointCoordinatorBuilder;
import org.apache.flink.runtime.checkpoint.CheckpointProperties;
import org.apache.flink.runtime.checkpoint.CheckpointRetentionPolicy;
import org.apache.flink.runtime.checkpoint.CheckpointsCleaner;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.DefaultCheckpointStatsTracker;
import org.apache.flink.runtime.checkpoint.PendingCheckpoint;
import org.apache.flink.runtime.checkpoint.StandaloneCheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.StandaloneCompletedCheckpointStore;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.DefaultExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.TestingDefaultExecutionGraphBuilder;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobmaster.TestingLogicalSlotBuilder;
import org.apache.flink.runtime.jobmaster.slotpool.DefaultDeclarativeSlotPool;
import org.apache.flink.runtime.jobmaster.slotpool.DefaultDeclarativeSlotPoolBuilder;
import org.apache.flink.runtime.messages.checkpoint.AcknowledgeCheckpoint;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.slots.ResourceRequirement;
import org.apache.flink.runtime.state.testutils.TestCompletedCheckpointStorageLocation;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.testutils.DirectScheduledExecutorService;
import org.apache.flink.runtime.util.ResourceCounter;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.clock.ManualClock;
import org.apache.flink.util.concurrent.ManuallyTriggeredScheduledExecutor;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createNoOpVertex;
import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.setVertexState;
import static org.apache.flink.runtime.jobgraph.JobGraphTestUtils.streamingJobGraph;
import static org.assertj.core.api.Assertions.assertThat;

/** Deterministic policy tests using real checkpoint, slot-pool and execution-graph components. */
class TaskManagerEvictionCoordinatorTest {
    @Test
    void repeatedIntentDoesNotRestartTheQuietWindow() throws Exception {
        try (Fixture f = new Fixture()) {
            f.mark(f.oldLocation.getResourceID());
            f.advance(4);
            f.mark(f.oldLocation.getResourceID());
            f.advance(1);
            assertThat(f.coordinator.getPhase())
                    .isEqualTo(TaskManagerEvictionCoordinator.Phase.WAITING_CHECKPOINT);
            assertThat(f.pool.getResourceRequirements())
                    .containsExactly(ResourceRequirement.create(ResourceProfile.UNKNOWN, 2));
            assertThat(f.restarts).isZero();
        }
    }

    @Test
    void continuousNewIntentsCannotExtendMaximumBatchWindow() throws Exception {
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
    void timeoutNeverWaivesResourceReadiness() throws Exception {
        try (Fixture f = new Fixture()) {
            f.setReady(false);
            f.mark(f.oldLocation.getResourceID());
            f.advance(100);
            assertThat(f.restarts).isZero();
            assertThat(f.status).isEqualTo(JobStatus.RUNNING);
            f.setReady(true);
            f.coordinator.reconcile();
            f.advance(10);
            assertThat(f.restarts).isEqualTo(1);
        }
    }

    @Test
    void checkpointStartedBeforeReadinessDoesNotCountAsFresh() throws Exception {
        try (Fixture f = new Fixture()) {
            f.checkpoints.triggerCheckpoint(false);
            f.checkpointExecutor.triggerAll();
            assertThat(f.checkpoints.getPendingCheckpoints()).containsOnlyKeys(2L);
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
    void timeoutIsNotResetWhenAnotherIntentMakesResourcesInsufficient() throws Exception {
        try (Fixture f = new Fixture()) {
            f.mark(f.oldLocation.getResourceID());
            f.advance(5);
            f.advance(8);
            f.setReady(false);
            f.mark(ResourceID.generate());
            f.advance(2);
            assertThat(f.restarts).isZero();
            f.setReady(true);
            f.advance(3);
            assertThat(f.restarts).isEqualTo(1);
        }
    }

    @Test
    void consecutiveCheckpointFailuresAllowFallbackToRetainedState() throws Exception {
        try (Fixture f = new Fixture()) {
            f.mark(f.oldLocation.getResourceID());
            f.advance(5);
            f.stats.reportFailedCheckpointsWithoutInProgress();
            f.stats.reportFailedCheckpointsWithoutInProgress();
            f.coordinator.reconcile();
            assertThat(f.restarts).isEqualTo(1);
        }
    }

    @Test
    void missingRetainedCheckpointLeavesExistingTasksRunning() throws Exception {
        try (Fixture f = new Fixture(false, false)) {
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
    void newIntentDuringRecoveryHoldsRedeploymentUntilHealthyCapacityReturns() throws Exception {
        try (Fixture f = new Fixture()) {
            f.startRestart();
            f.setReady(false);
            f.mark(ResourceID.generate());
            final AtomicInteger deployments = new AtomicInteger();
            f.coordinator.runWhenReadyToDeploy(deployments::incrementAndGet);
            f.coordinator.reconcile();
            assertThat(deployments).hasValue(0);
            f.setReady(true);
            f.coordinator.reconcile();
            assertThat(deployments).hasValue(1);
            assertThat(f.restarts).isEqualTo(1);
            f.runningOn(new LocalTaskManagerLocation());
            f.coordinator.reconcile();
            assertThat(f.coordinator.isActive()).isFalse();
            assertThat(f.pool.getResourceRequirements())
                    .containsExactly(ResourceRequirement.create(ResourceProfile.UNKNOWN, 1));
        }
    }

    @Test
    void intentForAlreadyRedeployedTaskIsPreservedForNextRound() throws Exception {
        try (Fixture f = new Fixture()) {
            f.startRestart();
            final TaskManagerLocation next = new LocalTaskManagerLocation();
            f.runningOn(next);
            f.mark(next.getResourceID());
            assertThat(f.coordinator.getPhase())
                    .isEqualTo(TaskManagerEvictionCoordinator.Phase.PREPARING);
            f.advance(5);
            f.completed(3, f.clock.absoluteTimeMillis());
            f.coordinator.reconcile();
            assertThat(f.restarts).isEqualTo(2);
        }
    }

    @Test
    void naturalRecoveryConsumesPendingIntentsWithoutAnotherPlannedRestart() throws Exception {
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
    void activeCheckpointHonorsMinPauseAndClosedEpochIgnoresCallback() throws Exception {
        try (Fixture f = new Fixture(true, true)) {
            f.mark(f.oldLocation.getResourceID());
            f.advance(5);
            f.checkpointExecutor.triggerAll();
            assertThat(f.checkpoints.getPendingCheckpoints()).isEmpty();
            assertThat(f.checkpoints.getActiveCheckpointTriggerDelay())
                    .contains(Duration.ofSeconds(1));
            f.advance(1);
            f.coordinator.reconcile();
            f.checkpointExecutor.triggerAll();
            assertThat(f.checkpoints.getPendingCheckpoints()).hasSize(1);
            final PendingCheckpoint pending =
                    f.checkpoints.getPendingCheckpoints().values().iterator().next();
            f.coordinator.close();
            f.checkpoints.receiveAcknowledgeMessage(
                    new AcknowledgeCheckpoint(
                            f.graph.getJobID(),
                            f.vertex.getCurrentExecutionAttempt().getAttemptId(),
                            pending.getCheckpointID()),
                    "test TaskManager");
            assertThat(pending.getCompletionFuture()).isCompleted();
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
        private final ManuallyTriggeredScheduledExecutor checkpointExecutor =
                new ManuallyTriggeredScheduledExecutor();
        private final DirectScheduledExecutorService graphExecutor =
                new DirectScheduledExecutorService();
        private final DefaultDeclarativeSlotPool pool =
                DefaultDeclarativeSlotPoolBuilder.builder()
                        .setSlotRequestMaxInterval(Duration.ZERO)
                        .build();
        private final SimpleAckingTaskManagerGateway gateway =
                new SimpleAckingTaskManagerGateway();
        private final DefaultExecutionGraph graph;
        private final ExecutionVertex vertex;
        private final CheckpointCoordinator checkpoints;
        private final StandaloneCheckpointIDCounter checkpointIds =
                new StandaloneCheckpointIDCounter();
        private final StandaloneCompletedCheckpointStore store =
                new StandaloneCompletedCheckpointStore(1);
        private final CheckpointsCleaner cleaner = new CheckpointsCleaner();
        private final DefaultCheckpointStatsTracker stats =
                new DefaultCheckpointStatsTracker(
                        10, UnregisteredMetricGroups.createUnregisteredJobManagerJobMetricGroup());
        private final TaskManagerLocation oldLocation = new LocalTaskManagerLocation();
        private final TaskManagerEvictionCoordinator coordinator;
        private JobStatus status = JobStatus.RUNNING;
        private boolean prepareReplacement = true;
        private AllocationID runningAllocation;
        private AllocationID replacementAllocation;
        private int restarts;

        private Fixture() throws Exception {
            this(false, true);
        }

        private Fixture(boolean activeCheckpoint, boolean retainedCheckpoint) throws Exception {
            configuration.set(
                    TaskManagerEvictionOptions.MAX_CHECKPOINT_WAIT, Duration.ofSeconds(10));
            configuration.set(
                    JobManagerOptions.SCHEDULER_RESCALE_TRIGGER_ACTIVE_CHECKPOINT_ENABLED,
                    activeCheckpoint);
            final JobVertex jobVertex = createNoOpVertex(1);
            final JobGraph jobGraph = streamingJobGraph(jobVertex);
            graph =
                    TestingDefaultExecutionGraphBuilder.newBuilder()
                            .setJobGraph(jobGraph)
                            .build(graphExecutor);
            graph.start(ComponentMainThreadExecutorServiceAdapter.forMainThread());
            graph.transitionToRunning();
            vertex = graph.getJobVertex(jobVertex.getID()).getTaskVertices()[0];
            vertex.tryAssignResource(
                    new TestingLogicalSlotBuilder()
                            .setTaskManagerLocation(oldLocation)
                            .createTestingLogicalSlot());
            setVertexState(vertex, ExecutionState.RUNNING);
            checkpoints =
                    new CheckpointCoordinatorBuilder()
                            .setClock(clock)
                            .setTimer(checkpointExecutor)
                            .setCheckpointIDCounter(checkpointIds)
                            .setCompletedCheckpointStore(store)
                            .setCheckpointsCleaner(cleaner)
                            .setCheckpointStatsTracker(stats)
                            .setCheckpointCoordinatorConfiguration(
                                    CheckpointCoordinatorConfiguration.builder()
                                            .setCheckpointInterval(60_000)
                                            .setCheckpointTimeout(60_000)
                                            .setMinPauseBetweenCheckpoints(
                                                    activeCheckpoint ? 6_000 : 0)
                                            .build())
                            .build(graph);
            if (retainedCheckpoint) {
                completed(1, 0);
            }
            pool.setResourceRequirements(ResourceCounter.withResource(ResourceProfile.UNKNOWN, 1));
            runningAllocation = offer(oldLocation);
            pool.reserveFreeSlot(runningAllocation, ResourceProfile.UNKNOWN);
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

        private AllocationID offer(TaskManagerLocation location) {
            final SlotOffer offer = new SlotOffer(new AllocationID(), 0, ResourceProfile.UNKNOWN);
            assertThat(
                            pool.offerSlots(
                                    Collections.singleton(offer),
                                    location,
                                    gateway,
                                    clock.relativeTimeMillis()))
                    .containsExactly(offer);
            return offer.getAllocationId();
        }

        private void mark(ResourceID id) {
            coordinator.notifyTaskManagersPendingEviction(Collections.singleton(id));
            updateReplacement();
            coordinator.reconcile();
        }

        private void setReady(boolean ready) {
            prepareReplacement = ready;
            if (!ready && replacementAllocation != null) {
                pool.releaseSlot(replacementAllocation, new FlinkException("Replacement lost"));
                replacementAllocation = null;
            }
            updateReplacement();
        }

        private void updateReplacement() {
            if (prepareReplacement && !pool.hasSufficientResourcesForTaskManagerEviction()) {
                replacementAllocation = offer(new LocalTaskManagerLocation());
            }
        }

        private void advance(int seconds) {
            clock.advanceTime(Duration.ofSeconds(seconds));
            coordinator.reconcile();
        }

        private void completed(long id, long triggerTime) throws Exception {
            store.addCheckpointAndSubsumeOldestOne(
                    new CompletedCheckpoint(
                            graph.getJobID(),
                            id,
                            triggerTime,
                            triggerTime,
                            Collections.emptyMap(),
                            Collections.emptyList(),
                            CheckpointProperties.forCheckpoint(
                                    CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
                            new TestCompletedCheckpointStorageLocation(),
                            null),
                    cleaner,
                    () -> {});
            checkpointIds.setCount(id + 1);
        }

        private void startRestart() throws Exception {
            mark(oldLocation.getResourceID());
            advance(5);
            completed(2, clock.absoluteTimeMillis());
            coordinator.reconcile();
            assertThat(restarts).isEqualTo(1);
        }

        private void runningOn(TaskManagerLocation newLocation) throws Exception {
            setReady(false);
            pool.releaseSlot(runningAllocation, new FlinkException("Previous execution cancelled"));
            runningAllocation = offer(newLocation);
            pool.reserveFreeSlot(runningAllocation, ResourceProfile.UNKNOWN);
            setVertexState(vertex, ExecutionState.CANCELED);
            vertex.resetForNewExecution();
            assertThat(
                            vertex.tryAssignResource(
                                    new TestingLogicalSlotBuilder()
                                            .setTaskManagerLocation(newLocation)
                                            .createTestingLogicalSlot()))
                    .isTrue();
            setVertexState(vertex, ExecutionState.RUNNING);
            status = JobStatus.RUNNING;
            prepareReplacement = true;
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
        public CheckpointCoordinator getCheckpointCoordinator() {
            return checkpoints;
        }

        @Override
        public long getFailedCheckpointCount() {
            return stats.createSnapshot().getCounts().getNumberOfFailedCheckpoints();
        }

        @Override
        public boolean restart() {
            restarts++;
            status = JobStatus.RESTARTING;
            setVertexState(vertex, ExecutionState.CANCELING);
            return true;
        }

        @Override
        public void close() throws Exception {
            coordinator.close();
            checkpoints.shutdown();
            store.shutdown(JobStatus.FINISHED, cleaner);
            graphExecutor.shutdownNow();
        }
    }
}

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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.TaskManagerEvictionOptions;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.checkpoint.CheckpointStatsSnapshot;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.runtime.jobmaster.slotpool.DeclarativeSlotPool;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.clock.Clock;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * JobMaster-owned coordinator for cooperative worker replacement. All methods and callbacks run on
 * the JobMaster main thread. The coordinator outlives scheduler execution states and never deletes
 * workers; draining slots are returned to the normal resource-reconciliation path.
 */
public final class TaskManagerEvictionCoordinator implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(TaskManagerEvictionCoordinator.class);
    private static final Duration RECONCILIATION_INTERVAL = Duration.ofSeconds(1);

    /** Narrow adapter to the scheduler's execution graph and planned restart entry point. */
    public interface Context {
        JobStatus getJobStatus();

        Optional<ExecutionGraph> getExecutionGraph();

        boolean restart();

        default void onFinished() {}
    }

    @VisibleForTesting
    enum Phase {
        IDLE,
        PREPARING,
        WAITING_CHECKPOINT,
        BLOCKED_NO_CHECKPOINT,
        RECOVERING,
        CLOSED
    }

    private final Context context;
    private final DeclarativeSlotPool slotPool;
    private final ScheduledExecutor executor;
    private final Clock clock;
    private final long quietPeriodMillis;
    private final long maxBatchWaitMillis;
    private final long maxCheckpointWaitMillis;
    private final int maxCheckpointFailures;
    private final boolean activeCheckpointEnabled;
    private final Set<ResourceID> pendingTaskManagers = new HashSet<>();
    private final List<Runnable> recoveryContinuations = new ArrayList<>();

    private Phase phase = Phase.IDLE;
    @Nullable private ScheduledFuture<?> scheduledCheck;
    private long firstIntentTime;
    private long lastIntentTime;
    private long checkpointDeadline = -1;
    private long resourcesReadyTime;
    private long checkpointIdBeforeReady;
    private long checkpointFailuresBeforeWait;
    private long epoch;
    private boolean activeCheckpointInFlight;
    private long plannedRestarts;
    private long checkpointFallbacks;

    public TaskManagerEvictionCoordinator(
            Context context,
            DeclarativeSlotPool slotPool,
            Configuration configuration,
            @Nullable JobCheckpointingSettings checkpointingSettings,
            ScheduledExecutor executor,
            Clock clock,
            MetricGroup metrics) {
        this.context = Preconditions.checkNotNull(context);
        this.slotPool = Preconditions.checkNotNull(slotPool);
        this.executor = Preconditions.checkNotNull(executor);
        this.clock = Preconditions.checkNotNull(clock);
        quietPeriodMillis =
                nonNegativeMillis(configuration.get(TaskManagerEvictionOptions.QUIET_PERIOD));
        maxBatchWaitMillis =
                nonNegativeMillis(configuration.get(TaskManagerEvictionOptions.MAX_BATCH_WAIT));
        maxCheckpointFailures =
                configuration.get(
                        JobManagerOptions.SCHEDULER_RESCALE_TRIGGER_MAX_CHECKPOINT_FAILURES);
        Preconditions.checkArgument(
                maxCheckpointFailures > 0, "Checkpoint failure limit must be positive.");
        final Duration defaultCheckpointWait =
                checkpointingSettings != null
                                && checkpointingSettings
                                        .getCheckpointCoordinatorConfiguration()
                                        .isCheckpointingEnabled()
                        ? Duration.ofMillis(
                                        checkpointingSettings
                                                .getCheckpointCoordinatorConfiguration()
                                                .getCheckpointInterval())
                                .multipliedBy((long) maxCheckpointFailures + 1)
                        : Duration.ZERO;
        maxCheckpointWaitMillis =
                nonNegativeMillis(
                        configuration
                                .getOptional(TaskManagerEvictionOptions.MAX_CHECKPOINT_WAIT)
                                .orElseGet(
                                        () ->
                                                configuration.get(
                                                        JobManagerOptions
                                                                .SCHEDULER_RESCALE_TRIGGER_MAX_DELAY,
                                                        defaultCheckpointWait)));
        activeCheckpointEnabled =
                configuration.get(
                        JobManagerOptions.SCHEDULER_RESCALE_TRIGGER_ACTIVE_CHECKPOINT_ENABLED);
        metrics.gauge("pendingTaskManagers", () -> pendingTaskManagers.size());
        metrics.gauge("phase", () -> phase.name());
        metrics.gauge("plannedRestarts", () -> plannedRestarts);
        metrics.gauge("checkpointFallbacks", () -> checkpointFallbacks);
        LOG.info(
                "Cooperative TaskManager eviction enabled: quiet period {} ms, maximum batch wait {} ms, checkpoint wait {} ms, failure limit {}.",
                quietPeriodMillis,
                maxBatchWaitMillis,
                maxCheckpointWaitMillis,
                maxCheckpointFailures);
    }

    private static long nonNegativeMillis(Duration duration) {
        Preconditions.checkArgument(
                !duration.isNegative(), "Eviction timeout must not be negative.");
        return duration.toMillis();
    }

    public void notifyTaskManagersPendingEviction(Collection<ResourceID> taskManagers) {
        if (phase == Phase.CLOSED) {
            return;
        }
        final Set<ResourceID> newlyMarked = slotPool.markTaskManagersForEviction(taskManagers);
        if (newlyMarked.isEmpty()) {
            return;
        }
        pendingTaskManagers.addAll(newlyMarked);
        lastIntentTime = clock.relativeTimeMillis();
        if (phase == Phase.IDLE) {
            beginRound(lastIntentTime);
        }
        LOG.info("Added TaskManager eviction intents {}; current phase {}.", newlyMarked, phase);
        scheduleCheck(Duration.ZERO);
    }

    private void beginRound(long now) {
        firstIntentTime = now;
        lastIntentTime = now;
        resetCheckpointWait();
        slotPool.beginTaskManagerEviction();
        phase = Phase.PREPARING;
    }

    public boolean isActive() {
        return phase != Phase.IDLE && phase != Phase.CLOSED;
    }

    public boolean isReadyToDeploy() {
        return !isActive() || slotPool.hasSufficientResourcesForTaskManagerEviction();
    }

    /** Called after cancellation, before resetting/restoring and allocating new executions. */
    public void runWhenReadyToDeploy(Runnable continuation) {
        if (phase == Phase.CLOSED) {
            return;
        }
        if (isReadyToDeploy()) {
            continuation.run();
        } else {
            recoveryContinuations.add(continuation);
            scheduleCheck(Duration.ZERO);
        }
    }

    private void scheduleCheck(Duration delay) {
        if (isActive() && scheduledCheck == null) {
            scheduledCheck =
                    executor.schedule(
                            () -> {
                                scheduledCheck = null;
                                reconcile();
                                scheduleCheck(RECONCILIATION_INTERVAL);
                            },
                            delay.toMillis(),
                            TimeUnit.MILLISECONDS);
        }
    }

    @VisibleForTesting
    void reconcile() {
        if (!isActive()) {
            return;
        }
        if (context.getJobStatus().isTerminalState()) {
            close();
            return;
        }
        if (isReadyToDeploy() && !recoveryContinuations.isEmpty()) {
            final List<Runnable> ready = new ArrayList<>(recoveryContinuations);
            recoveryContinuations.clear();
            for (Runnable continuation : ready) {
                // Each continuation must still validate its scheduler state/execution versions.
                continuation.run();
            }
        }
        final Optional<ExecutionGraph> graph = context.getExecutionGraph();
        if (context.getJobStatus() != JobStatus.RUNNING
                || !graph.isPresent()
                || !allTasksRunning(graph.get())) {
            if (phase != Phase.RECOVERING) {
                resetCheckpointWait();
                phase = Phase.RECOVERING;
            }
            return;
        }
        if (!hasAffectedTasks(graph.get())) {
            finish();
            return;
        }
        final long now = clock.relativeTimeMillis();
        if (phase == Phase.RECOVERING) {
            // A late intent can target an execution that has already been redeployed. Preserve it
            // and coalesce a new round; do not interrupt an in-progress recovery a second time.
            beginRound(now);
        }
        if (!slotPool.hasSufficientResourcesForTaskManagerEviction()) {
            phase = Phase.PREPARING;
            return;
        }
        if (phase == Phase.PREPARING) {
            if (now - lastIntentTime < quietPeriodMillis
                    && now - firstIntentTime < maxBatchWaitMillis) {
                return;
            }
            enterCheckpointWait(graph.get(), now);
        }
        evaluateCheckpoint(graph.get(), now);
    }

    private boolean allTasksRunning(ExecutionGraph graph) {
        for (ExecutionVertex vertex : graph.getAllExecutionVertices()) {
            final ExecutionState state = vertex.getExecutionState();
            if (state != ExecutionState.RUNNING && state != ExecutionState.FINISHED) {
                return false;
            }
        }
        return true;
    }

    private boolean hasAffectedTasks(ExecutionGraph graph) {
        for (ExecutionVertex vertex : graph.getAllExecutionVertices()) {
            final TaskManagerLocation location = vertex.getCurrentAssignedResourceLocation();
            if (vertex.getExecutionState() != ExecutionState.FINISHED
                    && location != null
                    && slotPool.isTaskManagerPendingEviction(location.getResourceID())) {
                return true;
            }
        }
        return false;
    }

    private void enterCheckpointWait(ExecutionGraph graph, long now) {
        resourcesReadyTime = clock.absoluteTimeMillis();
        checkpointIdBeforeReady = -1;
        final CheckpointCoordinator coordinator = graph.getCheckpointCoordinator();
        if (coordinator != null) {
            final CompletedCheckpoint latest =
                    coordinator.getCheckpointStore().getLatestCheckpoint();
            if (latest != null) {
                checkpointIdBeforeReady = latest.getCheckpointID();
            }
            for (long pending : coordinator.getPendingCheckpoints().keySet()) {
                checkpointIdBeforeReady = Math.max(checkpointIdBeforeReady, pending);
            }
        }
        if (checkpointDeadline < 0) {
            checkpointDeadline =
                    now > Long.MAX_VALUE - maxCheckpointWaitMillis
                            ? Long.MAX_VALUE
                            : now + maxCheckpointWaitMillis;
            checkpointFailuresBeforeWait = failedCheckpoints(graph);
        }
        phase = Phase.WAITING_CHECKPOINT;
    }

    private long failedCheckpoints(ExecutionGraph graph) {
        final CheckpointStatsSnapshot snapshot = graph.getCheckpointStatsSnapshot();
        return snapshot == null ? 0 : snapshot.getCounts().getNumberOfFailedCheckpoints();
    }

    private void evaluateCheckpoint(ExecutionGraph graph, long now) {
        final CheckpointCoordinator coordinator = graph.getCheckpointCoordinator();
        final CompletedCheckpoint latest =
                coordinator == null ? null : coordinator.getCheckpointStore().getLatestCheckpoint();
        final boolean fresh =
                latest != null
                        && latest.getCheckpointID() > checkpointIdBeforeReady
                        && latest.getTimestamp() >= resourcesReadyTime;
        final boolean fallback =
                now >= checkpointDeadline
                        || failedCheckpoints(graph) - checkpointFailuresBeforeWait
                                >= maxCheckpointFailures;
        if ((fresh || fallback) && latest != null) {
            // Recheck immediately before cancellation. A timeout never waives resource safety.
            if (slotPool.hasSufficientResourcesForTaskManagerEviction() && context.restart()) {
                plannedRestarts++;
                if (!fresh) {
                    checkpointFallbacks++;
                }
                LOG.info(
                        "Starting cooperative TaskManager replacement using checkpoint {} (fallback={}).",
                        latest.getCheckpointID(),
                        !fresh);
                resetCheckpointWait();
                phase = Phase.RECOVERING;
            }
        } else if (fallback) {
            if (phase != Phase.BLOCKED_NO_CHECKPOINT) {
                LOG.warn(
                        "TaskManager eviction blocked: no retained completed checkpoint is available. Existing tasks are left running.");
            }
            phase = Phase.BLOCKED_NO_CHECKPOINT;
        } else if (coordinator != null
                && activeCheckpointEnabled
                && !activeCheckpointInFlight
                && coordinator.isPeriodicCheckpointingConfigured()
                && coordinator
                        .getActiveCheckpointTriggerDelay()
                        .filter(Duration::isZero)
                        .isPresent()) {
            final long checkpointEpoch = epoch;
            activeCheckpointInFlight = true;
            coordinator
                    .triggerCheckpoint(false)
                    .whenCompleteAsync(
                            (checkpoint, failure) -> {
                                if (checkpointEpoch != epoch || !isActive()) {
                                    return;
                                }
                                activeCheckpointInFlight = false;
                                if (failure != null) {
                                    LOG.debug(
                                            "Active checkpoint for TaskManager eviction failed.",
                                            failure);
                                }
                                // The normal completion/store path, not this future alone,
                                // establishes recovery
                                // eligibility. In particular a newer intent may have invalidated
                                // resource readiness.
                                scheduleCheck(Duration.ZERO);
                            },
                            executor);
        }
    }

    private void resetCheckpointWait() {
        epoch++;
        checkpointDeadline = -1;
        activeCheckpointInFlight = false;
    }

    private void finish() {
        slotPool.finishTaskManagerEviction();
        pendingTaskManagers.clear();
        resetCheckpointWait();
        phase = Phase.IDLE;
        context.onFinished();
    }

    @VisibleForTesting
    Phase getPhase() {
        return phase;
    }

    @Override
    public void close() {
        if (phase == Phase.CLOSED) {
            return;
        }
        if (scheduledCheck != null) {
            scheduledCheck.cancel(false);
            scheduledCheck = null;
        }
        if (isActive()) {
            slotPool.finishTaskManagerEviction();
        }
        recoveryContinuations.clear();
        pendingTaskManagers.clear();
        resetCheckpointWait();
        phase = Phase.CLOSED;
    }
}

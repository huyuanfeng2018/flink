---
title: "Cooperative TaskManager Eviction"
weight: 80
type: docs
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements. See the NOTICE file
distributed with this work for additional information
regarding copyright ownership. The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied. See the License for the
specific language governing permissions and limitations
under the License.
-->

# Cooperative TaskManager Eviction

This opt-in capability supports a single streaming job in **native Kubernetes Application mode**,
with the Default or Adaptive scheduler and homogeneous TaskManagers using uniform default slot
resource profiles. It performs a planned job recovery, not live task migration or a zero-downtime
handover. It does not support revoking an eviction intent, JobManager eviction, Session clusters,
or fine-grained heterogeneous resource requirements.

## Enable and request eviction

Set the following cluster configuration before starting the application:

```yaml
jobmanager.taskmanager-eviction.enabled: true
jobmanager.taskmanager-eviction.batch.quiet-period: 5 s
jobmanager.taskmanager-eviction.batch.max-wait: 30 s
# Optional. Otherwise use the Adaptive scheduler's rescale-trigger policy.
jobmanager.taskmanager-eviction.checkpoint.max-wait: 2 min
```

The feature is disabled by default. An external resource controller requests eviction by annotating
one or more TaskManager Pods:

```bash
kubectl annotate pod <taskmanager-pod> flink/pending-eviction=true
```

Do not delete the Pod immediately. The annotation is an irrevocable intent, not a termination
signal. Flink continues running its existing tasks while preparing healthy slots. Newly allocated
slots are no longer assigned to that TaskManager. Existing allocation offers may be acknowledged
again to avoid interrupting running tasks. A task cannot be scheduled back onto a draining worker.
The ResourceManager replays the pending intent set when its JobMaster connection is re-established.
Existing annotated Pods are inspected on driver initialization as well as on watch updates.

## Resource and checkpoint gates

The JobMaster declares replacement demand through the existing declarative slot pool. During a
replacement cycle, effective demand is the larger of the normal scheduler demand and its pinned
recovery demand, plus allocations still held on draining workers. The pinned demand survives the
Default scheduler temporarily releasing its normal requests while cancelling executions.

Resources are ready only after the JobMaster actually holds sufficient **healthy** slots; Pod
creation, Kubernetes scheduling or TaskManager registration alone are not enough. Necessary
replacement slots remain in the resource demand and therefore are not excess idle resources.
Unrelated excess slots and idle TaskManagers keep their existing recycling behavior.
Temporary replacement capacity must fit both Flink's total resource limits and Kubernetes quotas.
If it does not, the original tasks keep running and eviction remains in resource preparation.

After the quiet period, or at most the batch maximum wait, a resource-ready cycle waits for a
checkpoint triggered after the readiness boundary. An already-pending checkpoint is not treated
as a fresh one merely because it completes later. Active checkpoint triggering follows the
Adaptive scheduler's active-checkpoint setting and respects the checkpoint coordinator's minimum
pause and in-progress checkpoint checks.

The wait limit is, in order of precedence:

1. `jobmanager.taskmanager-eviction.checkpoint.max-wait`, when configured;
2. `jobmanager.adaptive-scheduler.rescale-trigger.max-delay`, when configured;
3. the checkpoint interval multiplied by one plus
   `jobmanager.adaptive-scheduler.rescale-trigger.max-checkpoint-failures` (default 2).

If periodic checkpointing is disabled the inferred wait is zero. Reaching the wait limit, or the
checkpoint failure threshold, permits recovery from the latest retained completed checkpoint.
A timeout **never** waives the healthy-resource gate. If no retained completed checkpoint exists,
eviction is blocked and existing tasks continue running; Flink does not silently recover from
empty state. Consequently an application without recoverable checkpoint state cannot use this
capability to force a restart. Timing is evaluated on the JobMaster main thread, at approximately
one-second intervals while a cycle is active.

## Recovery and concurrent intents

Planned recovery reuses execution cancellation, coordinator notification, checkpoint restore and
scheduling without consuming the configured failure-restart budget. Genuine failures still follow
the normal failure policy. The Adaptive scheduler keeps the current parallelism during a planned
replacement rather than interpreting pre-warmed slots as a request for a separate scale-up.
Parallelism update requests during an active replacement are rejected and must be retried after
it finishes.

Repeated annotation events do not create additional demand or restart the batch timer. Additional
intents extend the same pending set, but do not move an existing checkpoint deadline. Before
redeployment the schedulers recheck the latest excluded TaskManagers and resource readiness.
Intents received during cancellation can therefore join the current recovery. If an intent arrives
after a new execution has already been assigned/deployed to its target worker, another bounded
replacement round may be necessary. Indefinitely arriving intents cannot be guaranteed to fit in
one restart. A genuine recovery already in progress may also incorporate pending intents.

When executions have left an old TaskManager, its slots are returned. The existing idle timeout,
`canBeReleased` check and resource-reconciliation path remove the Pod. There is no unconditional
Pod deletion at checkpoint completion and no global suspension of idle recycling.

## Observability and validation

The JobManager job metric subgroup `taskManagerEviction` exposes `pendingTaskManagers`, `phase`,
`plannedRestarts` and `checkpointFallbacks`. Logs identify the marked TaskManagers, phase and
checkpoint used. `BLOCKED_NO_CHECKPOINT` explicitly indicates absence of recoverable state.
The pending count describes the active cycle; it can include workers already detached while
another worker in the same cycle is still being handled.

Focused validation commands (including upstream dependency modules) are:

```bash
# Build the isolated RPC implementation and its loader before running RPC-based tests.
./mvnw -B -pl flink-kubernetes -am -DskipTests install
./mvnw -B -pl flink-kubernetes -am \
  -Dtest='*Eviction*,KubernetesPodTest,KubernetesResourceManagerDriverTest,DefaultSchedulerTest,ExecutingTest' \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

The deterministic tests cover demand preservation, duplicate offers, idle resource behavior,
checkpoint freshness and fallback, bounded batching, late intents, notification retry/replay and
planned scheduler recovery. A real Kubernetes soak test with stateful streaming sources/sinks,
quota exhaustion and genuine failovers is still required before production enablement.

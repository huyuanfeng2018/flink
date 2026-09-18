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
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/** Main-thread-confined, monotonic eviction intents and reliable JobMaster notification. */
final class PendingTaskManagerEvictions implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(PendingTaskManagerEvictions.class);
    private final Set<ResourceID> taskManagers = new HashSet<>();
    private final Map<JobMasterGateway, Listener> listeners = new HashMap<>();
    private final ResourceManagerId resourceManagerId;
    private final ScheduledExecutor executor;
    private final Duration rpcTimeout;
    private long revision;

    PendingTaskManagerEvictions(
            ResourceManagerId resourceManagerId, ScheduledExecutor executor, Duration rpcTimeout) {
        this.resourceManagerId = resourceManagerId;
        this.executor = executor;
        this.rpcTimeout = rpcTimeout;
    }

    boolean add(ResourceID taskManager) {
        if (!taskManagers.add(taskManager)) {
            return false;
        }
        revision++;
        for (Listener listener : new ArrayList<>(listeners.values())) {
            send(listener);
        }
        return true;
    }

    boolean contains(ResourceID taskManager) {
        return taskManagers.contains(taskManager);
    }

    void remove(ResourceID taskManager) {
        // Removal means the worker is gone, not that its eviction was revoked.
        taskManagers.remove(taskManager);
    }

    void register(JobMasterGateway gateway) {
        send(listeners.computeIfAbsent(gateway, Listener::new));
    }

    void unregister(JobMasterGateway gateway) {
        final Listener listener = listeners.remove(gateway);
        if (listener != null && listener.retry != null) {
            listener.retry.cancel(false);
        }
    }

    private void send(Listener listener) {
        if (listeners.get(listener.gateway) != listener
                || listener.inFlight
                || taskManagers.isEmpty()
                || listener.acknowledgedRevision == revision) {
            return;
        }
        if (listener.retry != null) {
            listener.retry.cancel(false);
            listener.retry = null;
        }
        final long sentRevision = revision;
        listener.inFlight = true;
        listener.gateway
                .notifyTaskManagersPendingEviction(
                        resourceManagerId, new ArrayList<>(taskManagers), rpcTimeout)
                .whenCompleteAsync(
                        (ignored, failure) -> {
                            listener.inFlight = false;
                            if (listeners.get(listener.gateway) != listener) {
                                return;
                            }
                            if (failure == null) {
                                listener.acknowledgedRevision = sentRevision;
                                send(listener);
                            } else {
                                LOG.debug(
                                        "Retrying eviction notification to JobMaster {}.",
                                        listener.gateway.getAddress(),
                                        failure);
                                listener.retry =
                                        executor.schedule(
                                                () -> send(listener), 1, TimeUnit.SECONDS);
                            }
                        },
                        executor);
    }

    @Override
    public void close() {
        for (JobMasterGateway gateway : new ArrayList<>(listeners.keySet())) {
            unregister(gateway);
        }
        taskManagers.clear();
    }

    private static final class Listener {
        private final JobMasterGateway gateway;
        private long acknowledgedRevision = -1;
        private boolean inFlight;
        @Nullable private ScheduledFuture<?> retry;

        private Listener(JobMasterGateway gateway) {
            this.gateway = gateway;
        }
    }
}

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

package org.apache.flink.configuration;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.docs.Documentation;

import java.time.Duration;

/** Options for cooperative TaskManager replacement without changing job parallelism. */
@Internal
public final class TaskManagerEvictionOptions {

    @Documentation.Section(Documentation.Sections.EXPERT_SCHEDULING)
    public static final ConfigOption<Boolean> ENABLED =
            ConfigOptions.key("jobmanager.taskmanager-eviction.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Enables cooperative TaskManager eviction for a single streaming job "
                                    + "in Kubernetes application mode. The platform marks a Pod with "
                                    + "flink/pending-eviction=true and must not delete it until Flink "
                                    + "releases it. Requires homogeneous TaskManagers and slots.");

    @Documentation.Section(Documentation.Sections.EXPERT_SCHEDULING)
    public static final ConfigOption<Duration> QUIET_PERIOD =
            ConfigOptions.key("jobmanager.taskmanager-eviction.batch.quiet-period")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(5))
                    .withDescription("Time without a new eviction intent before closing a batch.");

    @Documentation.Section(Documentation.Sections.EXPERT_SCHEDULING)
    public static final ConfigOption<Duration> MAX_BATCH_WAIT =
            ConfigOptions.key("jobmanager.taskmanager-eviction.batch.max-wait")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(30))
                    .withDescription(
                            "Maximum time to deliberately wait for more eviction intents, measured "
                                    + "from the first intent. Does not override the requirement to "
                                    + "prepare sufficient healthy slots before interrupting tasks.");

    @Documentation.Section(Documentation.Sections.EXPERT_SCHEDULING)
    public static final ConfigOption<Duration> MAX_CHECKPOINT_WAIT =
            ConfigOptions.key("jobmanager.taskmanager-eviction.checkpoint.max-wait")
                    .durationType()
                    .noDefaultValue()
                    .withDescription(
                            "Maximum checkpoint wait once replacement slots first become ready. "
                                    + "Defaults to the Adaptive Scheduler rescale-trigger.max-delay "
                                    + "policy. New intents do not reset this deadline. On expiry, "
                                    + "recovery uses the latest retained completed checkpoint. If none "
                                    + "exists, eviction remains blocked rather than discarding state.");

    private TaskManagerEvictionOptions() {}
}

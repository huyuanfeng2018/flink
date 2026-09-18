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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.TaskManagerEvictionOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobmaster.utils.JobMasterBuilder;
import org.apache.flink.runtime.rpc.TestingRpcService;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Exercises feature-enabled construction through the actual scheduler/slot-pool factories. */
class TaskManagerEvictionJobMasterTest {
    private final TestingRpcService rpcService = new TestingRpcService();

    @AfterEach
    void closeRpcService() throws Exception {
        rpcService.closeAsync().get(10, TimeUnit.SECONDS);
    }

    @ParameterizedTest
    @EnumSource(
            value = JobManagerOptions.SchedulerType.class,
            names = {"Default", "Adaptive"})
    void startsWithEvictionEnabled(JobManagerOptions.SchedulerType schedulerType) throws Exception {
        final Configuration configuration = new Configuration();
        configuration.set(TaskManagerEvictionOptions.ENABLED, true);
        configuration.set(JobManagerOptions.SCHEDULER, schedulerType);
        final JobGraph jobGraph = JobGraphTestUtils.singleNoOpJobGraph();
        try (JobMaster jobMaster =
                new JobMasterBuilder(jobGraph, rpcService)
                        .withConfiguration(configuration)
                        .createJobMaster()) {
            jobMaster.start();
            assertThat(
                            jobMaster
                                    .getSelfGateway(JobMasterGateway.class)
                                    .requestJobStatus(java.time.Duration.ofSeconds(10))
                                    .get(10, TimeUnit.SECONDS)
                                    .isTerminalState())
                    .isFalse();
        }
    }
}

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

package org.apache.flink.connector.hbase.util;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

/** Tests for {@link HBaseTimestampGenerator}. */
public class HBaseTimestampGeneratorTest {
    @Test
    public void testStronglyIncreasingTimestampGenerator() {
        HBaseTimestampGenerator timestampGenerator = HBaseTimestampGenerator.stronglyIncreasing();
        long lastTimestamp = 0;
        for (int i = 0; i < 100_000_000; i++) {
            final long now = timestampGenerator.get();
            if (lastTimestamp > 0) {
                assertTrue(now > lastTimestamp);
            }
            lastTimestamp = now;
        }
        final long realNow = timestampGenerator.getCurrentSystemTimeNano();
        assertTrue(
                "The increasing timestamp should not exceed the current actual timestamp after 100 million tests",
                realNow >= lastTimestamp);
    }
}
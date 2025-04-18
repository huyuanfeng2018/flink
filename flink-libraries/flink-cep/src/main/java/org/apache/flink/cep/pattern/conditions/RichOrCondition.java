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

package org.apache.flink.cep.pattern.conditions;

import org.apache.flink.annotation.Internal;

/**
 * A {@link RichIterativeCondition condition} which combines two conditions with a logical {@code
 * OR} and returns {@code true} if at least one is {@code true}.
 *
 * @param <T> Type of the element to filter
 */
@Internal
public class RichOrCondition<T> extends RichCompositeIterativeCondition<T> {

    private static final long serialVersionUID = 1L;

    public RichOrCondition(final IterativeCondition<T> left, final IterativeCondition<T> right) {
        super(left, right);
    }

    @Override
    public boolean filter(T value, Context<T> ctx) throws Exception {
        return getLeft().filter(value, ctx) || getRight().filter(value, ctx);
    }

    /**
     * @return One of the {@link IterativeCondition conditions} combined in this condition.
     */
    public IterativeCondition<T> getLeft() {
        return getNestedConditions()[0];
    }

    /**
     * @return One of the {@link IterativeCondition conditions} combined in this condition.
     */
    public IterativeCondition<T> getRight() {
        return getNestedConditions()[1];
    }
}

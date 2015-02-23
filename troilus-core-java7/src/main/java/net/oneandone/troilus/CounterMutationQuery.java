/*
 * Copyright 1&1 Internet AG, https://github.com/1and1/
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.oneandone.troilus;



import net.oneandone.troilus.java7.CounterMutation;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Statement;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;


/**
 * Counter mutation query implementation 
 *
 */
class CounterMutationQuery extends AbstractQuery<CounterMutation> implements CounterMutation {
    
    private final CounterMutationQueryData data;

    /**
     * @param ctx    the context
     * @param data   the query data
     */
    CounterMutationQuery(Context ctx, CounterMutationQueryData data) {
        super(ctx);
        this.data = data;
    }
    
    @Override
    protected CounterMutationQuery newQuery(Context newContext) {
        return new CounterMutationQuery(newContext, data);
    }
    
    
    @Override
    public CounterBatchMutationQuery combinedWith(CounterMutation other) {
        return new CounterBatchMutationQuery(getContext(), ImmutableList.of(this, other));
    }

    @Override
    public Result execute() {
        return ListenableFutures.getUninterruptibly(executeAsync());
    }
    
    @Override
    public ListenableFuture<Result> executeAsync() {
        ListenableFuture<ResultSet> future = performAsync(getStatementAsync());
        
        Function<ResultSet, Result> mapEntity = new Function<ResultSet, Result>() {
            @Override
            public Result apply(ResultSet resultSet) {
                return newResult(resultSet);
            }
        };
        
        return Futures.transform(future, mapEntity);
    }
    
    @Override
    public ListenableFuture<Statement> getStatementAsync() {
        return data.toStatementAsync(getContext());
    }
}


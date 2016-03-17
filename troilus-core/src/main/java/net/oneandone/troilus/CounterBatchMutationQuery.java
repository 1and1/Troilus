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


import java.util.concurrent.CompletableFuture;

import com.datastax.driver.core.BatchStatement.Type;
import com.datastax.driver.core.Statement;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;


 
/**
 * Counter batch mutation query 
 * 
 */
class CounterBatchMutationQuery extends MutationQuery<CounterMutation> implements CounterMutation {
    private final ImmutableList<CounterMutation> batchables;
    
    /**
     * @param ctx         the context to use
     * @param batchables  the statements to be performed within the batch
     */
    CounterBatchMutationQuery(Context ctx, ImmutableList<CounterMutation> batchables) {
        super(ctx);
        this.batchables = batchables;
    }

    
    ////////////////////
    // factory methods
    
    @Override
    protected CounterBatchMutationQuery newQuery(Context newContext) {
        return new CounterBatchMutationQuery(newContext, batchables);
    }
    
    private CounterBatchMutationQuery newQuery(ImmutableList<CounterMutation> batchables) {
        return new CounterBatchMutationQuery(getContext(), batchables);
    }

    //
    ////////////////////

    
    @Override
    public CounterBatchMutationQuery combinedWith(CounterMutation other) {
        return newQuery(Immutables.join(batchables, other));
    }
    
    @Override
    public CompletableFuture<Statement> getStatementAsync(final DBSession dbSession) {
        
        Function<CounterMutation, ListenableFuture<Statement>> statementFetcher = new Function<CounterMutation, ListenableFuture<Statement>>() {
            public ListenableFuture<Statement> apply(CounterMutation batchable) {
                return batchable.getStatementAsync(dbSession);
            };
        };
        return mergeToBatch(Type.COUNTER, batchables.iterator(), statementFetcher);
    }
}

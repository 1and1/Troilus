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



import net.oneandone.troilus.java7.BatchMutation;
import net.oneandone.troilus.java7.Mutation;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.BatchStatement.Type;
import com.datastax.driver.core.Statement;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;


 
/**
 * Batch mutation query
 */
class BatchMutationQuery extends AbstractQuery<BatchMutation> implements BatchMutation {
    private final ImmutableList<Mutation<?>> batchables;
    private final Type type;  
    
    

    BatchMutationQuery(Context ctx, Mutation<?> mutation1, Mutation<?> mutation2) {
        this(ctx, Type.LOGGED, ImmutableList.<Mutation<?>>of(mutation1, mutation2));
    }
    
    
    private BatchMutationQuery(Context ctx, Type type, ImmutableList<Mutation<?>> batchables) {
        super(ctx);
        this.type = type;
        this.batchables = batchables;
    }
    
    
    ////////////////////
    // factory methods
    
    @Override
    protected BatchMutationQuery newQuery(Context newContext) {
        return new BatchMutationQuery(newContext, type, batchables);
    }
    
    private BatchMutationQuery newQuery(Type type, ImmutableList<Mutation<?>> batchables) {
        return new BatchMutationQuery(getContext(), type, batchables);
    }
    
    //
    ///////////////////
    
    
    
    @Override
    public BatchMutationQuery withWriteAheadLog() {
        return newQuery(Type.LOGGED, batchables);
    }
    
    @Override
    public BatchMutationQuery withoutWriteAheadLog() {
        return newQuery(Type.UNLOGGED, batchables);
    }

    public BatchMutationQuery combinedWith(Mutation<?> other) {
        return newQuery(type, Immutables.join(batchables, other));
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
    
    public ListenableFuture<Statement> getStatementAsync() {
        return new BatchQueryFutureAdapter<Mutation<?>>(new BatchStatement(type), batchables.iterator());
    }
}

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




import java.util.concurrent.ExecutionException;

import net.oneandone.troilus.java7.CounterMutation;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.BatchStatement.Type;
import com.datastax.driver.core.Statement;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.UnmodifiableIterator;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

 
/**
 * Counter batch mutation query 
 * 
 */
class CounterBatchMutationQuery extends AbstractQuery<CounterMutation> implements CounterMutation {
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
        return new BatchQueryFutureAdapter(new BatchStatement(Type.COUNTER), batchables.iterator());
    }
  
    
    private static final class BatchQueryFutureAdapter extends AbstractFuture<Statement> {
        
        BatchQueryFutureAdapter(BatchStatement batchStmt, UnmodifiableIterator<CounterMutation> batchablesIt) {
            handle(batchStmt, batchablesIt);
        }
        
        
        private void handle(final BatchStatement batchStmt, final UnmodifiableIterator<CounterMutation> batchablesIt) {
            
            if (batchablesIt.hasNext()) {
                final ListenableFuture<Statement> statementFuture = batchablesIt.next().getStatementAsync();
                
                Runnable resultHandler = new Runnable() {
                    
                    @Override
                    public void run() {
                        try {
                            batchStmt.add(statementFuture.get());
                            handle(batchStmt, batchablesIt);
                        } catch (InterruptedException | ExecutionException | RuntimeException e) {
                            setException(ListenableFutures.unwrapIfNecessary(e));
                        }
                    }
                };
                statementFuture.addListener(resultHandler, MoreExecutors.directExecutor());
                
            } else {
                set(batchStmt);
            }
        }
    }        
}

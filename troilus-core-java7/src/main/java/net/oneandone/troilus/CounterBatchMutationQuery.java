/*
 * Copyright (c) 2014 1&1 Internet AG, Germany, http://www.1und1.de
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *  http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.oneandone.troilus;




import java.util.concurrent.ExecutionException;

import net.oneandone.troilus.java7.Dao.CounterBatchMutation;
import net.oneandone.troilus.java7.Dao.CounterBatchable;

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

 

class CounterBatchMutationQuery extends AbstractQuery<CounterBatchMutation> implements CounterBatchMutation {
    private final ImmutableList<CounterBatchable> batchables;
    
    
    protected CounterBatchMutationQuery(Context ctx, ImmutableList<CounterBatchable> batchables) {
        super(ctx);
        this.batchables = batchables;
    }
    
    @Override
    protected CounterBatchMutationQuery newQuery(Context newContext) {
        return new CounterBatchMutationQuery(newContext, batchables);
    }
    
        @Override
    public CounterBatchMutationQuery combinedWith(CounterBatchable other) {
        return new CounterBatchMutationQuery(getContext(), Immutables.merge(batchables, other));
    }

        
    private ListenableFuture<Statement> getStatementAsync() {
        return new QueryFuture(new BatchStatement(Type.COUNTER), batchables.iterator());
    }
        
        
    private static final class QueryFuture extends AbstractFuture<Statement> {
        
        public QueryFuture(BatchStatement batchStmt, UnmodifiableIterator<CounterBatchable> batchablesIt) {
            handle(batchStmt, batchablesIt);
        }
        
        private void handle(final BatchStatement batchStmt, final UnmodifiableIterator<CounterBatchable> batchablesIt) {
            
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
}

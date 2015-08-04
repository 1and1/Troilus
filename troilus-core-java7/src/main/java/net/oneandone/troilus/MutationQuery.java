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




import java.util.Set;
import java.util.concurrent.ExecutionException;

import net.oneandone.troilus.java7.Batchable;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.BatchStatement.Type;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.collect.UnmodifiableIterator;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;



/**
 * abstract mutation query implementation
 */
abstract class MutationQuery<Q> extends AbstractQuery<Q> {
    
    
    /**
     * @param ctx   the context
     */
    MutationQuery(Context ctx) {
        super(ctx);
    }
    
    public Result execute() {
        return ListenableFutures.getUninterruptibly(executeAsync());
    }
    
    public ListenableFuture<Result> executeAsync() {
        ListenableFuture<ResultSet> future = performAsync(getContext().getDbSession(), getStatementAsync(getContext()));
        
        Function<ResultSet, Result> mapEntity = new Function<ResultSet, Result>() {
            @Override
            public Result apply(ResultSet resultSet) {
                return newResult(resultSet);
            }
        };
        
        return Futures.transform(future, mapEntity);
    }
    
    
    public abstract ListenableFuture<Statement> getStatementAsync(final Context ctx);
    
    
    protected ListenableFuture<Statement> mergeStatements(Context ctx, ListenableFuture<Statement> statementFuture, ListenableFuture<ImmutableSet<Statement>> cascadingStatmentsFuture) {
        ListenableFuture<ImmutableSet<Statement>> statementsFuture = ListenableFutures.join(cascadingStatmentsFuture, statementFuture, ctx.getTaskExecutor());

        Function<ImmutableSet<Statement>, Statement> statementsBatcher = new Function<ImmutableSet<Statement>, Statement>() {
            
            public Statement apply(ImmutableSet<Statement> statements) {
                BatchStatement batchStatement = new BatchStatement(Type.LOGGED);
                for (Statement statement : statements) {
                    batchStatement.add(statement);
                }       
                return batchStatement;
            };
        };
        return Futures.transform(statementsFuture, statementsBatcher);
    }
    
    
    protected ListenableFuture<ImmutableSet<Statement>> transformBatchablesToStatement(Context ctx, ListenableFuture<ImmutableSet<? extends Batchable<?>>> batchablesFutureSet) {
                    
        Function<ImmutableSet<? extends Batchable<?>>, ImmutableSet<ListenableFuture<Statement>>> batchablesToStatement = new Function<ImmutableSet<? extends Batchable<?>>, ImmutableSet<ListenableFuture<Statement>>>() {                
            @Override
            public ImmutableSet<ListenableFuture<Statement>> apply(ImmutableSet<? extends Batchable<?>> batchables) {
                Set<ListenableFuture<Statement>> statementFutureSet = Sets.newHashSet();
                for(Batchable<?> batchable : batchables) {
                    statementFutureSet.add(batchable.getStatementAsync(getContext()));
                }
                return ImmutableSet.copyOf(statementFutureSet);                    
            }
        };            
        ListenableFuture<ImmutableSet<ListenableFuture<Statement>>> statementFutureSet = Futures.transform(batchablesFutureSet, batchablesToStatement);
        return ListenableFutures.flat(statementFutureSet, ctx.getTaskExecutor());
    }
    
    
    protected <T> ListenableFuture<Statement> mergeToBatch(Type batchType, UnmodifiableIterator<T> batchablesIt, Function<T, ListenableFuture<Statement>> statementFetcher) {
        return new BatchQueryFutureAdapter<>(new BatchStatement(batchType), batchablesIt, statementFetcher);
    }
    
    
    protected static final class BatchQueryFutureAdapter<T> extends AbstractFuture<Statement> {
        
        BatchQueryFutureAdapter(BatchStatement batchStmt, UnmodifiableIterator<T> batchablesIt, Function<T, ListenableFuture<Statement>> statementFetcher) {
            handle(batchStmt, batchablesIt, statementFetcher);
        }
        
        private void handle(final BatchStatement batchStmt, final UnmodifiableIterator<T> batchablesIt, final Function<T, ListenableFuture<Statement>> statementFetcher) {
            
            if (batchablesIt.hasNext()) {
                final ListenableFuture<Statement> statementFuture = statementFetcher.apply(batchablesIt.next());
                
                Runnable resultHandler = new Runnable() {
                    
                    @Override
                    public void run() {
                        try {
                            batchStmt.add(statementFuture.get());
                            handle(batchStmt, batchablesIt, statementFetcher);
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
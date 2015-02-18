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

import net.oneandone.troilus.java7.Batchable;
import net.oneandone.troilus.java7.CombinableMutation;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.BatchStatement.Type;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;



/**
 * abstract mutation query implementation
 */
abstract class MutationQuery<Q> extends AbstractQuery<Q> implements Batchable, CombinableMutation {
    
    
    /**
     * @param ctx   the context
     */
    MutationQuery(Context ctx) {
        super(ctx);
    }
    
    public BatchMutationQuery combinedWith(Batchable other) {
        return new BatchMutationQuery(getContext(), Type.LOGGED, ImmutableList.of(this, other));
    }
  
    
    public Result execute() {
        return ListenableFutures.getUninterruptibly(executeAsync());
    }
    
    public abstract ListenableFuture<Result> executeAsync();
    
    
    protected ListenableFuture<Statement> mergeStatements(ListenableFuture<Statement> statementFuture, ListenableFuture<ImmutableSet<Statement>> cascadingStatmentsFuture) {
        ListenableFuture<ImmutableSet<Statement>> statementsFuture = ListenableFutures.join(cascadingStatmentsFuture, statementFuture, getContext().getTaskExecutor());

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
    
    
    protected ListenableFuture<ImmutableSet<Statement>> transformBatchablesToStatement(ListenableFuture<ImmutableSet<? extends Batchable>> batchablesFutureSet) {
                    
        Function<ImmutableSet<? extends Batchable>, ImmutableSet<ListenableFuture<Statement>>> batchablesToStatement = new Function<ImmutableSet<? extends Batchable>, ImmutableSet<ListenableFuture<Statement>>>() {                
            @Override
            public ImmutableSet<ListenableFuture<Statement>> apply(ImmutableSet<? extends Batchable> batchables) {
                Set<ListenableFuture<Statement>> statementFutureSet = Sets.newHashSet();
                for(Batchable batchable : batchables) {
                    statementFutureSet.add(batchable.getStatementAsync());
                }
                return ImmutableSet.copyOf(statementFutureSet);                    
            }
        };            
        ListenableFuture<ImmutableSet<ListenableFuture<Statement>>> statementFutureSet = Futures.transform(batchablesFutureSet, batchablesToStatement);
        return ListenableFutures.flat(statementFutureSet, getContext().getTaskExecutor());
    }
}
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


import net.oneandone.troilus.AbstractQuery;
import net.oneandone.troilus.Context;
import net.oneandone.troilus.Result;
import net.oneandone.troilus.java7.BatchMutation;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.policies.RetryPolicy;
import com.google.common.util.concurrent.ListenableFuture;


/**
 * Java8 adapter base for mutation operations
 */
abstract class MutationQueryAdapter<Q, T extends MutationQuery<?>> extends AbstractQuery<Q> { 
    
    private final T query;
  
    /**
     * @param ctx    the context
     * @param query  the query
     */
    MutationQueryAdapter(Context ctx, T query) {
        super(ctx);
        this.query = query;
    }
    
    /**
     * @return  the query
     */
    protected T getQuery() {
        return query;
    }

    /**
     * performs the query in a sync way 
     * @return the result
     */
    public Result execute() {
        return CompletableFutures.getUninterruptibly(executeAsync());
    }
    
    /**
     * performs the query in an async way 
     * @return the result future 
     */
    public CompletableFuture<Result> executeAsync() {
        return CompletableFutures.toCompletableFuture(query.executeAsync());
    }
    
    public CompletableFuture<Statement> getStatementAsync() {
        return CompletableFutures.toCompletableFuture(query.getStatementAsync());
    }
    
    @Override
    public String toString() {
        return query.toString();
    }
    
    
    /**
     * @param batchable the batchable to map
     * @return the mapped batchable
     */
    public static net.oneandone.troilus.java7.Mutation<?> toJava7Mutation(Mutation<?> mutation) {
        return new MutationToJava7MutationAdapter(mutation);
    }
    
    @SuppressWarnings("rawtypes")
    private static class MutationToJava7MutationAdapter implements net.oneandone.troilus.java7.Mutation {
        private final Mutation<?> mutation;
        
        public MutationToJava7MutationAdapter(Mutation<?> mutation) {
            this.mutation = mutation;
        }

        @Override
        public Object withConsistency(ConsistencyLevel consistencyLevel) {
            return mutation.withConsistency(consistencyLevel);
        }
        
        @Override
        public Object withoutTracking() {
            return mutation.withoutTracking();
        }
        
        @Override
        public Object withRetryPolicy(RetryPolicy policy) {
            return mutation.withRetryPolicy(policy);
        }
        
        @Override
        public Object withSerialConsistency(ConsistencyLevel consistencyLevel) {
            return mutation.withSerialConsistency(consistencyLevel);
        }
        
        @Override
        public Object withTracking() {
            return mutation.withTracking();
        }
        
        @Override
        public BatchMutation combinedWith(net.oneandone.troilus.java7.Mutation other) {
            // TODO Auto-generated method stub
            return null;
        }
        
        @Override
        public Object execute() {
            return mutation.execute();
        }
        
        @Override
        public ListenableFuture executeAsync() {
            return CompletableFutures.toListenableFuture(mutation.executeAsync());
        }
        
        @Override
        public ListenableFuture<Statement> getStatementAsync() {
            return CompletableFutures.toListenableFuture(mutation.getStatementAsync());
        }
    }
}

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


import java.util.concurrent.Executor;

import net.oneandone.troilus.Context.DBSession;
import net.oneandone.troilus.java7.BatchMutation;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.policies.RetryPolicy;
import com.google.common.util.concurrent.ListenableFuture;


class Mutations { 
    
    private Mutations() {  }
    
    
    /**
     * @param batchable the batchable to map or null
     * @return the mapped batchable or null if mutation is null
     */
    public static net.oneandone.troilus.java7.Batchable<?> toJava7Mutation(Batchable<?> mutation) {
        if (mutation == null) {
            return null;
        } else {
            return new MutationToJava7MutationAdapter(mutation);
        }
    }
    
    @SuppressWarnings("rawtypes")
    private static class MutationToJava7MutationAdapter implements net.oneandone.troilus.java7.Batchable {
        private final Batchable<?> mutation;
        
        public MutationToJava7MutationAdapter(Batchable<?> mutation) {
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
        public BatchMutation combinedWith(net.oneandone.troilus.java7.Batchable other) {
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
        public ListenableFuture getStatementAsync(DBSession dbSession) {
            return CompletableFutures.toListenableFuture(mutation.getStatementAsync(dbSession));
        }
    }
}

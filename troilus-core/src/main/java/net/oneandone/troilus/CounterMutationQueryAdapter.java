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


import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import net.oneandone.troilus.Context;
import net.oneandone.troilus.Result;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.policies.RetryPolicy;
import com.google.common.util.concurrent.ListenableFuture;


/**
 * Java8 adapter of a CounterMutationQuery
 */
 class CounterMutationQueryAdapter extends BatchQueryAdapter<CounterMutation> implements CounterMutation {
        
     private final CounterMutationQuery query;
     
     /**
      * @param ctx     the context
      * @param query   the underlying query
      */
     CounterMutationQueryAdapter(Context ctx, CounterMutationQuery query) {
         super(ctx, query);
         this.query = query;
     }
     
     @Override
     protected CounterMutation newQuery(Context newContext) {
         return new CounterMutationQueryAdapter(newContext, query.newQuery(newContext));
     }
     
     @Override
     public CounterMutation combinedWith(CounterMutation other) {
         return new CounterBatchMutationQueryAdapter(getContext(), query.combinedWith(toJava7CounterMutation(other)));
     }

     
     static net.oneandone.troilus.java7.CounterMutation toJava7CounterMutation(CounterMutation mutation) {
         return new CounterMutationToJava7CounterMutationAdapter(mutation);
     }
     
     private static class CounterMutationToJava7CounterMutationAdapter implements net.oneandone.troilus.java7.CounterMutation {
         private final CounterMutation mutation;
         
         public CounterMutationToJava7CounterMutationAdapter(CounterMutation mutation) {
             this.mutation = mutation;
         }

         @Override
         public net.oneandone.troilus.java7.CounterMutation withConsistency(ConsistencyLevel consistencyLevel) {
             return (net.oneandone.troilus.java7.CounterMutation) mutation.withConsistency(consistencyLevel);
         }
         
         @Override
         public net.oneandone.troilus.java7.CounterMutation withoutTracking() {
             return (net.oneandone.troilus.java7.CounterMutation) mutation.withoutTracking();
         }
         
         @Override
         public net.oneandone.troilus.java7.CounterMutation withRetryPolicy(RetryPolicy policy) {
             return (net.oneandone.troilus.java7.CounterMutation) mutation.withRetryPolicy(policy);
         }
         
         @Override
         public net.oneandone.troilus.java7.CounterMutation withSerialConsistency(ConsistencyLevel consistencyLevel) {
             return (net.oneandone.troilus.java7.CounterMutation) mutation.withSerialConsistency(consistencyLevel);
         }
         
         @Override
         public net.oneandone.troilus.java7.CounterMutation withTracking() {
             return (net.oneandone.troilus.java7.CounterMutation) mutation.withTracking();
         }

         @Override
         public net.oneandone.troilus.java7.CounterMutation withWritetime(long microsSinceEpoch) {
             return (net.oneandone.troilus.java7.CounterMutation) mutation.withWritetime(microsSinceEpoch);
         }
         
         @Override
        public net.oneandone.troilus.java7.CounterMutation withTtl(int ttlSec) {
             return (net.oneandone.troilus.java7.CounterMutation) mutation.withTtl(Duration.ofSeconds(ttlSec));
        }
         
         @Override
         public net.oneandone.troilus.java7.CounterMutation combinedWith(net.oneandone.troilus.java7.CounterMutation other) {
             return toJava7CounterMutation(mutation.combinedWith(fromJava7CounterMutation(other)));
         }

         @Override
         public Result execute() {
             return mutation.execute();
         }
         
         @Override
        public ListenableFuture<Result> executeAsync() {
             return CompletableFutures.toListenableFuture(mutation.executeAsync());
         }
         
         @Override
         public ListenableFuture<Statement> getStatementAsync() {
             return CompletableFutures.toListenableFuture(mutation.getStatementAsync());
         }
         
         
         /**
          * @param batchable the batchable to map
          * @return the mapped batchable
          */
         private static CounterMutation fromJava7CounterMutation(net.oneandone.troilus.java7.CounterMutation mutation) {
             return new Java7CounterMutationToCounterMutationAdapter(mutation);
         }
         
         private static class Java7CounterMutationToCounterMutationAdapter implements CounterMutation {
             private final net.oneandone.troilus.java7.CounterMutation mutation;
             
             public Java7CounterMutationToCounterMutationAdapter(net.oneandone.troilus.java7.CounterMutation mutation) {
                 this.mutation = mutation;
             }

             @Override
             public CounterMutation withConsistency(ConsistencyLevel consistencyLevel) {
                 return (CounterMutation) mutation.withConsistency(consistencyLevel);
             }
             
             @Override
             public CounterMutation withoutTracking() {
                 return (CounterMutation) mutation.withoutTracking();
             }
             
             @Override
             public CounterMutation withRetryPolicy(RetryPolicy policy) {
                 return (CounterMutation) mutation.withRetryPolicy(policy);
             }
             
             @Override
             public CounterMutation withSerialConsistency(ConsistencyLevel consistencyLevel) {
                 return (CounterMutation) mutation.withSerialConsistency(consistencyLevel);
             }
             
             @Override
             public CounterMutation withTracking() {
                 return (CounterMutation) mutation.withTracking();
             }

             @Override
             public CounterMutation withWritetime(long microsSinceEpoch) {
                 return (CounterMutation) mutation.withWritetime(microsSinceEpoch);
             }

             @Override
             public CounterMutation withTtl(Duration ttl) {
                 return (CounterMutation) mutation.withTtl((int) ttl.getSeconds());
            }
             
             @Override
             public CounterMutation combinedWith(CounterMutation other) {
                 return fromJava7CounterMutation(mutation.combinedWith(toJava7CounterMutation(other)));
             }

             @Override
             public Result execute() {
                 return mutation.execute();
             }
             
             @Override
             public CompletableFuture<Result> executeAsync() {
                 return CompletableFutures.toCompletableFuture(mutation.executeAsync());
             }

             @Override
             public CompletableFuture<Statement> getStatementAsync() {
                 return CompletableFutures.toCompletableFuture(mutation.getStatementAsync());
             }
         }
     }
 }



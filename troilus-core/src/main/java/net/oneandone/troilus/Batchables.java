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

import com.datastax.driver.core.Statement;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;

import net.oneandone.troilus.java7.Batch;



class Batchables {
    
    private Batchables() {  }
      
    /**
     * @param batchable the batchable to map
     * @return the mapped batchable
     */
    public static net.oneandone.troilus.java7.Batchable toJava7Batchable(Context ctx, Batchable batchable) {
        return new BatchableToJava7BatchableAdapter(ctx, batchable);
    }
    
    private static class BatchableToJava7BatchableAdapter implements net.oneandone.troilus.java7.Batchable {
        private final Batchable batchable;
        private final Context ctx;
        
        public BatchableToJava7BatchableAdapter(Context ctx, Batchable batchable) {
            this.ctx = ctx;
            this.batchable = batchable;
        }
              
        @Override
        public ListenableFuture<Statement> getStatementAsync() {
            return CompletableFutures.toListenableFuture(batchable.getStatementAsync());
        }
        
        @Override
        public Batch combinedWith(net.oneandone.troilus.java7.Batchable other) {
            return new BatchMutationQuery(ctx, ImmutableList.of(other));
        }
    }
}
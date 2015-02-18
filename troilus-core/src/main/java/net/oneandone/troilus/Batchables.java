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
import com.google.common.util.concurrent.ListenableFuture;


/**
 * Batchable utility class
 */
class Batchables {
    
    private Batchables() {  }
      
    /**
     * @param batchable the batchable to map
     * @return the mapped batchable
     */
    public static net.oneandone.troilus.java7.Batchable toJava7Batchable(Batchable batchable) {
        return new BatchableToJava7BatchableAdapter(batchable);
    }
    
    private static class BatchableToJava7BatchableAdapter implements net.oneandone.troilus.java7.Batchable {
        private final Batchable batchable;
        
        public BatchableToJava7BatchableAdapter(Batchable batchable) {
            this.batchable = batchable;
        }
        
        @Override
        public ListenableFuture<Statement> getStatementAsync() {
            return CompletableFutures.toListenableFuture(batchable.getStatementAsync());
        }
    }
}

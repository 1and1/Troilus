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

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Statement;
import com.google.common.collect.UnmodifiableIterator;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

 

class BatchQueryFuture<T extends StatementSource> extends AbstractFuture<Statement> {
        
    public BatchQueryFuture(BatchStatement batchStmt, UnmodifiableIterator<T> batchablesIt) {
        handle(batchStmt, batchablesIt);
    }
    
    private void handle(final BatchStatement batchStmt, final UnmodifiableIterator<T> batchablesIt) {
        
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
    

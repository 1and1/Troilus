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
package com.unitedinternet.troilus;


import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;


import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BatchStatement.Type;
import com.datastax.driver.core.Statement;
import com.google.common.collect.ImmutableList;
import com.unitedinternet.troilus.Dao.BatchMutation;
import com.unitedinternet.troilus.Dao.Batchable;
import com.unitedinternet.troilus.Dao.Insertion;
import com.unitedinternet.troilus.Dao.Mutation;
import com.unitedinternet.troilus.interceptor.WriteQueryPreInterceptor;


 
class InsertionQuery extends AbstractQuery<Insertion> implements Insertion {
    
    private final WriteQueryData data;
  
    public InsertionQuery(Context ctx, WriteQueryData data) {
        super(ctx);
        this.data = data;
    }
    
    
    @Override
    protected InsertionQuery newQuery(Context newContext) {
        return new InsertionQuery(newContext, data);
    }
    
    public InsertionQuery withTtl(Duration ttl) {
        return newQuery(getContext().withTtl(ttl.getSeconds()));
    }
    
    public BatchMutation combinedWith(Batchable other) {
        return new BatchMutationQuery(getContext(), Type.LOGGED, ImmutableList.of(this, other));
    }
       
    @Override
    public void addTo(BatchStatement batchStatement) {
        batchStatement.add(getStatement());
    }

    
    @Override
    public Mutation<?> ifNotExits() {
        return new InsertionQuery(getContext(), data.ifNotExists(Optional.of(true)));
    }

  
    private Statement getStatement() {
        WriteQueryData queryData = data;
        for (WriteQueryPreInterceptor interceptor : getContext().getInterceptorRegistry().getInterceptors(WriteQueryPreInterceptor.class)) {
            queryData = interceptor.onPreWrite(queryData); 
        }
        
        return queryData.toStatement(getContext());
    }
    
    
    @Override
    public CompletableFuture<Result> executeAsync() {
        return new CompletableDbFuture(performAsync(getStatement()))
                        .thenApply(resultSet -> newResult(resultSet))
                        .thenApply(result -> {
                                                data.getIfNotExits().ifPresent(ifNotExists -> {
                                                    // check cas result column '[applied]'
                                                    if (ifNotExists && !result.wasApplied()) {
                                                        throw new IfConditionException("duplicated entry");  
                                                    }                                                                 
                                                });
                    
                                                return result;

                                             });
    }
}

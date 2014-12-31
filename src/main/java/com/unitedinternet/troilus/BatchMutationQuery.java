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



import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BatchStatement.Type;

import com.datastax.driver.core.Statement;
import com.google.common.collect.ImmutableList;


 

class BatchMutationQuery implements BatchMutation {
    private final Context ctx;
    private final QueryFactory queryFactory;
    private final ImmutableList<Batchable> batchables;
    private final Type type;  
    
    
    public BatchMutationQuery(Context ctx, QueryFactory queryFactory, Type type, ImmutableList<Batchable> batchables) {
        this.ctx = ctx;
        this.queryFactory = queryFactory;
        this.type = type;
        this.batchables = batchables;
    }
            
    @Override
    public BatchMutation withEnableTracking() {
        return queryFactory.newBatchMutation(ctx.withEnableTracking(), type, batchables);
    }
    
    @Override
    public BatchMutation withDisableTracking() {
        return queryFactory.newBatchMutation(ctx.withDisableTracking(), type, batchables);
    }
    
    
    @Override
    public Query<Result> withLockedBatchType() {
        return queryFactory.newBatchMutation(ctx, Type.LOGGED, batchables);
    }
    
    @Override
    public Query<Result> withUnlockedBatchType() {
        return queryFactory.newBatchMutation(ctx, Type.UNLOGGED, batchables);
    }
    
     
    @Override
    public BatchMutation combinedWith(Mutation<?> other) {
        return queryFactory.newBatchMutation(ctx, type, Immutables.merge(batchables, other));
    }
    
    @Override
    public Statement getStatement() {
        BatchStatement batchStmt = new BatchStatement(type);
        batchables.forEach(mutation -> batchStmt.add(((Batchable) mutation).getStatement()));
        return batchStmt;
    }
    
    public Result execute() {
        try {
            return executeAsync().get(Long.MAX_VALUE, TimeUnit.DAYS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw Exceptions.unwrapIfNecessary(e);
        } 
    }
    
    public CompletableFuture<Result> executeAsync() {
        return ctx.performAsync(getStatement()).thenApply(resultSet -> Result.newResult(resultSet));
    }
}

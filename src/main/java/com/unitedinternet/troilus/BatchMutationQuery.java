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

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BatchStatement.Type;
import com.datastax.driver.core.Statement;
import com.google.common.collect.ImmutableList;
import com.unitedinternet.troilus.Dao.BatchMutation;
import com.unitedinternet.troilus.Dao.Batchable;
import com.unitedinternet.troilus.Dao.Query;


 

class BatchMutationQuery extends MutationQuery<BatchMutation> implements BatchMutation {
    private final ImmutableList<Batchable> batchables;
    private final Type type;  
    
    
  
    protected BatchMutationQuery(Context ctx, QueryFactory queryFactory, Type type, ImmutableList<Batchable> batchables) {
        super(ctx, queryFactory);
        this.type = type;
        this.batchables = batchables;
    }
    
    @Override
    protected BatchMutation newQuery(Context newContext) {
        return newBatchMutationQuery(newContext, type, batchables);
    }
    
    
    @Override
    public Query<Result> withLockedBatchType() {
        return newBatchMutationQuery(Type.LOGGED, batchables);
    }
    
    @Override
    public Query<Result> withUnlockedBatchType() {
        return newBatchMutationQuery(Type.UNLOGGED, batchables);
    }

    @Override
    public BatchMutation combinedWith(Batchable other) {
        return newBatchMutationQuery(type, Immutables.merge(batchables, other));
    }

    @Override
    public Statement getStatement() {
        BatchStatement batchStmt = new BatchStatement(type);
        batchables.forEach(batchable -> batchable.addTo(batchStmt));
        return batchStmt;
    }
    
    
    public CompletableFuture<Result> executeAsync() {
        return performAsync(getStatement()).thenApply(resultSet -> new ResultImpl(resultSet));
    }
}

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
import com.unitedinternet.troilus.Dao.CounterBatchMutation;
import com.unitedinternet.troilus.Dao.CounterBatchable;


 

class CounterBatchMutationQuery extends MutationQuery<CounterBatchMutation> implements CounterBatchMutation {
    private final ImmutableList<CounterBatchable> batchables;
    
    
    protected CounterBatchMutationQuery(Context ctx, ImmutableList<CounterBatchable> batchables) {
        super(ctx);
        this.batchables = batchables;
    }
    
    @Override
    protected CounterBatchMutation newQuery(Context newContext) {
        return new CounterBatchMutationQuery(newContext, batchables);
    }
    
        @Override
    public CounterBatchMutation combinedWith(CounterBatchable other) {
        return new CounterBatchMutationQuery(getContext(), Immutables.merge(batchables, other));
    }

    @Override
    public Statement getStatement(Context ctx) {
        BatchStatement batchStmt = new BatchStatement(Type.COUNTER);
        batchables.forEach(batchable -> batchable.addTo(batchStmt));
        return batchStmt;
    }
    
    
    public CompletableFuture<Result> executeAsync() {
        return getContext().performAsync(getStatement(getContext())).thenApply(resultSet -> new ResultImpl(resultSet));
    }
}

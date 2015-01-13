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



import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BatchStatement.Type;
import com.datastax.driver.core.Statement;
import com.google.common.collect.ImmutableList;
import com.unitedinternet.troilus.minimal.MinimalDao.BatchMutation;
import com.unitedinternet.troilus.minimal.MinimalDao.Batchable;
import com.unitedinternet.troilus.minimal.MinimalDao.Query;


 

class MinimalBatchMutationQuery extends AbstractQuery<BatchMutation> implements BatchMutation {
    private final ImmutableList<Batchable> batchables;
    private final Type type;  
    
    
    MinimalBatchMutationQuery(Context ctx, Type type, ImmutableList<Batchable> batchables) {
        super(ctx);
        this.type = type;
        this.batchables = batchables;
    }
    
    @Override
    protected BatchMutation newQuery(Context newContext) {
        return new MinimalBatchMutationQuery(newContext, type, batchables);
    }
    
    @Override
    public Query<Result> withLockedBatchType() {
        return new MinimalBatchMutationQuery(getContext(), Type.LOGGED, batchables);
    }
    
    @Override
    public Query<Result> withUnlockedBatchType() {
        return new MinimalBatchMutationQuery(getContext(), Type.UNLOGGED, batchables);
    }

    @Override
    public BatchMutation combinedWith(Batchable other) {
        return new MinimalBatchMutationQuery(getContext(), type, Immutables.merge(batchables, other));
    }

    private Statement getStatement() {
        BatchStatement batchStmt = new BatchStatement(type);
        for (Batchable batchable : batchables) {
            batchable.addTo(batchStmt);
        }
        return batchStmt;
    }
    
    @Override
    public Result execute() {
        return newResult(performAsync(getStatement()).getUninterruptibly());
    }
}

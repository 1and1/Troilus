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
import com.unitedinternet.troilus.minimal.MinimalDao.CounterBatchMutation;
import com.unitedinternet.troilus.minimal.MinimalDao.CounterBatchable;

 

class MinimalCounterBatchMutationQuery extends AbstractQuery<CounterBatchMutation> implements CounterBatchMutation {
    private final ImmutableList<CounterBatchable> batchables;
    
    
    protected MinimalCounterBatchMutationQuery(Context ctx, ImmutableList<CounterBatchable> batchables) {
        super(ctx);
        this.batchables = batchables;
    }
    
    @Override
    protected MinimalCounterBatchMutationQuery newQuery(Context newContext) {
        return new MinimalCounterBatchMutationQuery(newContext, batchables);
    }
    
        @Override
    public CounterBatchMutation combinedWith(CounterBatchable other) {
        return new MinimalCounterBatchMutationQuery(getContext(), MinimalImmutables.merge(batchables, other));
    }

    private Statement getStatement() {
        BatchStatement batchStmt = new BatchStatement(Type.COUNTER);
        
        for (CounterBatchable batchable : batchables) {
            batchable.addTo(batchStmt);
        }
        
        return batchStmt;
    }
    
    @Override
    public Result execute() {
        return newResult(performAsync(getStatement()).getUninterruptibly());
    }
}

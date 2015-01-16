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
import com.unitedinternet.troilus.Dao.CounterBatchMutation;
import com.unitedinternet.troilus.Dao.CounterBatchable;


 

/**
 * Java8 adapter of a CounterBatchMutationQuery
 */
class CounterBatchMutationQueryAdapter extends AbstractQuery<CounterBatchMutation> implements CounterBatchMutation {
    
    private final CounterBatchMutationQuery query;
    
    
    /**
     * @param ctx     the context 
     * @param query   the underlying query
     */
    CounterBatchMutationQueryAdapter(Context ctx, CounterBatchMutationQuery query) {
        super(ctx);
        this.query = query;
    }
    
    
    @Override
    protected CounterBatchMutation newQuery(Context newContext) {
        return new CounterBatchMutationQueryAdapter(newContext, query.newQuery(newContext));
    }
    
    @Override
    public CounterBatchMutation combinedWith(CounterBatchable other) {
        return new CounterBatchMutationQueryAdapter(getContext(), query.combinedWith(new CounterBatchableAdapter(other)));
    }
    
    @Override
    public Result execute() {
        return getUninterruptibly(executeAsync());
    }
    
    @Override
    public CompletableFuture<Result> executeAsync() {
        return new ListenableToCompletableFutureAdapter<>(query.executeAsync());
    }
    


    /**
     * Java8 adapter of a CounterBatchable
     */
    static final class CounterBatchableAdapter implements com.unitedinternet.troilus.java7.Dao.CounterBatchable {
        private final CounterBatchable batchable;
        
        /**
         * @param batchable the underlying batchable
         */
        CounterBatchableAdapter(CounterBatchable batchable) {
            this.batchable = batchable;
        }
        
        @Override
        public void addTo(BatchStatement batchStatement) {
            batchable.addTo(batchStatement);
        }
    }
}

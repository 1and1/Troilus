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
import com.unitedinternet.troilus.minimal.MinimalDao.Insertion;
import com.unitedinternet.troilus.minimal.MinimalDao.Mutation;
import com.unitedinternet.troilus.minimal.WriteQueryData;
import com.unitedinternet.troilus.minimal.WriteQueryPreInterceptor;


 
class MinimalInsertQuery extends AbstractQuery<Insertion> implements Insertion {
    
    private final WriteQueryDataImpl data;
  
    public MinimalInsertQuery(Context ctx, WriteQueryDataImpl data) {
        super(ctx);
        this.data = data;
    }
    
    
    @Override
    protected MinimalInsertQuery newQuery(Context newContext) {
        return new MinimalInsertQuery(newContext, data);
    }
    
    public MinimalInsertQuery withTtl(int ttlSec) {
        return newQuery(getContext().withTtl(ttlSec));
    }
    
    public BatchMutation combinedWith(Batchable other) {
        return new MinimalBatchMutationQuery(getContext(), Type.LOGGED, ImmutableList.of(this, other));
    }
       
    @Override
    public void addTo(BatchStatement batchStatement) {
        batchStatement.add(getStatement());
    }

    
    @Override
    public Mutation<?> ifNotExits() {
        return new MinimalInsertQuery(getContext(), data.ifNotExists(true));
    }

  
    private Statement getStatement() {
        WriteQueryData queryData = data;
        for (WriteQueryPreInterceptor interceptor : getContext().getInterceptorRegistry().getInterceptors(WriteQueryPreInterceptor.class)) {
            queryData = interceptor.onPreWrite(queryData); 
        }
        
        return WriteQueryDataImpl.toStatement(queryData, getContext());
    }
    
    @Override
    public Result execute() {
        Result result = newResult(performAsync(getStatement()).getUninterruptibly());
        assertResultIsAppliedWhen((data.getIfNotExits() != null) && (data.getIfNotExits()), result, "duplicated entry");
        
        return result;
    }
}

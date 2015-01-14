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
import java.util.concurrent.CompletableFuture;

import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BatchStatement.Type;
import com.datastax.driver.core.Statement;
import com.google.common.collect.ImmutableList;
import com.unitedinternet.troilus.Dao.BatchMutation;
import com.unitedinternet.troilus.Dao.Batchable;
import com.unitedinternet.troilus.Dao.Deletion;
import com.unitedinternet.troilus.interceptor.DeleteQueryData;
import com.unitedinternet.troilus.interceptor.DeleteQueryPreInterceptor;



 
class DeleteQuery extends AbstractQuery<Deletion> implements Deletion {

    private final DeleteQueryData data;
      
    protected DeleteQuery(Context ctx, DeleteQueryData data) {
        super(ctx);
        this.data = data;
    }


    public Deletion withTtl(Duration ttl) {
        return newQuery(getContext().withTtl((int) ttl.getSeconds()));
    }
    
    public BatchMutation combinedWith(Batchable other) {
        return new BatchMutationQuery(getContext(), Type.LOGGED, ImmutableList.of(this, other));
    }
       
    @Override
    public void addTo(BatchStatement batchStatement) {
        batchStatement.add(getStatement());
    }


    @Override
    protected Deletion newQuery(Context newContext) {
        return new DeleteQuery(newContext, data);
    }
    
    
    @Override
    public Deletion onlyIf(Clause... onlyIfConditions) {
        return new DeleteQuery(getContext(), data.onlyIfConditions(ImmutableList.copyOf(onlyIfConditions)));
    }
    
   
    private Statement getStatement() {
        DeleteQueryData queryData = data;
        for (DeleteQueryPreInterceptor interceptor : getContext().getInterceptorRegistry().getInterceptors(DeleteQueryPreInterceptor.class)) {
            queryData = interceptor.onPreDelete(queryData); 
        }

        return DeleteQueryDataImpl.toStatement(queryData, getContext());
    }
    
    public CompletableFuture<Result> executeAsync() {
        return new CompletableDbFuture(performAsync(getStatement()))
                        .thenApply(resultSet -> newResult(resultSet))
                        .thenApply(result -> assertResultIsAppliedWhen(!data.getOnlyIfConditions().isEmpty(), result, "if condition does not match"));
    }
}
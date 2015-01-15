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
import com.unitedinternet.troilus.Dao.BatchMutation;
import com.unitedinternet.troilus.Dao.Batchable;
import com.unitedinternet.troilus.Dao.Deletion;


 
class DeleteQueryAdapter extends AbstractQuery<Deletion> implements Deletion {

    private final DeleteQuery query;
      
    protected DeleteQueryAdapter(Context ctx, DeleteQuery query) {
        super(ctx);
        this.query = query;
    }
    
    
    @Override
    protected Deletion newQuery(Context newContext) {
        return new DeleteQueryAdapter(newContext, query.newQuery(newContext));
    }
    
    @Override
    public Deletion withTtl(Duration ttl) {
        return newQuery(getContext().withTtl((int) ttl.getSeconds()));
    }

    @Override
    public Deletion onlyIf(Clause... onlyIfConditions) {
        return new DeleteQueryAdapter(getContext(), query.onlyIf(onlyIfConditions));
    }
    
    public BatchMutation combinedWith(Batchable other) {
        return new BatchMutationQueryAdapter(getContext(), query.combinedWith(new BatchMutationQueryAdapter.BatchableAdapter(other)));
    }
       
    @Override
    public void addTo(BatchStatement batchStatement) {
        query.addTo(batchStatement);
    }
    
    public CompletableFuture<Result> executeAsync() {
        return new ListenableToCompletableFutureAdapter<>(query.executeAsync());
    }
    
    @Override
    public String toString() {
        return query.toString();
    }
}
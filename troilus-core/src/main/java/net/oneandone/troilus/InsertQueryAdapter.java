/*
 * Copyright (c) 2015 1&1 Internet AG, Germany, http://www.1und1.de
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
package net.oneandone.troilus;


import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import net.oneandone.troilus.AbstractQuery;
import net.oneandone.troilus.Context;
import net.oneandone.troilus.InsertQuery;
import net.oneandone.troilus.Result;
import net.oneandone.troilus.Dao.BatchMutation;
import net.oneandone.troilus.Dao.Batchable;
import net.oneandone.troilus.Dao.Insertion;
import net.oneandone.troilus.Dao.Mutation;

import com.datastax.driver.core.Statement;
import com.google.common.util.concurrent.ListenableFuture;

 


/**
 * Java8 adapter of a InsertQuery
 */
class InsertQueryAdapter extends AbstractQuery<Insertion> implements Insertion {
    
    private final InsertQuery query;
  
    /**
     * @param ctx    the context
     * @param query  the query
     */
    InsertQueryAdapter(Context ctx, InsertQuery query) {
        super(ctx);
        this.query = query;
    }
    
    @Override
    protected InsertQueryAdapter newQuery(Context newContext) {
        return new InsertQueryAdapter(newContext, query.newQuery(newContext));
    }
  
    @Override
    public InsertQueryAdapter withTtl(Duration ttl) {
        return newQuery(getContext().withTtl((int) ttl.getSeconds()));
    }
  
    @Override
    public Mutation<?> ifNotExists() {
        return new InsertQueryAdapter(getContext(), query.ifNotExists());
    }
    
    @Override
    public BatchMutation combinedWith(Batchable other) {
        return new BatchMutationQueryAdapter(getContext(), query.combinedWith(new BatchMutationQueryAdapter.BatchableAdapter(other)));
    }
       
    @Override
    public ListenableFuture<Statement> getStatementAsync() {
        return query.getStatementAsync();
    }

    @Override
    public Result execute() {
        return CompletableFutures.getUninterruptibly(executeAsync());
    }
    
    @Override
    public CompletableFuture<Result> executeAsync() {
        return new ListenableToCompletableFutureAdapter<>(query.executeAsync());
    }
    
    @Override
    public String toString() {
        return query.toString();
    }
}

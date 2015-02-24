/*
 * Copyright 1&1 Internet AG, https://github.com/1and1/
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.oneandone.troilus;


import java.time.Duration;

import net.oneandone.troilus.Context;
import net.oneandone.troilus.InsertQuery;



/**
 * Java8 adapter of a InsertQuery
 */
class InsertQueryAdapter extends AbstractQueryAdapter<Insertion> implements Insertion {
  
    private final InsertQuery query;
    
    /**
     * @param ctx    the context
     * @param query  the query
     */
    InsertQueryAdapter(Context ctx, InsertQuery query) {
        super(ctx, query);
        this.query = query;
    }
    
    
    ////////////////////
    // factory methods
    
    @Override
    protected InsertQueryAdapter newQuery(Context newContext) {
        return new InsertQueryAdapter(newContext, query.newQuery(newContext));
    }
    
    private InsertQueryAdapter newQuery(InsertQuery query) {
        return new InsertQueryAdapter(getContext(), query.newQuery(getContext()));
    }
    
    // 
    ////////////////////
    
    
    @Override
    public BatchMutation combinedWith(Batchable<?> other) {
        return new BatchMutationQueryAdapter(getContext(), query.combinedWith(Mutations.toJava7Mutation(other)));
    }
    
    @Override
    public InsertQueryAdapter withTtl(Duration ttl) {
        return newQuery(getContext().withTtl((int) ttl.getSeconds()));
    }
    
    @Override
    public BatchableWithTime<Insertion> ifNotExists() {
        return newQuery(query.ifNotExists());
    }
}

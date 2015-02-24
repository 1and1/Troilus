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



import net.oneandone.troilus.BatchMutationQuery;
import net.oneandone.troilus.Context;


 
/**
 * Java8 adapter of a BatchMutationQuery
 */
class BatchMutationQueryAdapter extends AbstractQueryAdapter<BatchMutation> implements BatchMutation {
    
    private final BatchMutationQuery query;  
    
    
    /**
     * @param ctx    the context 
     * @param query  the underyling query
     */
    BatchMutationQueryAdapter(Context ctx, BatchMutationQuery query) {
        super(ctx, query);
        this.query = query;
    }

    
    ////////////////////
    // factory methods
    
    @Override
    protected BatchMutationQueryAdapter newQuery(Context newContext) {
        return new BatchMutationQueryAdapter(newContext, query.newQuery(newContext));
    }
    
    private BatchMutationQueryAdapter newQuery(BatchMutationQuery query) {
        return new BatchMutationQueryAdapter(getContext(), query.newQuery(getContext()));
    }
    
    // 
    ////////////////////
    
    
    @Override
    public BatchMutation withWriteAheadLog() {
        return newQuery(query.withWriteAheadLog());
    }
    
    @Override
    public BatchMutation withoutWriteAheadLog() {
        return newQuery(query.withoutWriteAheadLog());
    }

    public BatchMutation combinedWith(Batchable<?> other) {
        return newQuery(query.combinedWith(MutationQueryAdapter.toJava7Mutation(other)));
    }
}

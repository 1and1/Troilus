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




import net.oneandone.troilus.java7.BatchMutation;
import net.oneandone.troilus.java7.Batchable;

import com.datastax.driver.core.BatchStatement.Type;
import com.datastax.driver.core.Statement;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;


 
/**
 * Batch mutation query
 */
class BatchMutationQuery extends MutationQuery<BatchMutation> implements BatchMutation {
    private final ImmutableList<Batchable<?>> batchables;
    private final Type type;  
    
    
 
    BatchMutationQuery(Context ctx, Batchable<?> mutation1, Batchable<?> mutation2) {
        this(ctx, Type.LOGGED, join(mutation1, mutation2));
    }
 
    private static ImmutableList<Batchable<?>> join(Batchable<?> mutation1, Batchable<?> mutation2) {
        if ((mutation1 != null) && (mutation2 != null)) {
            return ImmutableList.<Batchable<?>>of(mutation1, mutation2);            
        } else if ((mutation1 != null) && (mutation2 == null)) {
            return ImmutableList.<Batchable<?>>of(mutation1);
        } else if ((mutation1 == null) && (mutation2 != null)) {
            return ImmutableList.<Batchable<?>>of(mutation1);
        } else {
            return null;
        }
    }
    
    private BatchMutationQuery(Context ctx, Type type, ImmutableList<Batchable<?>> batchables) {
        super(ctx);
        this.type = type;
        this.batchables = batchables;
    }
    
    
    ////////////////////
    // factory methods
    
    @Override
    protected BatchMutationQuery newQuery(Context newContext) {
        return new BatchMutationQuery(newContext, type, batchables);
    }
    
    private BatchMutationQuery newQuery(Type type, ImmutableList<Batchable<?>> batchables) {
        return new BatchMutationQuery(getContext(), type, batchables);
    }
    
    //
    ///////////////////
    
    
    
    @Override
    public BatchMutationQuery withWriteAheadLog() {
        return newQuery(Type.LOGGED, batchables);
    }
    
    @Override
    public BatchMutationQuery withoutWriteAheadLog() {
        return newQuery(Type.UNLOGGED, batchables);
    }

    @Override
    public BatchMutationQuery combinedWith(Batchable<?> other) {
        if (other == null) {
            return this;
        } else {
            return newQuery(type, Immutables.join(batchables, other));
        }
    }

    @Override    
    public ListenableFuture<Statement> getStatementAsync() {
        
        Function<Batchable<?>, ListenableFuture<Statement>> statementFetcher = new Function<Batchable<?>, ListenableFuture<Statement>>() {
            public ListenableFuture<Statement> apply(Batchable<?> batchable) {
                return batchable.getStatementAsync();
            };
        };
        return mergeToBatch(type, batchables.iterator(), statementFetcher);
    }
}

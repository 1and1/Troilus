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
package net.oneandone.troilus;



import net.oneandone.troilus.java7.Dao.BatchMutation;
import net.oneandone.troilus.java7.Dao.Batchable;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.BatchStatement.Type;
import com.datastax.driver.core.Statement;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;


 

class BatchMutationQuery extends AbstractQuery<BatchMutation> implements BatchMutation {
    private final ImmutableList<Batchable> batchables;
    private final Type type;  
    
    
    BatchMutationQuery(Context ctx, Type type, ImmutableList<Batchable> batchables) {
        super(ctx);
        this.type = type;
        this.batchables = batchables;
    }
    
    @Override
    protected BatchMutationQuery newQuery(Context newContext) {
        return new BatchMutationQuery(newContext, type, batchables);
    }
    
    @Override
    public BatchMutationQuery withWriteAheadLog() {
        return new BatchMutationQuery(getContext(), Type.LOGGED, batchables);
    }
    
    @Override
    public BatchMutationQuery withoutWriteAheadLog() {
        return new BatchMutationQuery(getContext(), Type.UNLOGGED, batchables);
    }

    @Override
    public BatchMutationQuery combinedWith(Batchable other) {
        return new BatchMutationQuery(getContext(), type, Immutables.merge(batchables, other));
    }

    private ListenableFuture<Statement> getStatementAsync() {
        BatchStatement batchStmt = new BatchStatement(type);
        for (Batchable batchable : batchables) {
            batchable.addTo(batchStmt);
        }
        
        return Futures.<Statement>immediateFuture(batchStmt);
    }
    
    
    @Override
    public Result execute() {
        return ListenableFutures.getUninterruptibly(executeAsync());
    }
    
    
    @Override
    public ListenableFuture<Result> executeAsync() {
        ListenableFuture<ResultSet> future = performAsync(getStatementAsync());
        
        Function<ResultSet, Result> mapEntity = new Function<ResultSet, Result>() {
            @Override
            public Result apply(ResultSet resultSet) {
                return newResult(resultSet);
            }
        };
        
        return Futures.transform(future, mapEntity);
    }
}

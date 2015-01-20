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


import net.oneandone.troilus.java7.Dao.Batchable;
import net.oneandone.troilus.java7.Dao.Insertion;
import net.oneandone.troilus.java7.interceptor.WriteQueryData;
import net.oneandone.troilus.java7.interceptor.WriteQueryRequestInterceptor;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.BatchStatement.Type;
import com.datastax.driver.core.Statement;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * insert query implementation
 */
class InsertQuery extends AbstractQuery<Insertion> implements Insertion {
    
    private final WriteQueryDataImpl data;
  
    
    /**
     * @param ctx   the context
     * @param data  the data
     */
    InsertQuery(Context ctx, WriteQueryDataImpl data) {
        super(ctx);
        this.data = data;
    }
    
    
    @Override
    protected InsertQuery newQuery(Context newContext) {
        return new InsertQuery(newContext, data);
    }
    
    @Override
    public InsertQuery withTtl(int ttlSec) {
        return newQuery(getContext().withTtl(ttlSec));
    }

    @Override
    public BatchMutationQuery combinedWith(Batchable other) {
        return new BatchMutationQuery(getContext(), Type.LOGGED, ImmutableList.of(this, other));
    }
    
    @Override
    public InsertQuery ifNotExists() {
        return new InsertQuery(getContext(), data.ifNotExists(true));
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
                Result result = newResult(resultSet);
                assertResultIsAppliedWhen((data.getIfNotExits() != null) && (data.getIfNotExits()), result, "duplicated entry");

                return result;
            }
        };
        
        return Futures.transform(future, mapEntity);
    }
    
    @Override
    public ListenableFuture<Statement> getStatementAsync() {
        // TODO real async impl

        WriteQueryData queryData = data;
        for (WriteQueryRequestInterceptor interceptor : getContext().getInterceptorRegistry().getInterceptors(WriteQueryRequestInterceptor.class)) {
            queryData = interceptor.onWriteRequest(queryData); 
        }
        
        return Futures.immediateFuture(WriteQueryDataImpl.toStatement(queryData, getContext()));
    }
}
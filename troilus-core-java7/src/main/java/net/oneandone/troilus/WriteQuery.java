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


import java.util.Set;

import net.oneandone.troilus.java7.Mutation;
import net.oneandone.troilus.java7.interceptor.CascadeOnWriteInterceptor;
import net.oneandone.troilus.java7.interceptor.WriteQueryData;
import net.oneandone.troilus.java7.interceptor.WriteQueryRequestInterceptor;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Statement;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;



/**
 * abstract write query implementation
 */
abstract class WriteQuery<Q> extends MutationQuery<Q> {
    
    private final WriteQueryData data;
  
    
    /**
     * @param ctx   the context
     * @param data  the data
     */
    WriteQuery(Context ctx, WriteQueryData data) {
        super(ctx);
        this.data = data;
    }
    
    protected WriteQueryData getData() {
        return data;
    }
    
    public ListenableFuture<Result> executeAsync() {
        ListenableFuture<ResultSet> future = performAsync(getStatementAsync());
        
        Function<ResultSet, Result> mapEntity = new Function<ResultSet, Result>() {
            @Override
            public Result apply(ResultSet resultSet) {
                Result result = newResult(resultSet);
                if (isLwt() && !result.wasApplied()) {
                    throw new IfConditionException(result, "duplicated entry");
                }
                return result;
            }
        };
        
        return Futures.transform(future, mapEntity);
    }
    
    private boolean isLwt() {
        return ((data.getIfNotExits() != null) && (data.getIfNotExits()) || !data.getOnlyIfConditions().isEmpty());                
    }
    

    
    public ListenableFuture<Statement> getStatementAsync() {
        // perform request executors
        ListenableFuture<WriteQueryData> queryDataFuture = executeRequestInterceptorsAsync(Futures.<WriteQueryData>immediateFuture(data));        
        
        // query data to statement
        Function<WriteQueryData, Statement> queryDataToStatement = new Function<WriteQueryData, Statement>() {
            @Override
            public Statement apply(WriteQueryData queryData) {
                return WriteQueryDataImpl.toStatement(queryData, getContext());
            }
        };
        
        
        ListenableFuture<Statement> statementFuture = Futures.transform(queryDataFuture, queryDataToStatement);
        if (getContext().getInterceptorRegistry().getInterceptors(CascadeOnWriteInterceptor.class).isEmpty()) {
            return statementFuture;
            
            
        // cascading statements   
        } else {
            ListenableFuture<ImmutableSet<Statement>> cascadingStatmentsFuture = executeCascadeInterceptorsAsync(queryDataFuture);
            return mergeStatements(statementFuture, cascadingStatmentsFuture);
        }
    }
    
    
    
    
    private ListenableFuture<WriteQueryData> executeRequestInterceptorsAsync(ListenableFuture<WriteQueryData> queryDataFuture) {

        for (WriteQueryRequestInterceptor interceptor : getContext().getInterceptorRegistry().getInterceptors(WriteQueryRequestInterceptor.class).reverse()) {
            final WriteQueryRequestInterceptor icptor = interceptor;

            Function<WriteQueryData, ListenableFuture<WriteQueryData>> mapperFunction = new Function<WriteQueryData, ListenableFuture<WriteQueryData>>() {
                @Override
                public ListenableFuture<WriteQueryData> apply(WriteQueryData queryData) {
                    return icptor.onWriteRequestAsync(queryData);
                }
            };

            queryDataFuture = ListenableFutures.transform(queryDataFuture, mapperFunction, getContext().getTaskExecutor());
        }

        return queryDataFuture; 
    }
    
    
    
    private ListenableFuture<ImmutableSet<Statement>> executeCascadeInterceptorsAsync(ListenableFuture<WriteQueryData> queryDataFuture) {
        Set<ListenableFuture<ImmutableSet<Statement>>> statmentFutures = Sets.newHashSet();
        
        for (CascadeOnWriteInterceptor interceptor : getContext().getInterceptorRegistry().getInterceptors(CascadeOnWriteInterceptor.class).reverse()) {
            final CascadeOnWriteInterceptor icptor = interceptor;

            Function<WriteQueryData, ListenableFuture<ImmutableSet<? extends Mutation<?>>>> querydataToBatchables = new Function<WriteQueryData, ListenableFuture<ImmutableSet<? extends Mutation<?>>>>() {
                @Override
                public ListenableFuture<ImmutableSet<? extends Mutation<?>>> apply(WriteQueryData queryData) {
                    return icptor.onWriteAsync(queryData);                    
                }
            };
            ListenableFuture<ImmutableSet<? extends Mutation<?>>> batchablesFutureSet = ListenableFutures.transform(queryDataFuture, querydataToBatchables, getContext().getTaskExecutor());
            
            ListenableFuture<ImmutableSet<Statement>> flattenStatementFutureSet = transformBatchablesToStatement(batchablesFutureSet);
            statmentFutures.add(flattenStatementFutureSet);
        }

        return ListenableFutures.flat(ImmutableSet.copyOf(statmentFutures), getContext().getTaskExecutor());
    }
}
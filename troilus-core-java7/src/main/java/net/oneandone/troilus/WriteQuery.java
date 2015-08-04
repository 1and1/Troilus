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

import net.oneandone.troilus.Context.DBSession;
import net.oneandone.troilus.java7.Batchable;
import net.oneandone.troilus.java7.interceptor.CascadeOnWriteInterceptor;
import net.oneandone.troilus.java7.interceptor.WriteQueryData;
import net.oneandone.troilus.java7.interceptor.WriteQueryRequestInterceptor;

import com.datastax.driver.core.Statement;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
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
    
    
    public CounterMutationQuery incr(String name) {
        return incr(name, 1);
    }
    
    public CounterMutationQuery incr(String name, long value) {
        return new CounterMutationQuery(getContext(), 
                                        new CounterMutationQueryData(data.getTablename()).keys(getData().getKeys())
                                                                                         .whereConditions(getData().getWhereConditions())
                                                                                         .name(name)
                                                                                         .diff(value));  
    }
    
    public CounterMutationQuery decr(String name) {
        return decr(name, 1);
    }
    
    public CounterMutationQuery decr(String name, long value) {
        return new CounterMutationQuery(getContext(), 
                                        new CounterMutationQueryData(data.getTablename()).keys(getData().getKeys())
                                                                                         .whereConditions(getData().getWhereConditions())
                                                                                         .name(name)
                                                                                         .diff(0 - value));  
    }
    
    protected ImmutableMap<Object, Optional<Object>> addToMap(String name, Object key, Object value, ImmutableMap<Object, Optional<Object>> values) {
        return (values == null) ? ImmutableMap.of(key, Optionals.toGuavaOptional(value)) : Immutables.join(values, key, Optionals.toGuavaOptional(value));
    }
    
    @Override
    public ListenableFuture<Result> executeAsync() {
        ListenableFuture<Result> future = super.executeAsync();
        
        Function<Result, Result> validateLwtIfFunction = new Function<Result, Result>() {
            @Override
            public Result apply(Result result) {
                if (isLwt() && !result.wasApplied()) {
                    throw new IfConditionException(result, "duplicated entry");
                }
                return result;
            }
        };
        return Futures.transform(future, validateLwtIfFunction);
    }

    
    
    private boolean isLwt() {
        return ((data.getIfNotExits() != null) && (data.getIfNotExits()) || !data.getOnlyIfConditions().isEmpty());                
    }
    


    
    
    public ListenableFuture<Statement> getStatementAsync(final DBSession dbSession) {
        // perform request executors
        ListenableFuture<WriteQueryData> queryDataFuture = executeRequestInterceptorsAsync(Futures.<WriteQueryData>immediateFuture(data));        
        
        // query data to statement
        Function<WriteQueryData, ListenableFuture<Statement>> queryDataToStatement = new Function<WriteQueryData, ListenableFuture<Statement>>() {
            @Override
            public ListenableFuture<Statement> apply(WriteQueryData queryData) {
                return WriteQueryDataImpl.toStatementAsync(queryData, getExecutionSpec(), dbSession);
            }
        };
        
        
        ListenableFuture<Statement> statementFuture = ListenableFutures.transform(queryDataFuture, queryDataToStatement, getExecutor());
        if (getInterceptorRegistry().getInterceptors(CascadeOnWriteInterceptor.class).isEmpty()) {
            return statementFuture;
            
            
        // cascading statements   
        } else {
            ListenableFuture<ImmutableSet<Statement>> cascadingStatmentsFuture = executeCascadeInterceptorsAsync(dbSession, queryDataFuture);
            return mergeStatements(statementFuture, cascadingStatmentsFuture);
        }
    }
    
    
    
    
    private ListenableFuture<WriteQueryData> executeRequestInterceptorsAsync(ListenableFuture<WriteQueryData> queryDataFuture) {

        for (WriteQueryRequestInterceptor interceptor : getInterceptorRegistry().getInterceptors(WriteQueryRequestInterceptor.class).reverse()) {
            final WriteQueryRequestInterceptor icptor = interceptor;

            Function<WriteQueryData, ListenableFuture<WriteQueryData>> mapperFunction = new Function<WriteQueryData, ListenableFuture<WriteQueryData>>() {
                @Override
                public ListenableFuture<WriteQueryData> apply(WriteQueryData queryData) {
                    return icptor.onWriteRequestAsync(queryData);
                }
            };

            // running interceptors within dedicated threads!
            queryDataFuture = ListenableFutures.transform(queryDataFuture, mapperFunction, getExecutor());
        }

        return queryDataFuture; 
    }
    
    
    
    private ListenableFuture<ImmutableSet<Statement>> executeCascadeInterceptorsAsync(DBSession dbSession, ListenableFuture<WriteQueryData> queryDataFuture) {
        Set<ListenableFuture<ImmutableSet<Statement>>> statmentFutures = Sets.newHashSet();
        
        for (CascadeOnWriteInterceptor interceptor : getInterceptorRegistry().getInterceptors(CascadeOnWriteInterceptor.class).reverse()) {
            final CascadeOnWriteInterceptor icptor = interceptor;

            Function<WriteQueryData, ListenableFuture<ImmutableSet<? extends Batchable<?>>>> querydataToBatchables = new Function<WriteQueryData, ListenableFuture<ImmutableSet<? extends Batchable<?>>>>() {
                @Override
                public ListenableFuture<ImmutableSet<? extends Batchable<?>>> apply(WriteQueryData queryData) {
                    return icptor.onWriteAsync(queryData);                    
                }
            };
            
            // running interceptors within dedicated threads!
            ListenableFuture<ImmutableSet<? extends Batchable<?>>> batchablesFutureSet = ListenableFutures.transform(queryDataFuture, querydataToBatchables, getExecutor());
            
            ListenableFuture<ImmutableSet<Statement>> flattenStatementFutureSet = transformBatchablesToStatement(dbSession, batchablesFutureSet);
            statmentFutures.add(flattenStatementFutureSet);
        }

        return ListenableFutures.flat(ImmutableSet.copyOf(statmentFutures), getExecutor());
    }
}
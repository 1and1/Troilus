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

import net.oneandone.troilus.java7.interceptor.CascadeOnWriteInterceptor;
import net.oneandone.troilus.java7.interceptor.WriteQueryData;
import net.oneandone.troilus.java7.interceptor.WriteQueryRequestInterceptor;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BatchStatement.Type;
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
    
    private final WriteQueryDataImpl data;
  
    
    /**
     * @param ctx   the context
     * @param data  the data
     */
    WriteQuery(Context ctx, WriteQueryDataImpl data) {
        super(ctx);
        this.data = data;
    }
    
    protected WriteQueryDataImpl getData() {
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
            
        } else {
            // TODO make is real async
            ListenableFuture<ImmutableSet<Statement>> cascadingStatments = executeCascadeInterceptorsAsync(queryDataFuture);
            
            BatchStatement batchStatement = new BatchStatement(Type.LOGGED);
            batchStatement.add(ListenableFutures.getUninterruptibly(statementFuture));
            for (Statement statement : ListenableFutures.getUninterruptibly(cascadingStatments)) {
                batchStatement.add(statement);
            }
            
            return Futures.<Statement>immediateFuture(batchStatement);
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
        // TODO make is real async
        WriteQueryData queryData = ListenableFutures.getUninterruptibly(queryDataFuture);
        
        Set<Statement> statements = Sets.newHashSet();
        for (CascadeOnWriteInterceptor interceptor : getContext().getInterceptorRegistry().getInterceptors(CascadeOnWriteInterceptor.class).reverse()) {
            
            ImmutableSet<? extends Batchable> batchables = ListenableFutures.getUninterruptibly(interceptor.onWriteAsync(queryData));
            for (Batchable batchable : batchables) {
                Statement stmt = ListenableFutures.getUninterruptibly(batchable.getStatementAsync());
                statements.add(stmt);
            }
        }

        return Futures.immediateFuture(ImmutableSet.copyOf(statements)); 
    }

}
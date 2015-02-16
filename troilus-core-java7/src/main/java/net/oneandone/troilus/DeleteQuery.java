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

import net.oneandone.troilus.interceptor.DeleteQueryData;
import net.oneandone.troilus.java7.Deletion;
import net.oneandone.troilus.java7.interceptor.CascadeOnDeleteInterceptor;
import net.oneandone.troilus.java7.interceptor.DeleteQueryRequestInterceptor;

import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Statement;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;



/**
 * delete query implementation
 */
class DeleteQuery extends MutationQuery<Deletion> implements Deletion {

    private final DeleteQueryData data;
    
    /**
     * @param ctx    the context
     * @param data   the data
     */
    DeleteQuery(Context ctx, DeleteQueryData data) {
        super(ctx);
        this.data = data;
    }
    
    @Override
    protected DeleteQuery newQuery(Context newContext) {
        return new DeleteQuery(newContext, data);
    }
    
    @Override
    public DeleteQuery onlyIf(Clause... onlyIfConditions) {
        return new DeleteQuery(getContext(), data.onlyIfConditions(ImmutableList.copyOf(onlyIfConditions)));
    }
   
    @Override
    public DeleteQuery ifExists() {
        return new DeleteQuery(getContext(), data.ifExists(true));
    }
    
    @Override
    public ListenableFuture<Result> executeAsync() {
        ListenableFuture<ResultSet> future = performAsync(getStatementAsync());
        
        Function<ResultSet, Result> mapResult = new Function<ResultSet, Result>() {
            @Override
            public Result apply(ResultSet resultSet) {
                if (resultSet == null) {
                    throw new NullPointerException();
                }

                Result result = newResult(resultSet);
                if (!data.getOnlyIfConditions().isEmpty() && !result.wasApplied()) {
                    throw new IfConditionException(result, "if condition does not match");
                }
                return result;
            }
        };
        
        return Futures.transform(future, mapResult);
    }
    
    @Override
    public ListenableFuture<Statement> getStatementAsync() {
        
        // perform request executors
        ListenableFuture<DeleteQueryData> queryDataFuture = executeRequestInterceptorsAsync(Futures.<DeleteQueryData>immediateFuture(data));
        
        // query data to statement
        Function<DeleteQueryData, Statement> queryDataToStatement = new Function<DeleteQueryData, Statement>() {
            @Override
            public Statement apply(DeleteQueryData queryData) {
                if (queryData == null) {
                    throw new NullPointerException();
                }
                return DeleteQueryDataImpl.toStatement(queryData, getContext());
            }
        };
        
        
        ListenableFuture<Statement> statementFuture = Futures.transform(queryDataFuture, queryDataToStatement);
        if (getContext().getInterceptorRegistry().getInterceptors(CascadeOnDeleteInterceptor.class).isEmpty()) {
            return statementFuture;
        
        // cascading statements   
        } else {
            ListenableFuture<ImmutableSet<Statement>> cascadingStatmentsFuture = executeCascadeInterceptorsAsync(queryDataFuture);
            return mergeStatements(statementFuture, cascadingStatmentsFuture);
        }
    }

    
    
    
   
    
    private ListenableFuture<DeleteQueryData> executeRequestInterceptorsAsync(ListenableFuture<DeleteQueryData> queryDataFuture) {

        for (DeleteQueryRequestInterceptor interceptor : getContext().getInterceptorRegistry().getInterceptors(DeleteQueryRequestInterceptor.class).reverse()) {
            final DeleteQueryRequestInterceptor icptor = interceptor;

            Function<DeleteQueryData, ListenableFuture<DeleteQueryData>> mapperFunction = new Function<DeleteQueryData, ListenableFuture<DeleteQueryData>>() {
                @Override
                public ListenableFuture<DeleteQueryData> apply(DeleteQueryData queryData) {
                    return icptor.onDeleteRequestAsync(queryData);
                }
            };
            
            queryDataFuture = ListenableFutures.transform(queryDataFuture, mapperFunction, getContext().getTaskExecutor());
        }

        return queryDataFuture; 
    }
    
    
    private ListenableFuture<ImmutableSet<Statement>> executeCascadeInterceptorsAsync(ListenableFuture<DeleteQueryData> queryDataFuture) {
        Set<ListenableFuture<ImmutableSet<Statement>>> statmentFutures = Sets.newHashSet();
        
        for (CascadeOnDeleteInterceptor interceptor : getContext().getInterceptorRegistry().getInterceptors(CascadeOnDeleteInterceptor.class).reverse()) {
            final CascadeOnDeleteInterceptor icptor = interceptor;

            Function<DeleteQueryData, ListenableFuture<ImmutableSet<? extends Batchable>>> querydataToBatchables = new Function<DeleteQueryData, ListenableFuture<ImmutableSet<? extends Batchable>>>() {
                @Override
                public ListenableFuture<ImmutableSet<? extends Batchable>> apply(DeleteQueryData queryData) {
                    return icptor.onDeleteAsync(queryData);                    
                }
            };
            ListenableFuture<ImmutableSet<? extends Batchable>> batchablesFutureSet = ListenableFutures.transform(queryDataFuture, querydataToBatchables, getContext().getTaskExecutor());
            
            ListenableFuture<ImmutableSet<Statement>> flattenStatementFutureSet = transformBatchablesToStatement(batchablesFutureSet);
            statmentFutures.add(flattenStatementFutureSet);
        }

        return ListenableFutures.flat(ImmutableSet.copyOf(statmentFutures), getContext().getTaskExecutor());
    }
    
    
    
}

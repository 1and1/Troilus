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
import net.oneandone.troilus.java7.Batchable;
import net.oneandone.troilus.java7.interceptor.CascadeOnDeleteInterceptor;
import net.oneandone.troilus.java7.interceptor.DeleteQueryRequestInterceptor;

import com.datastax.driver.core.querybuilder.Clause;
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
    
    
    ////////////////////
    // factory methods

    @Override
    protected DeleteQuery newQuery(Context newContext) {
        return new DeleteQuery(newContext, data);
    }
    
    private DeleteQuery newQuery(DeleteQueryData data) {
        return new DeleteQuery(getContext(), data);
    }

    //
    ////////////////////

    
    @Override
    public BatchMutationQuery combinedWith(Batchable<?> other) {
        return new BatchMutationQuery(getContext(), this, other);
    }
    
    @Override
    public DeleteQuery onlyIf(Clause... onlyIfConditions) {
        return newQuery(data.onlyIfConditions(ImmutableList.copyOf(onlyIfConditions)));
    }
   
    @Override
    public DeleteQuery ifExists() {
        return newQuery(data.ifExists(true));
    }
        
    @Override
    public ListenableFuture<Result> executeAsync() {
        ListenableFuture<Result> future = super.executeAsync();
        
        Function<Result, Result> validateOnlyIfFunction = new Function<Result, Result>() {
            @Override
            public Result apply(Result result) {
                if (!data.getOnlyIfConditions().isEmpty() && !result.wasApplied()) {
                    throw new IfConditionException(result, "if condition does not match");
                }
                return result;
            }
        };
        return Futures.transform(future, validateOnlyIfFunction);
    }
    
    @Override
    public ListenableFuture<Statement> getStatementAsync(final Context ctx) {
        
        // perform request executors
        ListenableFuture<DeleteQueryData> queryDataFuture = executeRequestInterceptorsAsync(ctx, Futures.<DeleteQueryData>immediateFuture(data));
        
        // query data to statement
        Function<DeleteQueryData, ListenableFuture<Statement>> queryDataToStatement = new Function<DeleteQueryData, ListenableFuture<Statement>>() {
            @Override
            public ListenableFuture<Statement> apply(DeleteQueryData queryData) {
                if (queryData == null) {
                    throw new NullPointerException();
                }
                return DeleteQueryDataImpl.toStatementAsync(queryData, ctx);
            }
        };
        
        
        ListenableFuture<Statement> statementFuture = ListenableFutures.transform(queryDataFuture, queryDataToStatement, ctx.getTaskExecutor());
        if (getContext().getInterceptorRegistry().getInterceptors(CascadeOnDeleteInterceptor.class).isEmpty()) {
            return statementFuture;
        
        // cascading statements   
        } else {
            ListenableFuture<ImmutableSet<Statement>> cascadingStatmentsFuture = executeCascadeInterceptorsAsync(ctx, queryDataFuture);
            return mergeStatements(ctx, statementFuture, cascadingStatmentsFuture);
        }
    }

    
    
    
   
    
    private ListenableFuture<DeleteQueryData> executeRequestInterceptorsAsync(Context ctx, ListenableFuture<DeleteQueryData> queryDataFuture) {

        for (DeleteQueryRequestInterceptor interceptor : ctx.getInterceptorRegistry().getInterceptors(DeleteQueryRequestInterceptor.class).reverse()) {
            final DeleteQueryRequestInterceptor icptor = interceptor;

            Function<DeleteQueryData, ListenableFuture<DeleteQueryData>> mapperFunction = new Function<DeleteQueryData, ListenableFuture<DeleteQueryData>>() {
                @Override
                public ListenableFuture<DeleteQueryData> apply(DeleteQueryData queryData) {
                    return icptor.onDeleteRequestAsync(queryData);
                }
            };
            
            // running interceptors within dedicated threads!
            queryDataFuture = ListenableFutures.transform(queryDataFuture, mapperFunction, ctx.getTaskExecutor());
        }

        return queryDataFuture; 
    }
    
    
    private ListenableFuture<ImmutableSet<Statement>> executeCascadeInterceptorsAsync(Context ctx, ListenableFuture<DeleteQueryData> queryDataFuture) {
        Set<ListenableFuture<ImmutableSet<Statement>>> statmentFutures = Sets.newHashSet();
        
        for (CascadeOnDeleteInterceptor interceptor : getContext().getInterceptorRegistry().getInterceptors(CascadeOnDeleteInterceptor.class).reverse()) {
            final CascadeOnDeleteInterceptor icptor = interceptor;

            Function<DeleteQueryData, ListenableFuture<ImmutableSet<? extends Batchable<?>>>> querydataToBatchables = new Function<DeleteQueryData, ListenableFuture<ImmutableSet<? extends Batchable<?>>>>() {
                @Override
                public ListenableFuture<ImmutableSet<? extends Batchable<?>>> apply(DeleteQueryData queryData) {
                    return icptor.onDeleteAsync(queryData);                    
                }
            };
            ListenableFuture<ImmutableSet<? extends Batchable<?>>> batchablesFutureSet = ListenableFutures.transform(queryDataFuture, querydataToBatchables);
            
            ListenableFuture<ImmutableSet<Statement>> flattenStatementFutureSet = transformBatchablesToStatement(ctx, batchablesFutureSet);
            statmentFutures.add(flattenStatementFutureSet);
        }

        // running interceptors within dedicated threads!
        return ListenableFutures.flat(ImmutableSet.copyOf(statmentFutures), ctx.getTaskExecutor());
    }
}

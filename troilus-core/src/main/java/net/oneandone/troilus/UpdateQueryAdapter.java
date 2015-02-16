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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import net.oneandone.troilus.AbstractQuery;
import net.oneandone.troilus.Context;
import net.oneandone.troilus.ColumnName;
import net.oneandone.troilus.Result;
import net.oneandone.troilus.UpdateQuery;
import net.oneandone.troilus.UpdateQuery.CounterMutationQuery;

import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Clause;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;


 
/**
 * Java8 adapter of a UpdateQuery
 */
class UpdateQueryAdapter extends AbstractQuery<UpdateQueryAdapter> implements WriteWithCounter, UpdateWithUnitAndCounter  {
    
    private final UpdateQuery query;
    
    /**
     * @param ctx     the context 
     * @param query   the underlying query
     */
    UpdateQueryAdapter(Context ctx, UpdateQuery query) {
        super(ctx);
        this.query = query;
    }
    
    private UpdateQuery getQuery() {
        return query;
    }
    
    @Override
    public ListenableFuture<Statement> getStatementAsync() {
        return query.getStatementAsync();
    }
    
    @Override
    public Result execute() {
        return CompletableFutures.getUninterruptibly(executeAsync());
    }
    
    @Override
    public CompletableFuture<Result> executeAsync() {
        return CompletableFutures.toCompletableFuture(query.executeAsync());
    }
    
    @Override
    public BatchMutation combinedWith(Batchable other) {
        return new BatchMutationQueryAdapter(getContext(), query.combinedWith(other));
    }
    
    @Override
    protected UpdateQueryAdapter newQuery(Context newContext) {
        return new UpdateQueryAdapter(newContext, getQuery().newQuery(newContext));
    }

    @Override
    public UpdateQueryAdapter withTtl(Duration ttl) {
        return new UpdateQueryAdapter(getContext(), getQuery().withTtl((int) ttl.getSeconds()));
    }

    @Override
    public Update<Write> onlyIf(Clause... conditions) {
        return new UpdateQueryAdapter(getContext(), getQuery().onlyIf(conditions));
    }

    @Override
    public Insertion ifNotExists() {
        return new InsertQueryAdapter(getContext(), getQuery().ifNotExists());
    }

    @Override
    public UpdateQueryAdapter entity(Object entity) {
        return new UpdateQueryAdapter(getContext(), getQuery().entity(entity));
    }
    
    @Override
    public UpdateQueryAdapter value(String name, Object value) {
        return new UpdateQueryAdapter(getContext(), getQuery().value(name, value));
    }
    
    @Override
    public <T> Write value(ColumnName<T> name, T value) {
        return new UpdateQueryAdapter(getContext(), getQuery().value(name.getName(), value));
    }
    
    @Override
    public UpdateQueryAdapter values(ImmutableMap<String, Object> nameValuePairsToAdd) {
        return new UpdateQueryAdapter(getContext(), getQuery().values(nameValuePairsToAdd));
    }

    @Override
    public UpdateQueryAdapter removeSetValue(String name, Object value) {
        return new UpdateQueryAdapter(getContext(), getQuery().removeSetValue(name, value));
    }
    
    @Override
    public <T> Write removeSetValue(ColumnName<Set<T>> name, T value) {
        return removeSetValue(name.getName(), value);
    }

    @Override
    public Write addSetValue(String name, Object value) {
        return new UpdateQueryAdapter(getContext(), getQuery().addSetValue(name, value));
    }
    
    @Override
    public <T> Write addSetValue(ColumnName<Set<T>> name, T value) {
        return addSetValue(name.getName(), value);
    }
   
    @Override
    public Write prependListValue(String name, Object value) {
        return new UpdateQueryAdapter(getContext(), getQuery().prependListValue(name, value));
    } 
    
    @Override
    public <T> Write prependListValue(ColumnName<List<T>> name, T value) {
        return prependListValue(name.getName(), value);
    }
    
    @Override
    public Write appendListValue(String name, Object value) {
        return new UpdateQueryAdapter(getContext(), getQuery().appendListValue(name, value));
    }
    
    @Override
    public <T> Write appendListValue(ColumnName<List<T>> name, T value) {
        return appendListValue(name.getName(), value);
    }
    
    @Override
    public Write removeListValue(String name, Object value) {
        return new UpdateQueryAdapter(getContext(), getQuery().removeListValue(name, value));
    }
    
    @Override
    public <T> Write removeListValue(ColumnName<List<T>> name, T value) {
        return removeListValue(name.getName(), value);
    }
    
    @Override
    public Write putMapValue(String name, Object key, Object value) {
        return new UpdateQueryAdapter(getContext(), getQuery().putMapValue(name, key, value));
    }
    
    @Override
    public <T, V> Write putMapValue(ColumnName<Map<T, V>> name, T key, V value) {
        return putMapValue(name.getName(), key, value);
    }
        
    @Override
    public CounterMutationQueryAdapter incr(String name) {
        return new CounterMutationQueryAdapter(getContext(), getQuery().incr(name));
    }
    
    @Override
    public CounterMutationQueryAdapter incr(String name, long value) {
        return new CounterMutationQueryAdapter(getContext(), getQuery().incr(name, value));
    }
    
    @Override
    public CounterMutationQueryAdapter decr(String name) {
        return new CounterMutationQueryAdapter(getContext(), getQuery().decr(name));
    }
    
    @Override
    public CounterMutationQueryAdapter decr(String name, long value) {
        return new CounterMutationQueryAdapter(getContext(), getQuery().decr(name, value));
    }
    
    
    
    /**
     * Java8 adapter of a CounterMutationQuery
     */
     private static class CounterMutationQueryAdapter extends AbstractQuery<CounterMutation> implements CounterMutation {
            
         private final CounterMutationQuery query;
         
         /**
          * @param ctx     the context
          * @param query   the underlying query
          */
         CounterMutationQueryAdapter(Context ctx, CounterMutationQuery query) {
             super(ctx);
             this.query = query;
         }
         
         @Override
         protected CounterMutation newQuery(Context newContext) {
             return new CounterMutationQueryAdapter(newContext, query.newQuery(newContext));
         }
    
         @Override
         public CounterMutation withTtl(Duration ttl) {
             return new CounterMutationQueryAdapter(getContext(), query.withTtl((int) ttl.getSeconds()));
         }
         
         @Override
         public CounterBatchMutation combinedWith(CounterBatchable other) {
             return new CounterBatchMutationQueryAdapter(getContext(), query.combinedWith(other));
         }
    
         @Override
         public ListenableFuture<Statement> getStatementAsync() {
             return query.getStatementAsync();
         }
    
         @Override
         public Result execute() {
             return CompletableFutures.getUninterruptibly(executeAsync());
         }
         
         @Override
         public CompletableFuture<Result> executeAsync() {
             return CompletableFutures.toCompletableFuture(query.executeAsync());
         }
         
         @Override
         public String toString() {
             return query.toString();
         }
     }
    

 }


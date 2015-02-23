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

import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Clause;
import com.google.common.collect.ImmutableMap;


 
/**
 * Java8 adapter of a UpdateQuery
 */
class UpdateQueryAdapter extends AbstractQuery<UpdateQueryAdapter> implements UpdateWithUnitAndCounter  {
    
    private final UpdateQuery query;
    
    /**
     * @param ctx     the context 
     * @param query   the underlying query
     */
    UpdateQueryAdapter(Context ctx, UpdateQuery query) {
        super(ctx);
        this.query = query;
    }


    ////////////////////
    // factory methods
     
    @Override
    protected UpdateQueryAdapter newQuery(Context newContext) {
        return new UpdateQueryAdapter(newContext, getQuery().newQuery(newContext));
    }

    private UpdateQueryAdapter newQuery(UpdateQuery query) {
        return new UpdateQueryAdapter(getContext(), query.newQuery(getContext()));
    }
    
    //
    ////////////////////

    
    private UpdateQuery getQuery() {
        return query;
    }
    
    public CompletableFuture<Statement> getStatementAsync() {
        return CompletableFutures.toCompletableFuture(query.getStatementAsync());
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
    public BatchMutation combinedWith(Batchable<?> other) {
        return new BatchMutationQueryAdapter(getContext(), query.combinedWith(MutationQueryAdapter.toJava7Mutation(other)));
    }
    

    @Override
    public UpdateQueryAdapter withTtl(Duration ttl) {
        return newQuery(getQuery().withTtl((int) ttl.getSeconds()));
    }

    @Override
    public BatchableWithTime<UpdateWithUnitAndCounter> onlyIf(Clause... conditions) {
        return newQuery(getQuery().onlyIf(conditions));
    }

    @Override
    public UpdateQueryAdapter entity(Object entity) {
        return newQuery(getQuery().entity(entity));
    }
    
    @Override
    public UpdateQueryAdapter value(String name, Object value) {
        return newQuery(getQuery().value(name, value));
    }

    @Override
    public <T> UpdateWithUnitAndCounter value(ColumnName<T> name, T value) {
        return newQuery(getQuery().value(name.getName(), value));
    }
    
    @Override
    public UpdateQueryAdapter values(ImmutableMap<String, Object> nameValuePairsToAdd) {
        return newQuery(getQuery().values(nameValuePairsToAdd));
    }

    @Override
    public UpdateQueryAdapter removeSetValue(String name, Object value) {
        return newQuery(getQuery().removeSetValue(name, value));
    }

    @Override
    public <T> UpdateWithUnitAndCounter removeSetValue(ColumnName<Set<T>> name,T value) {
        return removeSetValue(name.getName(), value);
    }

    @Override
    public UpdateWithUnitAndCounter addSetValue(String name, Object value) {
        return newQuery(getQuery().addSetValue(name, value));
    }

    @Override
    public <T> UpdateWithUnitAndCounter addSetValue(ColumnName<Set<T>> name, T value) {
        return addSetValue(name.getName(), value);
    }

    @Override
    public UpdateWithUnitAndCounter prependListValue(String name, Object value) {
        return newQuery(getQuery().prependListValue(name, value));
    } 

    @Override
    public <T> UpdateWithUnitAndCounter prependListValue(ColumnName<List<T>> name, T value) {
        return prependListValue(name.getName(), value);
    }

    @Override
    public UpdateWithUnitAndCounter appendListValue(String name, Object value) {
        return newQuery(getQuery().appendListValue(name, value));
    }
    
    @Override
    public <T> UpdateWithUnitAndCounter appendListValue(ColumnName<List<T>> name, T value) {
        return appendListValue(name.getName(), value);
    }
    
    @Override
    public UpdateWithUnitAndCounter removeListValue(String name, Object value) {
        return newQuery(getQuery().removeListValue(name, value));
    }

    @Override
    public <T> UpdateWithUnitAndCounter removeListValue(ColumnName<List<T>> name, T value) {
        return removeListValue(name.getName(), value);
    }

    @Override
    public UpdateWithUnitAndCounter putMapValue(String name, Object key, Object value) {
        return newQuery(getQuery().putMapValue(name, key, value));
    }

    @Override
    public <T, V> UpdateWithUnitAndCounter putMapValue(ColumnName<Map<T, V>> name, T key, V value) {
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
}


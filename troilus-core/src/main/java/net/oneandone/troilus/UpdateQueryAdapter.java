/*
 * Copyright (c) 2015 1&1 Internet AG, Germany, http://www.1und1.de
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




import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import net.oneandone.troilus.AbstractQuery;
import net.oneandone.troilus.Context;
import net.oneandone.troilus.InsertQuery;
import net.oneandone.troilus.Name;
import net.oneandone.troilus.Result;
import net.oneandone.troilus.UpdateQuery;
import net.oneandone.troilus.WriteQueryDataImpl;
import net.oneandone.troilus.Dao.BatchMutation;
import net.oneandone.troilus.Dao.Batchable;
import net.oneandone.troilus.Dao.CounterBatchMutation;
import net.oneandone.troilus.Dao.CounterBatchable;
import net.oneandone.troilus.Dao.CounterMutation;
import net.oneandone.troilus.Dao.Insertion;
import net.oneandone.troilus.Dao.Update;
import net.oneandone.troilus.Dao.UpdateWithValuesAndCounter;
import net.oneandone.troilus.Dao.Write;
import net.oneandone.troilus.Dao.WriteWithCounter;
import net.oneandone.troilus.UpdateQuery.CounterMutationQuery;

import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Clause;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;


 
/**
 * Java8 adapter of a UpdateQuery
 */
class UpdateQueryAdapter extends AbstractQuery<WriteWithCounter> implements WriteWithCounter, UpdateWithValuesAndCounter  {

    private final UpdateQuery query;
    
    
    /**
     * @param ctx     the context 
     * @param query   the underyling query
     */
    UpdateQueryAdapter(Context ctx, UpdateQuery query) {
        super(ctx);
        this.query = query;
    }
    
    @Override
    protected UpdateQueryAdapter newQuery(Context newContext) {
        return new UpdateQueryAdapter(newContext, query.newQuery(newContext));
    }

    @Override
    public UpdateQueryAdapter withTtl(Duration ttl) {
        return new UpdateQueryAdapter(getContext(), query.withTtl((int) ttl.getSeconds()));
    }

    @Override
    public Update<Write> onlyIf(Clause... conditions) {
        return new UpdateQueryAdapter(getContext(), query.onlyIf(conditions));
    }

    @Override
    public Insertion ifNotExists() {
        return new InsertQueryAdapter(getContext(), query.ifNotExists());
    }

    public Insertion entity(Object entity) {
        return new InsertQueryAdapter(getContext(), new InsertQuery(getContext(), new WriteQueryDataImpl().valuesToMutate(getContext().getBeanMapper().toValues(entity))));
    }
    
    @Override
    public UpdateQueryAdapter value(String name, Object value) {
        return new UpdateQueryAdapter(getContext(), query.value(name, value));
    }
    
    @Override
    public <T> Write value(Name<T> name, T value) {
        return new UpdateQueryAdapter(getContext(), query.value(name.getName(), name.convertWrite(value)));
    }
    
    @Override
    public UpdateQueryAdapter values(ImmutableMap<String, Object> nameValuePairsToAdd) {
        return new UpdateQueryAdapter(getContext(), query.values(nameValuePairsToAdd));
    }

    @Override
    public UpdateQueryAdapter removeSetValue(String name, Object value) {
        return new UpdateQueryAdapter(getContext(), query.removeSetValue(name, value));
    }

    @Override
    public UpdateQueryAdapter addSetValue(String name, Object value) {
        return new UpdateQueryAdapter(getContext(), query.addSetValue(name, value));
    }
   
    @Override
    public Write prependListValue(String name, Object value) {
        return new UpdateQueryAdapter(getContext(), query.prependListValue(name, value));
    } 
    
    @Override
    public Write appendListValue(String name, Object value) {
        return new UpdateQueryAdapter(getContext(), query.appendListValue(name, value));
    }
    
    @Override
    public Write removeListValue(String name, Object value) {
        return new UpdateQueryAdapter(getContext(), query.removeListValue(name, value));
    }
    
    @Override
    public Write putMapValue(String name, Object key, Object value) {
        return new UpdateQueryAdapter(getContext(), query.putMapValue(name, key, value));
    }
        
    @Override
    public CounterMutationQueryAdapter incr(String name) {
        return new CounterMutationQueryAdapter(getContext(), query.incr(name));
    }
    
    @Override
    public CounterMutationQueryAdapter incr(String name, long value) {
        return new CounterMutationQueryAdapter(getContext(), query.incr(name, value));
    }
    
    @Override
    public CounterMutationQueryAdapter decr(String name) {
        return new CounterMutationQueryAdapter(getContext(), query.decr(name));
    }
    
    @Override
    public CounterMutationQueryAdapter decr(String name, long value) {
        return new CounterMutationQueryAdapter(getContext(), query.decr(name, value));
    }
    
    @Override
    public BatchMutation combinedWith(Batchable other) {
        return new BatchMutationQueryAdapter(getContext(), query.combinedWith(new BatchMutationQueryAdapter.BatchableAdapter(other)));
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

    
      

    /**
     * Java8 adapter of a CounterMutationQuery
     */
    private static final class CounterMutationQueryAdapter extends AbstractQuery<CounterMutation> implements CounterMutation {
        
        private final CounterMutationQuery query;

        
        /**
         * @param ctx     the context
         * @param query   the undewrlying query
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
            return new CounterBatchMutationQueryAdapter(getContext(), query.combinedWith(new CounterBatchMutationQueryAdapter.CounterBatchableAdapter(other)));
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


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
package com.unitedinternet.troilus;




import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.querybuilder.Clause;
import com.google.common.collect.ImmutableMap;
import com.unitedinternet.troilus.Dao.BatchMutation;
import com.unitedinternet.troilus.Dao.Batchable;
import com.unitedinternet.troilus.Dao.CounterBatchMutation;
import com.unitedinternet.troilus.Dao.CounterBatchable;
import com.unitedinternet.troilus.Dao.Insertion;
import com.unitedinternet.troilus.Dao.Update;
import com.unitedinternet.troilus.Dao.UpdateWithValuesAndCounter;
import com.unitedinternet.troilus.Dao.Write;
import com.unitedinternet.troilus.Dao.WriteWithCounter;
import com.unitedinternet.troilus.Dao.CounterMutation;
import com.unitedinternet.troilus.UpdateQuery.CounterMutationQuery;


 
class UpdateQueryAdapter extends AbstractQuery<WriteWithCounter> implements WriteWithCounter, UpdateWithValuesAndCounter  {

    private final UpdateQuery query;
    
    
    public UpdateQueryAdapter(Context ctx, UpdateQuery query) {
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
    public Insertion ifNotExits() {
        return new InsertQueryAdapter(getContext(), query.ifNotExits());
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
        return new UpdateQueryAdapter(getContext(), query.value(name.getName(), value));
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
    

    public BatchMutation combinedWith(Batchable other) {
        return new BatchMutationQueryAdapter(getContext(), query.combinedWith(new BatchMutationQueryAdapter.BatchableAdapter(other)));
    }
       
    @Override
    public void addTo(BatchStatement batchStatement) {
        query.addTo(batchStatement);
    }
    
    
    @Override
    public CompletableFuture<Result> executeAsync() {
        return new ListenableToCompletableFutureAdapter<>(query.executeAsync());
    }
  
    @Override
    public String toString() {
        return query.toString();
    }

    
      
    private static final class CounterMutationQueryAdapter extends AbstractQuery<CounterMutation> implements CounterMutation {
        
        private final CounterMutationQuery query;

        public CounterMutationQueryAdapter(Context ctx, CounterMutationQuery query) {
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
        public void addTo(BatchStatement batchStatement) {
            query.addTo(batchStatement);
        }

        @Override
        public CompletableFuture<Result> executeAsync() {
            return new ListenableToCompletableFutureAdapter<>(query.executeAsync());
        }
        
        @Override
        public String toString() {
            return query.toString();
        }
    }
}


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
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BatchStatement.Type;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Clause;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
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
import com.unitedinternet.troilus.DaoImpl.WriteQueryDataAdapter;
import com.unitedinternet.troilus.interceptor.WriteQueryData;
import com.unitedinternet.troilus.interceptor.WriteQueryPreInterceptor;


 
class UpdateQuery extends AbstractQuery<WriteWithCounter> implements WriteWithCounter, UpdateWithValuesAndCounter  {

    private final WriteQueryData data;
    
    
    public UpdateQuery(Context ctx, WriteQueryData data) {
        super(ctx);
        this.data = data;
    }
    
    @Override
    protected UpdateQuery newQuery(Context newContext) {
        return new UpdateQuery(newContext, data);
    }
    
    public UpdateQuery withTtl(Duration ttl) {
        return newQuery(getContext().withTtl((int) ttl.getSeconds()));
    }
    
    public BatchMutation combinedWith(Batchable other) {
        return new BatchMutationQuery(getContext(), Type.LOGGED, ImmutableList.of(this, other));
    }
       
    @Override
    public void addTo(BatchStatement batchStatement) {
        batchStatement.add(getStatement());
    }
    
    
    public InsertQuery entity(Object entity) {
        return new InsertQuery(getContext(), 
                               new WriteQueryDataAdapter().valuesToMutate(mapOptional(getContext().getBeanMapper().toValues(entity))));
    }
    
    private ImmutableMap<String, Optional<Object>> mapOptional(ImmutableMap<String, com.google.common.base.Optional<Object>> m) {
        return Java8Immutables.transform(m, name -> name, guavaOptional -> Optional.ofNullable(guavaOptional.orNull())); 
    }
    
    
    @Override
    public UpdateQuery value(String name, Object value) {
        return new UpdateQuery(getContext(), 
                               data.valuesToMutate(Java8Immutables.merge(data.getValuesToMutate(), name, toOptional(value))));
    }
    

    @Override
    public <T> Write value(Name<T> name, T value) {
        return value(name.getName(), value);
    }
    
    @Override
    public UpdateQuery values(ImmutableMap<String, Object> nameValuePairsToAdd) {
        return new UpdateQuery(getContext(), 
                               data.valuesToMutate(Java8Immutables.merge(data.getValuesToMutate(), Java8Immutables.transform(nameValuePairsToAdd, name -> name, value -> toOptional(value)))));
    }


    @Override
    public UpdateQuery removeSetValue(String name, Object value) {
        ImmutableSet<Object> values = data.getSetValuesToRemove().get(name);
        values = (values == null) ? ImmutableSet.of(value) : Java8Immutables.merge(values, value);

        return new UpdateQuery(getContext(), 
                               data.setValuesToRemove(Java8Immutables.merge(data.getSetValuesToRemove(), name, values)));
    }

    
    @Override
    public UpdateQuery addSetValue(String name, Object value) {
        ImmutableSet<Object> values = data.getSetValuesToAdd().get(name);
        values = (values == null) ? ImmutableSet.of(value): Java8Immutables.merge(values, value);

        return new UpdateQuery(getContext(), 
                               data.setValuesToAdd(Java8Immutables.merge(data.getSetValuesToAdd(), name, values)));
    }
   
    
    @Override
    public Write prependListValue(String name, Object value) {
        ImmutableList<Object> values = data.getListValuesToPrepend().get(name);
        values = (values == null) ? ImmutableList.of(value) : Java8Immutables.merge(values, value);

        return new UpdateQuery(getContext(), 
                               data.listValuesToPrepend(Java8Immutables.merge(data.getListValuesToPrepend(), name, values)));
    } 
    
    

    @Override
    public Write appendListValue(String name, Object value) {
        ImmutableList<Object> values = data.getListValuesToAppend().get(name);
        values = (values == null) ? ImmutableList.of(value) : Java8Immutables.merge(values, value);

        return new UpdateQuery(getContext(), 
                               data.listValuesToAppend(Java8Immutables.merge(data.getListValuesToAppend(), name, values)));
    }
    
    
    
    @Override
    public Write removeListValue(String name, Object value) {
        ImmutableList<Object> values = data.getListValuesToRemove().get(name);
        values = (values == null) ? ImmutableList.of(value) : Java8Immutables.merge(values, value);

        return new UpdateQuery(getContext(), 
                               data.listValuesToRemove(Java8Immutables.merge(data.getListValuesToRemove(), name, values)));
    }
   
    
    @Override
    public Write putMapValue(String name, Object key, Object value) {
        ImmutableMap<Object, Optional<Object>> values = data.getMapValuesToMutate().get(name);
        values = (values == null) ? ImmutableMap.of(key, toOptional(value)) : Java8Immutables.merge(values, key, toOptional(value));

        return new UpdateQuery(getContext(), 
                               data.mapValuesToMutate(Java8Immutables.merge(data.getMapValuesToMutate(), name, values)));
    }
    
    

    @Override
    public Update<Write> onlyIf(Clause... conditions) {
        return new UpdateQuery(getContext(), 
                               data.onlyIfConditions(ImmutableList.<Clause>builder().addAll(data.getOnlyIfConditions())
                                                                                    .addAll(ImmutableList.copyOf(conditions))
                                                                                    .build()));
    }


    @Override
    public Insertion ifNotExits() {
        return new InsertQuery(getContext(), new WriteQueryDataAdapter().valuesToMutate(Java8Immutables.merge(data.getValuesToMutate(), Java8Immutables.transform(data.getKeys(), name -> name, value -> toOptional(value))))
                                                                     .ifNotExists(Optional.of(true)));
    }

        
    @Override
    public CounterMutationQuery incr(String name) {
        return incr(name, 1);
    }
    
    @Override
    public CounterMutationQuery incr(String name, long value) {
        return new CounterMutationQuery(getContext(), 
                                        new CounterMutationQueryData().keys(data.getKeys())
                                                                      .whereConditions(data.getWhereConditions())
                                                                      .name(name)
                                                                      .diff(value));  
    }
    
    
    @Override
    public CounterMutationQuery decr(String name) {
        return decr(name, 1);
    }
    
    @Override
    public CounterMutationQuery decr(String name, long value) {
        return new CounterMutationQuery(getContext(), 
                                        new CounterMutationQueryData().keys(data.getKeys())
                                                                      .whereConditions(data.getWhereConditions())
                                                                      .name(name)
                                                                      .diff(0 - value));  
    }
    
    
        
    private Statement getStatement() {
        WriteQueryData queryData = data;
        for (WriteQueryPreInterceptor interceptor : getContext().getInterceptorRegistry().getInterceptors(WriteQueryPreInterceptor.class)) {
            queryData = interceptor.onPreWrite(queryData);
        }
        
        return WriteQueryDataAdapter.toStatement(queryData, getContext());
    }
    
    
    @Override
    public CompletableFuture<Result> executeAsync() {
        return new CompletableDbFuture(performAsync(getStatement()))
                        .thenApply(resultSet -> newResult(resultSet))
                        .thenApply(result -> assertResultIsAppliedWhen(!data.getOnlyIfConditions().isEmpty(), result, "if condition does not match"));
    }
  
    
    private boolean isOptional(Object obj) {
        return (obj == null) ? false 
                             : (Optional.class.isAssignableFrom(obj.getClass()));
    }
 

    @SuppressWarnings("unchecked")
    private <T> Optional<T> toOptional(T obj) {
        return (obj == null) ? Optional.empty() 
                             : isOptional(obj) ? (Optional<T>) obj: Optional.of(obj);
    }
 

    
      
    private static final class CounterMutationQuery extends AbstractQuery<CounterMutation> implements CounterMutation {
        
        private final CounterMutationQueryData data;

        public CounterMutationQuery(Context ctx, CounterMutationQueryData data) {
            super(ctx);
            this.data = data;
        }
        
        @Override
        protected CounterMutation newQuery(Context newContext) {
            return new CounterMutationQuery(newContext, data);
        }
   
        @Override
        public CounterMutation withTtl(Duration ttl) {
            return newQuery(getContext().withTtl((int) ttl.getSeconds()));
        }
        
        @Override
        public CounterBatchMutation combinedWith(CounterBatchable other) {
            return new CounterBatchMutationQuery(getContext(), ImmutableList.of(this, other));
        }
   
        @Override
        public void addTo(BatchStatement batchStatement) {
            batchStatement.add(getStatement());
        }
        
        
        private Statement getStatement() {
            return data.toStatement(getContext());
        }

        
        public CompletableFuture<Result> executeAsync() {
            return new CompletableDbFuture(performAsync(getStatement()))
                            .thenApply(resultSet -> newResult(resultSet));
        }
    }
}


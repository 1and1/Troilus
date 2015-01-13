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



import java.util.Map;
import java.util.Map.Entry;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BatchStatement.Type;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Clause;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.unitedinternet.troilus.minimal.MinimalDao.BatchMutation;
import com.unitedinternet.troilus.minimal.MinimalDao.Batchable;
import com.unitedinternet.troilus.minimal.MinimalDao.CounterBatchMutation;
import com.unitedinternet.troilus.minimal.MinimalDao.CounterBatchable;
import com.unitedinternet.troilus.minimal.MinimalDao.CounterMutation;
import com.unitedinternet.troilus.minimal.MinimalDao.Insertion;
import com.unitedinternet.troilus.minimal.MinimalDao.Update;
import com.unitedinternet.troilus.minimal.MinimalDao.UpdateWithValuesAndCounter;
import com.unitedinternet.troilus.minimal.MinimalDao.Write;
import com.unitedinternet.troilus.minimal.MinimalDao.WriteWithCounter;
import com.unitedinternet.troilus.minimal.WriteQueryData;
import com.unitedinternet.troilus.minimal.WriteQueryPreInterceptor;


 
class MinimalUpdateQuery extends AbstractQuery<WriteWithCounter> implements WriteWithCounter, UpdateWithValuesAndCounter  {

    private final WriteQueryDataImpl data;
    
    
    public MinimalUpdateQuery(Context ctx, WriteQueryDataImpl data) {
        super(ctx);
        this.data = data;
    }
    
    @Override
    protected MinimalUpdateQuery newQuery(Context newContext) {
        return new MinimalUpdateQuery(newContext, data);
    }
    
    public MinimalUpdateQuery withTtl(long ttlSec) {
        return newQuery(getContext().withTtl(ttlSec));
    }
    
    public BatchMutation combinedWith(Batchable other) {
        return new MinimalBatchMutationQuery(getContext(), Type.LOGGED, ImmutableList.of(this, other));
    }
       
    @Override
    public void addTo(BatchStatement batchStatement) {
        batchStatement.add(getStatement());
    }
    
    
    public MinimalInsertQuery entity(Object entity) {
        return new MinimalInsertQuery(getContext(), 
                                      new WriteQueryDataImpl().valuesToMutate(getContext().getBeanMapper().toValues(entity)));
    }
    
    @Override
    public MinimalUpdateQuery value(String name, Object value) {
        return new MinimalUpdateQuery(getContext(), 
                                      data.valuesToMutate(Immutables.merge(data.getValuesToMutate(), name, Optional.fromNullable(value))));
    }
    

    @Override
    public <T> Write value(Name<T> name, T value) {
        return value(name.getName(), value);
    }
    
    @Override
    public MinimalUpdateQuery values(ImmutableMap<String, Object> nameValuePairsToAdd) {
        return new MinimalUpdateQuery(getContext(), 
                                      data.valuesToMutate(Immutables.merge(data.getValuesToMutate(), toOptional(nameValuePairsToAdd))));
    }


    @Override
    public MinimalUpdateQuery removeSetValue(String name, Object value) {
        ImmutableSet<Object> values = data.getSetValuesToRemove().get(name);
        values = (values == null) ? ImmutableSet.of(value) : Immutables.merge(values, value);

        return new MinimalUpdateQuery(getContext(), 
                                      data.setValuesToRemove(Immutables.merge(data.getSetValuesToRemove(), name, values)));
    }

    
    @Override
    public MinimalUpdateQuery addSetValue(String name, Object value) {
        ImmutableSet<Object> values = data.getSetValuesToAdd().get(name);
        values = (values == null) ? ImmutableSet.of(value): Immutables.merge(values, value);

        return new MinimalUpdateQuery(getContext(), 
                                      data.setValuesToAdd(Immutables.merge(data.getSetValuesToAdd(), name, values)));
    }
   
    
    @Override
    public Write prependListValue(String name, Object value) {
        ImmutableList<Object> values = data.getListValuesToPrepend().get(name);
        values = (values == null) ? ImmutableList.of(value) : Immutables.merge(values, value);

        return new MinimalUpdateQuery(getContext(), 
                                      data.listValuesToPrepend(Immutables.merge(data.getListValuesToPrepend(), name, values)));
    } 
    
    

    @Override
    public Write appendListValue(String name, Object value) {
        ImmutableList<Object> values = data.getListValuesToAppend().get(name);
        values = (values == null) ? ImmutableList.of(value) : Immutables.merge(values, value);

        return new MinimalUpdateQuery(getContext(), 
                                      data.listValuesToAppend(Immutables.merge(data.getListValuesToAppend(), name, values)));
    }
    
    
    
    @Override
    public Write removeListValue(String name, Object value) {
        ImmutableList<Object> values = data.getListValuesToRemove().get(name);
        values = (values == null) ? ImmutableList.of(value) : Immutables.merge(values, value);

        return new MinimalUpdateQuery(getContext(), 
                                      data.listValuesToRemove(Immutables.merge(data.getListValuesToRemove(), name, values)));
    }
   
    
    @Override
    public Write putMapValue(String name, Object key, Object value) {
        ImmutableMap<Object, Optional<Object>> values = data.getMapValuesToMutate().get(name);
        values = (values == null) ? ImmutableMap.of(key, toOptional(value)) : Immutables.merge(values, key, toOptional(value));

        return new MinimalUpdateQuery(getContext(), 
                                      data.mapValuesToMutate(Immutables.merge(data.getMapValuesToMutate(), name, values)));
    }
    
    

    @Override
    public Update<Write> onlyIf(Clause... conditions) {
        return new MinimalUpdateQuery(getContext(), 
                                      data.onlyIfConditions(ImmutableList.<Clause>builder().addAll(data.getOnlyIfConditions())
                                                                                           .addAll(ImmutableList.copyOf(conditions))
                                                                                           .build()));
    }


    @Override
    public Insertion ifNotExits() {
        return new MinimalInsertQuery(getContext(), new WriteQueryDataImpl().valuesToMutate(Immutables.merge(data.getValuesToMutate(), toOptional(data.getKeyNameValuePairs())))
                                                                               .ifNotExists(true));
    }

        
    @Override
    public CounterMutationQuery incr(String name) {
        return incr(name, 1);
    }
    
    @Override
    public CounterMutationQuery incr(String name, long value) {
        return new CounterMutationQuery(getContext(), 
                                        new CounterMutationQueryData().keys(data.getKeyNameValuePairs())
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
                                        new CounterMutationQueryData().keys(data.getKeyNameValuePairs())
                                                                      .whereConditions(data.getWhereConditions())
                                                                      .name(name)
                                                                      .diff(0 - value));  
    }
    
    
        
    private Statement getStatement() {
        WriteQueryData queryData = data;
        for (WriteQueryPreInterceptor interceptor : getContext().getInterceptorRegistry().getInterceptors(WriteQueryPreInterceptor.class)) {
            queryData = interceptor.onPreWrite(queryData);
        }
        
        return WriteQueryDataImpl.toStatement(queryData, getContext());
    }
    
    
    @Override
    public Result execute() {
        Result result = newResult(performAsync(getStatement()).getUninterruptibly());
        assertResultIsAppliedWhen(!data.getOnlyIfConditions().isEmpty(), result, "if condition does not match");
        
        return result;
    }
    

    private boolean isOptional(Object obj) {
        return (obj == null) ? false 
                             : (Optional.class.isAssignableFrom(obj.getClass()));
    }
 

    @SuppressWarnings("unchecked")
    private <T> Optional<T> toOptional(T obj) {
        return (obj == null) ? Optional.<T>absent() 
                             : isOptional(obj) ? (Optional<T>) obj: Optional.of(obj);
    }
 


    private ImmutableMap<String, Optional<Object>> toOptional(ImmutableMap<String, Object> map) {
        Map<String, Optional<Object>> result = Maps.newHashMap();
        
        for (Entry<String, Object> entry : map.entrySet()) {
            result.put(entry.getKey(), toOptional(entry.getValue()));
        }
        
        return ImmutableMap.copyOf(result);
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
        public CounterMutation withTtl(long ttlSec) {
            return newQuery(getContext().withTtl(ttlSec));
        }
        
        @Override
        public CounterBatchMutation combinedWith(CounterBatchable other) {
            return new MinimalCounterBatchMutationQuery(getContext(), ImmutableList.of(this, other));
        }
   
        @Override
        public void addTo(BatchStatement batchStatement) {
            batchStatement.add(getStatement());
        }
        
        
        private Statement getStatement() {
            return data.toStatement(getContext());
        }

        @Override
        public Result execute() {
            return newResult(performAsync(getStatement()).getUninterruptibly());
        }
    }
}


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

import java.util.List;
import java.util.Map;
import java.util.Set;

import net.oneandone.troilus.java7.Batchable;
import net.oneandone.troilus.java7.UpdateWithUnitAndCounter;
import net.oneandone.troilus.java7.interceptor.WriteQueryData;

import com.datastax.driver.core.querybuilder.Clause;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;



 
/**
 * update query implementation
 */
class UpdateQuery extends WriteQuery<UpdateWithUnitAndCounter> implements UpdateWithUnitAndCounter  {
     
    
    /**
     * @param ctx   the context 
     * @param data  the query data
     */
    UpdateQuery(Context ctx, WriteQueryData data) {
        super(ctx, data);
    }

    
    ////////////////////
    // factory methods
    
    @Override
    protected UpdateQuery newQuery(Context newContext) {
        return new UpdateQuery(newContext, getData());
    }
    
    private UpdateQuery newQuery(WriteQueryData data) {
        return new UpdateQuery(getContext(), data);
    }

    // 
    ////////////////////

    
    
    @Override
    public BatchMutationQuery combinedWith(Batchable<?> other) {
        return new BatchMutationQuery(getContext(), this, other);
    }

    /**
     * @param entity   the entity to insert
     * @return the new insert query
     */@Override
     public UpdateQuery entity(Object entity) {
        ImmutableMap<String, Optional<Object>> values = getBeanMapper().toValues(entity, getCatalog().getColumnNames(getData().getTablename()));
        return newQuery(getData().valuesToMutate(Immutables.join(getData().getValuesToMutate(), values)));
    }
    
    @Override
    public UpdateQuery withTtl(int ttlSec) {
        return newQuery(getContext().withTtl(ttlSec));
    }
    
    @Override
    public UpdateQuery value(String name, Object value) {
        return newQuery(getData().valuesToMutate(Immutables.join(getData().getValuesToMutate(), name, Optionals.toGuavaOptional(value))));
    }
    
    @Override
    public <T> UpdateQuery value(ColumnName<T> name, T value) {
        return value(name.getName(), value);
    }
    
    @Override
    public UpdateQuery values(ImmutableMap<String, Object> nameValuePairsToAdd) {
        return newQuery(getData().valuesToMutate(Immutables.join(getData().getValuesToMutate(), Optionals.toGuavaOptional(nameValuePairsToAdd))));
    }

    @Override
    public UpdateQuery removeSetValue(String name, Object value) {
        ImmutableSet<Object> values = getData().getSetValuesToRemove().get(name);
        values = (values == null) ? ImmutableSet.of(value) : Immutables.join(values, value);

        return newQuery(getData().setValuesToRemove(Immutables.join(getData().getSetValuesToRemove(), name, values)));
    }
  
    @Override
    public <T> UpdateWithUnitAndCounter removeSetValue(ColumnName<Set<T>> name, T value) {
        return removeSetValue(name.getName(), value);
    }

    @Override
    public UpdateQuery addSetValue(String name, Object value) {
        ImmutableSet<Object> values = getData().getSetValuesToAdd().get(name);
        values = (values == null) ? ImmutableSet.of(value): Immutables.join(values, value);

        return newQuery(getData().setValuesToAdd(Immutables.join(getData().getSetValuesToAdd(), name, values)));
    }
    

    @Override
    public <T> UpdateWithUnitAndCounter addSetValue(ColumnName<Set<T>> name, T value) {
        return addSetValue(name.getName(), value);
    }
   
    @Override
    public UpdateQuery prependListValue(String name, Object value) {
        ImmutableList<Object> values = getData().getListValuesToPrepend().get(name);
        values = (values == null) ? ImmutableList.of(value) : Immutables.join(values, value);

        return newQuery(getData().listValuesToPrepend(Immutables.join(getData().getListValuesToPrepend(), name, values)));
    } 
    
    @Override
    public <T> UpdateWithUnitAndCounter prependListValue(ColumnName<List<T>> name, T value) {
        return prependListValue(name.getName(), value);
    }
    
    @Override
    public UpdateQuery appendListValue(String name, Object value) {
        ImmutableList<Object> values = getData().getListValuesToAppend().get(name);
        values = (values == null) ? ImmutableList.of(value) : Immutables.join(values, value);

        return newQuery(getData().listValuesToAppend(Immutables.join(getData().getListValuesToAppend(), name, values)));
    }
    
    @Override
    public <T> UpdateWithUnitAndCounter appendListValue(ColumnName<List<T>> name, T value) {
        return appendListValue(name.getName(), value);
    }
    
    @Override
    public UpdateQuery removeListValue(String name, Object value) {
        ImmutableList<Object> values = getData().getListValuesToRemove().get(name);
        values = (values == null) ? ImmutableList.of(value) : Immutables.join(values, value);

        return newQuery(getData().listValuesToRemove(Immutables.join(getData().getListValuesToRemove(), name, values)));
    }
    
    
    @Override
    public <T> UpdateWithUnitAndCounter removeListValue(ColumnName<List<T>> name, T value) {
        return removeListValue(name.getName(), value);
    }
   
    @Override
    public UpdateQuery putMapValue(String name, Object key, Object value) {
        ImmutableMap<Object, Optional<Object>> values = getData().getMapValuesToMutate().get(name);
        values = addToMap(name, key, value, values);
        
        return newQuery(getData().mapValuesToMutate(Immutables.join(getData().getMapValuesToMutate(), name, values)));
    }
    
    @Override
    public <T, V> UpdateWithUnitAndCounter putMapValue(ColumnName<Map<T, V>> name, T key, V value) {
       return putMapValue(name.getName(), key, value);
    }
    
    public <T, V> UpdateQuery putMapValues(String columnName, Map<T, V> map) {
        ImmutableMap<Object, Optional<Object>> values = getData().getMapValuesToMutate().get(columnName);
        for(Map.Entry<T, V> entry : map.entrySet()) {
        	values = addToMap(columnName, entry.getKey(), entry.getValue(), values);
        }
       
        return newQuery(getData().mapValuesToMutate(Immutables.join(getData().getMapValuesToMutate(), columnName, values)));
    }
    
    
    @Override
    public UpdateQuery onlyIf(Clause... conditions) {
        return newQuery(getData().onlyIfConditions(ImmutableList.<Clause>builder().addAll(getData().getOnlyIfConditions())
                                                                                  .addAll(ImmutableList.copyOf(conditions))
                                                                                  .build()));
    }
 }


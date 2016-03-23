
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
import java.util.Optional;
import java.util.Set;

import com.datastax.driver.core.querybuilder.Clause;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;



 
/**
 * update query implementation
 */
class WriteWithCounterQuery extends WriteQuery<WriteWithCounter> implements WriteWithCounter  {
    
    /**
     * @param ctx   the context 
     * @param data  the query data
     */
    WriteWithCounterQuery(final Context ctx, final WriteQueryData data) {
        super(ctx, data);
    }

    
    ////////////////////
    // factory methods
    
    @Override
    protected WriteWithCounterQuery newQuery(final Context newContext) {
        return new WriteWithCounterQuery(newContext, getData());
    }
    
    private WriteWithCounterQuery newQuery(final WriteQueryData data) {
        return new WriteWithCounterQuery(getContext(), data);
    }

    // 
    ////////////////////

    

    @Override
    public BatchMutationQuery combinedWith(final Batchable<?> other) {
        return new BatchMutationQuery(getContext(), this, other);
    }
    
    /**
     * @param entity   the entity to insert
     * @return the new insert query
     */@Override
     public WriteWithCounterQuery entity(final Object entity) {
        ImmutableMap<String, Optional<Object>> values = getBeanMapper().toValues(entity, getCatalog().getColumnNames(getData().getTablename()));
        return newQuery(getData().valuesToMutate(Immutables.join(getData().getValuesToMutate(), values)));
    }
    
    @Override
    public WriteWithCounterQuery withTtl(final Duration ttlSec) {
        return newQuery(getContext().withTtl(ttlSec));
    }
    
    @Override
    public WriteWithCounterQuery value(final String name, final Object value) {
        return newQuery(getData().valuesToMutate(Immutables.join(getData().getValuesToMutate(), name, Optionals.toOptional(value))));
    }
    
    @Override
    public <T> WriteWithCounterQuery value(final ColumnName<T> name, final T value) {
        return value(name.getName(), value);
    }
    
    @Override
    public WriteWithCounterQuery values(final ImmutableMap<String, Object> nameValuePairsToAdd) {
        return newQuery(getData().valuesToMutate(Immutables.join(getData().getValuesToMutate(), Optionals.toOptional(nameValuePairsToAdd))));
    }

    @Override
    public WriteWithCounterQuery removeSetValue(final String name, final Object value) {
        ImmutableSet<Object> values = getData().getSetValuesToRemove().get(name);
        values = (values == null) ? ImmutableSet.of(value) : Immutables.join(values, value);

        return newQuery(getData().setValuesToRemove(Immutables.join(getData().getSetValuesToRemove(), name, values)));
    }
    
    @Override
    public <T> WriteWithCounterQuery removeSetValue(final ColumnName<Set<T>> name, final T value) {
        return removeSetValue(name.getName(), value);
    }

    @Override
    public WriteWithCounterQuery addSetValue(final String name, final Object value) {
        ImmutableSet<Object> values = getData().getSetValuesToAdd().get(name);
        values = (values == null) ? ImmutableSet.of(value): Immutables.join(values, value);

        return newQuery(getData().setValuesToAdd(Immutables.join(getData().getSetValuesToAdd(), name, values)));
    }
    
    @Override
    public <T> WriteWithCounterQuery addSetValue(final ColumnName<Set<T>> name, final T value) {
        return addSetValue(name.getName(), value);
    }
   
    @Override
    public WriteWithCounterQuery prependListValue(final String name, final Object value) {
        ImmutableList<Object> values = getData().getListValuesToPrepend().get(name);
        values = (values == null) ? ImmutableList.of(value) : Immutables.join(value, values);

        return newQuery(getData().listValuesToPrepend(Immutables.join(getData().getListValuesToPrepend(), name, values)));
    } 
    
    @Override
    public <T> WriteWithCounterQuery prependListValue(final ColumnName<List<T>> name, final T value) {
        return prependListValue(name.getName(), value);
    }
    
    @Override
    public WriteWithCounterQuery appendListValue(final String name, final Object value) {
        ImmutableList<Object> values = getData().getListValuesToAppend().get(name);
        values = (values == null) ? ImmutableList.of(value) : Immutables.join(values, value);

        return newQuery(getData().listValuesToAppend(Immutables.join(getData().getListValuesToAppend(), name, values)));
    }
    
    @Override
    public <T> WriteWithCounterQuery appendListValue(final ColumnName<List<T>> name, final T value) {
        return appendListValue(name.getName(), value);
    }
    
    @Override
    public WriteWithCounterQuery removeListValue(final String name, final Object value) {
        ImmutableList<Object> values = getData().getListValuesToRemove().get(name);
        values = (values == null) ? ImmutableList.of(value) : Immutables.join(values, value);

        return newQuery(getData().listValuesToRemove(Immutables.join(getData().getListValuesToRemove(), name, values)));
    }
    
    @Override
    public <T> WriteWithCounterQuery removeListValue(final ColumnName<List<T>> name, final T value) {
        return removeListValue(name.getName(), value);
    }
   
    @Override
    public WriteWithCounterQuery putMapValue(final String name, final Object key, final Object value) {
        ImmutableMap<Object, Optional<Object>> values = getData().getMapValuesToMutate().get(name);
        values = addToMap(name, key, value, values);

        return newQuery(getData().mapValuesToMutate(Immutables.join(getData().getMapValuesToMutate(), name, values)));
    }

    
    @Override
    public <T, V> WriteWithCounterQuery putMapValue(final ColumnName<Map<T, V>> name, final T key, final V value) {
        return putMapValue(name.getName(), key, value);
    }
    
    @Override
    public WriteWithCounterQuery onlyIf(final Clause... conditions) {
        return newQuery(getData().onlyIfConditions(ImmutableList.<Clause>builder().addAll(getData().getOnlyIfConditions())
                                                                                  .addAll(ImmutableList.copyOf(conditions))
                                                                                  .build()));
    }

    @Override
    public InsertQuery ifNotExists() {
        return new InsertQuery(getContext(), new WriteQueryDataImpl(getData().getTablename()).valuesToMutate(Immutables.join(getData().getValuesToMutate(), Optionals.toOptional(getData().getKeys())))
                                                                                             .ifNotExists(true));
    }
}
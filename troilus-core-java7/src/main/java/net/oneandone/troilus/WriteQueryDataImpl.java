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


import static com.datastax.driver.core.querybuilder.QueryBuilder.addAll;
import static com.datastax.driver.core.querybuilder.QueryBuilder.appendAll;
import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.discardAll;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.insertInto;
import static com.datastax.driver.core.querybuilder.QueryBuilder.prependAll;
import static com.datastax.driver.core.querybuilder.QueryBuilder.putAll;
import static com.datastax.driver.core.querybuilder.QueryBuilder.removeAll;
import static com.datastax.driver.core.querybuilder.QueryBuilder.set;
import static com.datastax.driver.core.querybuilder.QueryBuilder.ttl;
import static com.datastax.driver.core.querybuilder.QueryBuilder.update;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import net.oneandone.troilus.java7.interceptor.WriteQueryData;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;


 
/**
 * write query data implementation
 *
 */
class WriteQueryDataImpl implements WriteQueryData {

    private final String tablename;
    
    private final ImmutableMap<String, Object> keys;
    private final ImmutableList<Clause> whereConditions;
    
    private final ImmutableMap<String, Optional<Object>> valuesToMutate;
    private final ImmutableMap<String, ImmutableSet<Object>> setValuesToAdd;
    private final ImmutableMap<String, ImmutableSet<Object>> setValuesToRemove;
    private final ImmutableMap<String, ImmutableList<Object>> listValuesToAppend;
    private final ImmutableMap<String, ImmutableList<Object>> listValuesToPrepend;
    private final ImmutableMap<String, ImmutableList<Object>> listValuesToRemove;
    private final ImmutableMap<String, ImmutableMap<Object, Optional<Object>>> mapValuesToMutate;
    
    private final ImmutableList<Clause> onlyIfConditions;
    private final Boolean ifNotExists;
    
    

    /**
     * constructor
     */
    WriteQueryDataImpl(String tablename) {
        this(tablename,
             ImmutableMap.<String, Object>of(),
             ImmutableList.<Clause>of(),
             ImmutableMap.<String, Optional<Object>>of(),
             ImmutableMap.<String, ImmutableSet<Object>>of(),
             ImmutableMap.<String, ImmutableSet<Object>>of(),
             ImmutableMap.<String, ImmutableList<Object>>of(),
             ImmutableMap.<String, ImmutableList<Object>>of(),
             ImmutableMap.<String, ImmutableList<Object>>of(),
             ImmutableMap.<String, ImmutableMap<Object, Optional<Object>>>of(),
             ImmutableList.<Clause>of(),
             null);
    }

    
    private WriteQueryDataImpl(String tablemname,
                               ImmutableMap<String, Object> keys, 
                               ImmutableList<Clause> whereConditions, 
                               ImmutableMap<String, Optional<Object>> valuesToMutate, 
                               ImmutableMap<String, ImmutableSet<Object>> setValuesToAdd,
                               ImmutableMap<String, ImmutableSet<Object>> setValuesToRemove,
                               ImmutableMap<String, ImmutableList<Object>> listValuesToAppend, 
                               ImmutableMap<String, ImmutableList<Object>> listValuesToPrepend,
                               ImmutableMap<String, ImmutableList<Object>> listValuesToRemove,
                               ImmutableMap<String, ImmutableMap<Object, Optional<Object>>> mapValuesToMutate,
                               ImmutableList<Clause> onlyIfConditions,
                               Boolean ifNotExists) {
        this.tablename = tablemname;
        this.keys = keys;
        this.whereConditions = whereConditions;
        this.valuesToMutate = valuesToMutate;
        this.setValuesToAdd = setValuesToAdd;
        this.setValuesToRemove = setValuesToRemove;
        this.listValuesToAppend = listValuesToAppend;
        this.listValuesToPrepend = listValuesToPrepend;
        this.listValuesToRemove = listValuesToRemove;
        this.mapValuesToMutate = mapValuesToMutate;
        this.onlyIfConditions = onlyIfConditions;
        this.ifNotExists = ifNotExists;
    }
    
    
    @Override
    public WriteQueryDataImpl keys(ImmutableMap<String, Object> keys) {
        return new WriteQueryDataImpl(this.tablename,
                                      keys, 
                                      this.whereConditions,
                                      this.valuesToMutate, 
                                      this.setValuesToAdd,
                                      this.setValuesToRemove,
                                      this.listValuesToAppend,
                                      this.listValuesToPrepend,
                                      this.listValuesToRemove,
                                      this.mapValuesToMutate,
                                      this.onlyIfConditions,
                                      this.ifNotExists);
    }
    
    @Override
    public WriteQueryDataImpl whereConditions(ImmutableList<Clause> whereConditions) {
        return new WriteQueryDataImpl(this.tablename,
                                      this.keys, 
                                      whereConditions,
                                      this.valuesToMutate, 
                                      this.setValuesToAdd,
                                      this.setValuesToRemove,
                                      this.listValuesToAppend,
                                      this.listValuesToPrepend,
                                      this.listValuesToRemove,
                                      this.mapValuesToMutate,
                                      this.onlyIfConditions,
                                      this.ifNotExists);
    }
    
    @Override
    public WriteQueryDataImpl valuesToMutate(ImmutableMap<String, Optional<Object>> valuesToMutate) {
        return new WriteQueryDataImpl(this.tablename,
                                      this.keys, 
                                      this.whereConditions,
                                      valuesToMutate, 
                                      this.setValuesToAdd,
                                      this.setValuesToRemove,
                                      this.listValuesToAppend,
                                      this.listValuesToPrepend,
                                      this.listValuesToRemove,
                                      this.mapValuesToMutate,
                                      this.onlyIfConditions,
                                      this.ifNotExists);
    }
 
    @Override
    public WriteQueryDataImpl setValuesToAdd(ImmutableMap<String, ImmutableSet<Object>> setValuesToAdd) {
        return new WriteQueryDataImpl(this.tablename,
                                      this.keys, 
                                      this.whereConditions,
                                      this.valuesToMutate, 
                                      setValuesToAdd,
                                      this.setValuesToRemove,
                                      this.listValuesToAppend,
                                      this.listValuesToPrepend,
                                      this.listValuesToRemove,
                                      this.mapValuesToMutate,
                                      this.onlyIfConditions,
                                      this.ifNotExists);
    }
    
    @Override
    public WriteQueryDataImpl setValuesToRemove(ImmutableMap<String, ImmutableSet<Object>> setValuesToRemove) {
        return new WriteQueryDataImpl(this.tablename,
                                      this.keys, 
                                      this.whereConditions,
                                      this.valuesToMutate, 
                                      this.setValuesToAdd,
                                      setValuesToRemove,
                                      this.listValuesToAppend,
                                      this.listValuesToPrepend,
                                      this.listValuesToRemove,
                                      this.mapValuesToMutate,
                                      this.onlyIfConditions,
                                      this.ifNotExists);
    }
 
    @Override
    public WriteQueryDataImpl listValuesToAppend(ImmutableMap<String, ImmutableList<Object>> listValuesToAppend) {
        return new WriteQueryDataImpl(this.tablename,
                                      this.keys, 
                                      this.whereConditions,
                                      this.valuesToMutate, 
                                      this.setValuesToAdd,
                                      this.setValuesToRemove,
                                      listValuesToAppend,
                                      this.listValuesToPrepend,
                                      this.listValuesToRemove,
                                      this.mapValuesToMutate,
                                      this.onlyIfConditions,
                                      this.ifNotExists);
    }
   
    @Override
    public WriteQueryDataImpl listValuesToPrepend(ImmutableMap<String, ImmutableList<Object>> listValuesToPrepend) {
        return new WriteQueryDataImpl(this.tablename,
                                      this.keys, 
                                      this.whereConditions,
                                      this.valuesToMutate, 
                                      this.setValuesToAdd,
                                      this.setValuesToRemove,
                                      this.listValuesToAppend,
                                      listValuesToPrepend,
                                      this.listValuesToRemove,
                                      this.mapValuesToMutate,
                                      this.onlyIfConditions,
                                      this.ifNotExists);
    }
 
    @Override
    public WriteQueryDataImpl listValuesToRemove(ImmutableMap<String, ImmutableList<Object>> listValuesToRemove) {
         return new WriteQueryDataImpl(this.tablename,
                                       this.keys, 
                                       this.whereConditions,
                                       this.valuesToMutate, 
                                       this.setValuesToAdd,
                                       this.setValuesToRemove,
                                       this.listValuesToAppend,
                                       this.listValuesToPrepend,
                                       listValuesToRemove,
                                       this.mapValuesToMutate,
                                       this.onlyIfConditions,
                                       this.ifNotExists);
    }
 
    @Override
    public WriteQueryDataImpl mapValuesToMutate(ImmutableMap<String, ImmutableMap<Object, Optional<Object>>> mapValuesToMutate) {
        return new WriteQueryDataImpl(this.tablename,
                                      this.keys, 
                                      this.whereConditions,
                                      this.valuesToMutate, 
                                      this.setValuesToAdd,
                                      this.setValuesToRemove,
                                      this.listValuesToAppend,
                                      this.listValuesToPrepend,
                                      this.listValuesToRemove,
                                      mapValuesToMutate,
                                      this.onlyIfConditions,
                                      this.ifNotExists);
    }

    @Override
    public WriteQueryDataImpl onlyIfConditions(ImmutableList<Clause> onlyIfConditions) {
        return new WriteQueryDataImpl(this.tablename,
                                      this.keys, 
                                      this.whereConditions,
                                      this.valuesToMutate, 
                                      this.setValuesToAdd,
                                      this.setValuesToRemove,
                                      this.listValuesToAppend,
                                      this.listValuesToPrepend,
                                      this.listValuesToRemove,
                                      this.mapValuesToMutate,
                                      onlyIfConditions,
                                      this.ifNotExists);
    }

    @Override
    public WriteQueryDataImpl ifNotExists(Boolean ifNotExists) {
        return new WriteQueryDataImpl(this.tablename,
                                      this.keys, 
                                      this.whereConditions,
                                      this.valuesToMutate, 
                                      this.setValuesToAdd,
                                      this.setValuesToRemove,
                                      this.listValuesToAppend,
                                      this.listValuesToPrepend,
                                      this.listValuesToRemove,
                                      this.mapValuesToMutate,
                                      this.onlyIfConditions,
                                      ifNotExists);
    }
    
    @Override
    public String getTablename() {
        return tablename;
    }
    
    @Override
    public ImmutableMap<String, Object> getKeys() {
        return keys;
    }

    @Override
    public <T> boolean hasKey(ColumnName<T> name) {
        return hasKey(name.getName());
    }
    
    @Override
    public boolean hasKey(String name) {
        return getKeys().containsKey(name);
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public <T> T getKey(ColumnName<T> name) {
        return (T) getKey(name.getName());
    }

    @Override
    public Object getKey(String name) {
        return getKeys().get(name);
    }
    
    @Override
    public ImmutableList<Clause> getWhereConditions() {
        return whereConditions;
    }

    @Override
    public ImmutableMap<String, Optional<Object>> getValuesToMutate() {
        return valuesToMutate;
    }

    @Override
    public <T> boolean hasValueToMutate(ColumnName<T> name) {
        return hasValueToMutate(name.getName());
    }
    
    @Override
    public boolean hasValueToMutate(String name) {
        return getValuesToMutate().containsKey(name);
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public <T> T getValueToMutate(ColumnName<T> name) {
        return (T) getValueToMutate(name.getName());
    }
    
    @Override
    public Object getValueToMutate(String name) {
        Optional<Object> optional = getValuesToMutate().get(name);
        if (optional == null) {
            return null;
        } else {
            return optional.orNull();
        }
    }
    
    @Override
    public ImmutableMap<String, ImmutableSet<Object>> getSetValuesToAdd() {
        return setValuesToAdd;
    }
    
    @Override
    public <T> boolean hasSetValuesToAdd(ColumnName<Set<T>> name) {
        return hasSetValuesToAdd(name.getName());
    }
    
    @Override
    public boolean hasSetValuesToAdd(String name) {
        return setValuesToAdd.containsKey(name);
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public <T> ImmutableSet<T> getSetValuesToAdd(ColumnName<Set<T>> name) {
        return (ImmutableSet<T>) getSetValuesToAdd(name.getName());
    }
    
    @Override
    public ImmutableSet<Object> getSetValuesToAdd(String name) {
        ImmutableSet<Object> values = setValuesToAdd.get(name);
        if (values == null) {
            return ImmutableSet.of();
        } else {
            return values;
        }
    }    
    
    @Override
    public <T> boolean hasSetValuesToAddOrSet(ColumnName<Set<T>> name) {
        return hasSetValuesToAddOrSet(name.getName());
    }
    
    @Override
    public boolean hasSetValuesToAddOrSet(String name) {
        return hasSetValuesToAdd(name) || hasValueToMutate(name);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> ImmutableSet<T> getSetValuesToAddOrSet(ColumnName<Set<T>> name) {
        return (ImmutableSet<T>) getSetValuesToAddOrSet(name.getName());
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public ImmutableSet<Object> getSetValuesToAddOrSet(String name) {
        ImmutableSet<Object> valuesToAdd = getSetValuesToAdd(name);
        ImmutableSet<Object> valuesToMutate = (ImmutableSet<Object>) getValueToMutate(name);
        if (valuesToMutate == null) {
            return valuesToAdd;
        } else {
            return ImmutableSet.<Object>builder().addAll(valuesToAdd).addAll(valuesToMutate).build();
        }
    }

    @Override
    public ImmutableMap<String, ImmutableSet<Object>> getSetValuesToRemove() {
        return setValuesToRemove;
    }
    
    @Override
    public <T> boolean hasSetValuesToRemove(ColumnName<Set<T>> name) {
        return hasSetValuesToRemove(name.getName());
    }
    
    @Override
    public boolean hasSetValuesToRemove(String name) {
        return setValuesToRemove.containsKey(name);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> ImmutableSet<T> getSetValuesToRemove(ColumnName<Set<T>> name) {
        return (ImmutableSet<T>) getSetValuesToRemove(name.getName());
    }
    
    @Override
    public ImmutableSet<Object> getSetValuesToRemove(String name) {
        ImmutableSet<Object> values = setValuesToRemove.get(name);
        if (values == null) {
            return ImmutableSet.of();
        } else {
            return values;
        }
    }
    
    
    @Override
    public ImmutableMap<String, ImmutableList<Object>> getListValuesToAppend() {
        return listValuesToAppend;
    }
    
    @Override
    public <T> boolean hasListValuesToAppend(ColumnName<List<T>> name) {
        return hasListValuesToAppend(name.getName());
    }
    
    @Override
    public boolean hasListValuesToAppend(String name) {
        return listValuesToAppend.containsKey(name);
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public <T> ImmutableList<T> getListValuesToAppend(ColumnName<List<T>> name) {
        return (ImmutableList<T>) getListValuesToAppend(name.getName());
    }
    
    @Override
    public ImmutableList<Object> getListValuesToAppend(String name) {
        ImmutableList<Object> values = listValuesToAppend.get(name);
        if (values == null) {
            return ImmutableList.of();
        } else  {
            return values;
        }
    }
    
    @Override
    public ImmutableMap<String, ImmutableList<Object>> getListValuesToPrepend() {
        return listValuesToPrepend;
    }
    
    @Override
    public <T> boolean hasListValuesToPrepend(ColumnName<List<T>> name) {
        return hasListValuesToPrepend(name.getName());
    }
    
    @Override
    public boolean hasListValuesToPrepend(String name) {
        return listValuesToPrepend.containsKey(name);
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public <T> ImmutableList<T> getListValuesToPrepend(ColumnName<List<T>> name) {
        return (ImmutableList<T>) getListValuesToPrepend(name.getName());
    }
    
    @Override
    public ImmutableList<Object> getListValuesToPrepend(String name) {
        ImmutableList<Object> values = listValuesToPrepend.get(name);
        if (values == null) {
            return ImmutableList.of();
        } else {
            return values;
        }
    }
    
    @Override
    public <T> boolean hasListValuesToAddOrSet(ColumnName<List<T>> name) {
        return hasListValuesToAddOrSet(name.getName());
    }
    
    @Override
    public boolean hasListValuesToAddOrSet(String name) {
        return hasListValuesToAppend(name) || hasListValuesToPrepend(name) || hasValueToMutate(name);
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public <T> ImmutableList<T> getListValuesToAddOrSet(ColumnName<List<T>> name) {
        return (ImmutableList<T>) getListValuesToAddOrSet(name.getName());
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public ImmutableList<Object> getListValuesToAddOrSet(String name) {
        ImmutableList<Object> valuesToAppend = getListValuesToAppend(name);
        ImmutableList<Object> valuesToPrepend = getListValuesToPrepend(name);
        ImmutableList<Object> valuesToMutate = (ImmutableList<Object>) getValueToMutate(name);
        
        if (valuesToMutate == null) {
            return ImmutableList.<Object>builder().addAll(valuesToAppend).addAll(valuesToPrepend).build();
        } else {
            return ImmutableList.<Object>builder().addAll(valuesToAppend).addAll(valuesToPrepend).addAll(valuesToMutate).build();
        }
    }
    
    @Override
    public ImmutableMap<String, ImmutableList<Object>> getListValuesToRemove() {
        return listValuesToRemove;
    }
    
    @Override
    public <T> boolean hasListValuesToRemove(ColumnName<List<T>> name) {
        return hasListValuesToRemove(name.getName());
    }
    
    @Override
    public boolean hasListValuesToRemove(String name) {
        return listValuesToRemove.containsKey(name);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> ImmutableList<T> getListValuesToRemove(ColumnName<List<T>> name) {
        return (ImmutableList<T>) getListValuesToRemove(name.getName());
    }
    
    @Override
    public ImmutableList<Object> getListValuesToRemove(String name) {
        ImmutableList<Object> values = listValuesToRemove.get(name);
        if (values == null) {
            return ImmutableList.of();
        } else {
            return values;
        }
    }
    
    @Override
    public ImmutableMap<String, ImmutableMap<Object, Optional<Object>>> getMapValuesToMutate() {
        return mapValuesToMutate;
    }
    
    @Override
    public <T, V> boolean hasMapValuesToMutate(ColumnName<Map<T, V>> name) {
        return hasMapValuesToMutate(name.getName());
    }
    
    @Override
    public boolean hasMapValuesToMutate(String name) {
        return mapValuesToMutate.containsKey(name);
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public <T, V> ImmutableMap<T, Optional<V>> getMapValuesToMutate(ColumnName<Map<T, V>> name) {
        Map<T, Optional<V>> result = Maps.newHashMap();
        for (Entry<Object, Optional<Object>> entry : getMapValuesToMutate(name.getName()).entrySet()) {
            result.put((T) entry.getKey(), (Optional<V>) entry.getValue()); 
        }
        return ImmutableMap.copyOf(result);
    }
    
    @Override
    public ImmutableMap<Object, Optional<Object>> getMapValuesToMutate(String name) {
        ImmutableMap<Object, Optional<Object>> values = mapValuesToMutate.get(name);
        if (values == null) {
            return ImmutableMap.of();
        } else {
            return values;
        }
    }

    @Override
    public ImmutableList<Clause> getOnlyIfConditions() {
        return onlyIfConditions;
    }
    
    @Override
    public Boolean getIfNotExits() {
        return ifNotExists;
    }
    
    
  
    
    /**
     * @param data   the query data
     * @param ctx    the context
     * @return the query data as statement
     */
    static ListenableFuture<Statement> toStatementAsync(WriteQueryData data, Context ctx) {
        
        if (isKeyOnlyStatement(data)) {
            Map<String, Optional<Object>> valuesToMUtate = Maps.newHashMap();
            for (Entry<String, Object> entry : data.getKeys().entrySet()) {
                valuesToMUtate.put(entry.getKey(), Optional.of(entry.getValue()));
            }
            
            data = data.valuesToMutate(ImmutableMap.copyOf(valuesToMUtate)).keys(ImmutableMap.<String, Object>of());
        }
        
        
        if ((data.getIfNotExits() != null) || (data.getKeys().isEmpty() && data.getWhereConditions().isEmpty())) {
            return toInsertStatementAsync(data, ctx);
        } else {
            return toUpdateStatementAsync(data, ctx);
        }
    }
    
    
    private static ListenableFuture<Statement> toInsertStatementAsync(WriteQueryData data,Context ctx) {
        Insert insert = insertInto(data.getTablename());
        
        List<Object> values = Lists.newArrayList();
        
        for(Entry<String, Optional<Object>> entry : data.getValuesToMutate().entrySet()) {
            insert.value(entry.getKey(), bindMarker());  
            values.add(ctx.toStatementValue(data.getTablename(), entry.getKey(), entry.getValue().orNull())); 
        }
        
        if (data.getIfNotExits() != null) {
            insert.ifNotExists();
            if (ctx.getSerialConsistencyLevel() != null) {
                insert.setSerialConsistencyLevel(ctx.getSerialConsistencyLevel());
            }
        }

        if (ctx.getTtlSec() != null) {
            insert.using(ttl(bindMarker()));  
            values.add((Integer) ctx.getTtlSec());
        }

        
        ListenableFuture<PreparedStatement> preparedStatementFuture = ctx.getDbSession().prepareAsync(insert);
        return ctx.getDbSession().bindAsync(preparedStatementFuture, values.toArray());
    }
    
    
    
    
    private static ListenableFuture<Statement> toUpdateStatementAsync(WriteQueryData data, Context ctx) {
        com.datastax.driver.core.querybuilder.Update update = update(data.getTablename());
        
        
        for (Clause onlyIfCondition : data.getOnlyIfConditions()) {
            update.onlyIf(onlyIfCondition);
        }

        
        // key-based update
        if (data.getWhereConditions().isEmpty()) {
            List<Object> values = Lists.newArrayList();
            
            
            if (ctx.getTtlSec() != null) {
                update.using(QueryBuilder.ttl(bindMarker())); 
                values.add((Integer) ctx.getTtlSec()); 
            }
            
            for (Entry<String, Optional<Object>> entry : data.getValuesToMutate().entrySet()) {
                update.with(set(entry.getKey(), bindMarker())); 
                values.add(toStatementValue(ctx, data.getTablename(), entry.getKey(), entry.getValue().orNull()));
            }

            for (Entry<String, ImmutableSet<Object>> entry : data.getSetValuesToAdd().entrySet()) {
                update.with(addAll(entry.getKey(), bindMarker())); 
                values.add(toStatementValue(ctx, data.getTablename(), entry.getKey(), entry.getValue()));
            }
            for(Entry<String, ImmutableSet<Object>> entry : data.getSetValuesToRemove().entrySet()) {
                update.with(removeAll(entry.getKey(), bindMarker())); 
                values.add(toStatementValue(ctx, data.getTablename(), entry.getKey(), entry.getValue()));
            }

            for (Entry<String, ImmutableList<Object>> entry : data.getListValuesToPrepend().entrySet()) {
                update.with(prependAll(entry.getKey(), bindMarker())); 
                values.add(toStatementValue(ctx, data.getTablename(), entry.getKey(), entry.getValue()));
            } 
            for (Entry<String, ImmutableList<Object>> entry : data.getListValuesToAppend().entrySet()) {
                update.with(appendAll(entry.getKey(), bindMarker())); 
                values.add(toStatementValue(ctx, data.getTablename(), entry.getKey(), entry.getValue()));
            } 
            for (Entry<String, ImmutableList<Object>> entry : data.getListValuesToRemove().entrySet()) {
                update.with(discardAll(entry.getKey(), bindMarker())); 
                values.add(toStatementValue(ctx, data.getTablename(), entry.getKey(), entry.getValue()));
            } 

            for(Entry<String, ImmutableMap<Object, Optional<Object>>> entry : data.getMapValuesToMutate().entrySet()) {
                update.with(putAll(entry.getKey(), bindMarker())); 
                values.add(toStatementValue(ctx, data.getTablename(), entry.getKey(), entry.getValue()));
            }
            
            
            for(Entry<String, Object> entry : data.getKeys().entrySet()) {
                update.where(eq(entry.getKey(), bindMarker())); 
                values.add(toStatementValue(ctx, data.getTablename(), entry.getKey(), entry.getValue())); 
            }
            
   
            
            ListenableFuture<PreparedStatement> preparedStatementFuture = ctx.getDbSession().prepareAsync(update);
            return ctx.getDbSession().bindAsync(preparedStatementFuture, values.toArray());
            
        // where condition-based update
        } else {
            for (Entry<String, Optional<Object>> entry : data.getValuesToMutate().entrySet()) {
                update.with(set(entry.getKey(), toStatementValue(ctx, data.getTablename(), entry.getKey(), entry.getValue().orNull())));
            }

            for (Entry<String, ImmutableSet<Object>> entry : data.getSetValuesToAdd().entrySet()) {
                update.with(addAll(entry.getKey(), toStatementValue(ctx, data.getTablename(), entry.getKey(), entry.getValue())));
            }
            for (Entry<String, ImmutableSet<Object>> entry : data.getSetValuesToRemove().entrySet()) {
                update.with(removeAll(entry.getKey(), toStatementValue(ctx, data.getTablename(), entry.getKey(), entry.getValue())));
            }
            
            for (Entry<String, ImmutableList<Object>> entry : data.getListValuesToPrepend().entrySet()) {
                update.with(prependAll(entry.getKey(), toStatementValue(ctx, data.getTablename(), entry.getKey(), entry.getValue())));
            } 
            for (Entry<String, ImmutableList<Object>> entry : data.getListValuesToAppend().entrySet()) {
                update.with(appendAll(entry.getKey(), toStatementValue(ctx, data.getTablename(), entry.getKey(), entry.getValue())));
            } 
            for (Entry<String, ImmutableList<Object>> entry : data.getListValuesToRemove().entrySet()) {
                update.with(discardAll(entry.getKey(), toStatementValue(ctx, data.getTablename(), entry.getKey(), entry.getValue())));
            } 

            for(Entry<String, ImmutableMap<Object, Optional<Object>>> entry : data.getMapValuesToMutate().entrySet()) {
                update.with(putAll(entry.getKey(), toStatementValue(ctx, data.getTablename(), entry.getKey(), entry.getValue())));
            }

            if (ctx.getTtlSec() != null) {
                update.using(QueryBuilder.ttl(ctx.getTtlSec().intValue()));
            }

            for (Clause whereCondition : data.getWhereConditions()) {
                update.where(whereCondition);
            }
                        
            return Futures.<Statement>immediateFuture(update);
        }
    }
    
    
    private static boolean isKeyOnlyStatement(WriteQueryData data) {
        return data.getListValuesToAppend().isEmpty() && 
               data.getListValuesToPrepend().isEmpty() &&
               data.getListValuesToRemove().isEmpty() &&
               data.getMapValuesToMutate().isEmpty() &&
               data.getSetValuesToAdd().isEmpty() &&
               data.getSetValuesToRemove().isEmpty() &&
               data.getValuesToMutate().isEmpty();
    }
    

    private static Object toStatementValue(Context ctx, String tablename, String name, Object value) {
        return ctx.toStatementValue(tablename, name, value);
    }
    
    
    private static ImmutableSet<Object> toStatementValue(Context ctx, String tablename, String name, ImmutableSet<Object> values) {
        return ImmutableSet.copyOf(toStatementValue(ctx, tablename, name, ImmutableList.copyOf(values))); 
    }

    
    private static ImmutableList<Object> toStatementValue(Context ctx, String tablename, String name, ImmutableList<Object> values) {
        
        List<Object> result = Lists.newArrayList();

        for (Object value : values) {
            result.add(toStatementValue(ctx, tablename, name, value));
        }
        
        return ImmutableList.copyOf(result);
    }
  
    
    private static Map<Object, Object> toStatementValue(Context ctx, String tablename, String name, ImmutableMap<Object, Optional<Object>> map) {
        Map<Object, Object> m = Maps.newHashMap();
        for (Entry<Object, Optional<Object>> entry : map.entrySet()) {
            m.put(toStatementValue(ctx, tablename, name, toStatementValue(ctx, tablename, name, entry.getKey())), toStatementValue(ctx, tablename, name, entry.getValue().orNull()));
        }
        return m;
    } 
}
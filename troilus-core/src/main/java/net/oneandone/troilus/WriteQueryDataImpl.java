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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

 
/**
 * write query data implementation
 */
class WriteQueryDataImpl implements WriteQueryData {
    private final Tablename tablename;
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
    WriteQueryDataImpl(final Tablename tablename) {
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

    
    private WriteQueryDataImpl(final Tablename tablemname,
                               final ImmutableMap<String, Object> keys, 
                               final ImmutableList<Clause> whereConditions, 
                               final ImmutableMap<String, Optional<Object>> valuesToMutate, 
                               final ImmutableMap<String, ImmutableSet<Object>> setValuesToAdd,
                               final ImmutableMap<String, ImmutableSet<Object>> setValuesToRemove,
                               final ImmutableMap<String, ImmutableList<Object>> listValuesToAppend, 
                               final ImmutableMap<String, ImmutableList<Object>> listValuesToPrepend,
                               final ImmutableMap<String, ImmutableList<Object>> listValuesToRemove,
                               final ImmutableMap<String, ImmutableMap<Object, Optional<Object>>> mapValuesToMutate,
                               final ImmutableList<Clause> onlyIfConditions,
                               final Boolean ifNotExists) {
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
    public WriteQueryDataImpl keys(final ImmutableMap<String, Object> keys) {
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
    public WriteQueryDataImpl whereConditions(final ImmutableList<Clause> whereConditions) {
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
    public WriteQueryDataImpl valuesToMutate(final ImmutableMap<String, Optional<Object>> valuesToMutate) {
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
    public WriteQueryDataImpl setValuesToAdd(final ImmutableMap<String, ImmutableSet<Object>> setValuesToAdd) {
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
    public WriteQueryDataImpl setValuesToRemove(final ImmutableMap<String, ImmutableSet<Object>> setValuesToRemove) {
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
    public WriteQueryDataImpl listValuesToAppend(final ImmutableMap<String, ImmutableList<Object>> listValuesToAppend) {
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
    public WriteQueryDataImpl listValuesToPrepend(final ImmutableMap<String, ImmutableList<Object>> listValuesToPrepend) {
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
    public WriteQueryDataImpl listValuesToRemove(final ImmutableMap<String, ImmutableList<Object>> listValuesToRemove) {
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
    public WriteQueryDataImpl onlyIfConditions(final ImmutableList<Clause> onlyIfConditions) {
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
    public Tablename getTablename() {
        return tablename;
    }
    
    @Override
    public ImmutableMap<String, Object> getKeys() {
        return keys;
    }

    @Override
    public <T> boolean hasKey(final ColumnName<T> name) {
        return hasKey(name.getName());
    }
    
    @Override
    public boolean hasKey(final String name) {
        return getKeys().containsKey(name);
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public <T> T getKey(final ColumnName<T> name) {
        return (T) getKey(name.getName());
    }

    @Override
    public Object getKey(final String name) {
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
    public <T> boolean hasValueToMutate(final ColumnName<T> name) {
        return hasValueToMutate(name.getName());
    }
    
    @Override
    public boolean hasValueToMutate(final String name) {
        return getValuesToMutate().containsKey(name);
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public <T> T getValueToMutate(final ColumnName<T> name) {
        return (T) getValueToMutate(name.getName());
    }
    
    @Override
    public Object getValueToMutate(final String name) {
        final Optional<Object> optional = getValuesToMutate().get(name);
        if (optional == null) {
            return null;
        } else {
            return optional.orElse(null);
        }
    }
    
    @Override
    public ImmutableMap<String, ImmutableSet<Object>> getSetValuesToAdd() {
        return setValuesToAdd;
    }
    
    @Override
    public <T> boolean hasSetValuesToAdd(final ColumnName<Set<T>> name) {
        return hasSetValuesToAdd(name.getName());
    }
    
    @Override
    public boolean hasSetValuesToAdd(final String name) {
        return setValuesToAdd.containsKey(name);
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public <T> ImmutableSet<T> getSetValuesToAdd(final ColumnName<Set<T>> name) {
        return (ImmutableSet<T>) getSetValuesToAdd(name.getName());
    }
    
    @Override
    public ImmutableSet<Object> getSetValuesToAdd(final String name) {
        final ImmutableSet<Object> values = setValuesToAdd.get(name);
        if (values == null) {
            return ImmutableSet.of();
        } else {
            return values;
        }
    }    
    
    @Override
    public <T> boolean hasSetValuesToAddOrSet(final ColumnName<Set<T>> name) {
        return hasSetValuesToAddOrSet(name.getName());
    }
    
    @Override
    public boolean hasSetValuesToAddOrSet(final String name) {
        return hasSetValuesToAdd(name) || hasValueToMutate(name);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> ImmutableSet<T> getSetValuesToAddOrSet(final ColumnName<Set<T>> name) {
        return (ImmutableSet<T>) getSetValuesToAddOrSet(name.getName());
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public ImmutableSet<Object> getSetValuesToAddOrSet(final String name) {
        final ImmutableSet<Object> valuesToAdd = getSetValuesToAdd(name);
        final ImmutableSet<Object> valuesToMutate = (ImmutableSet<Object>) getValueToMutate(name);
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
    public <T> boolean hasSetValuesToRemove(final ColumnName<Set<T>> name) {
        return hasSetValuesToRemove(name.getName());
    }
    
    @Override
    public boolean hasSetValuesToRemove(String name) {
        return setValuesToRemove.containsKey(name);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> ImmutableSet<T> getSetValuesToRemove(final ColumnName<Set<T>> name) {
        return (ImmutableSet<T>) getSetValuesToRemove(name.getName());
    }
    
    @Override
    public ImmutableSet<Object> getSetValuesToRemove(final String name) {
        final ImmutableSet<Object> values = setValuesToRemove.get(name);
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
    public <T> boolean hasListValuesToAppend(final ColumnName<List<T>> name) {
        return hasListValuesToAppend(name.getName());
    }
    
    @Override
    public boolean hasListValuesToAppend(String name) {
        return listValuesToAppend.containsKey(name);
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public <T> ImmutableList<T> getListValuesToAppend(final ColumnName<List<T>> name) {
        return (ImmutableList<T>) getListValuesToAppend(name.getName());
    }
    
    @Override
    public ImmutableList<Object> getListValuesToAppend(final String name) {
        final ImmutableList<Object> values = listValuesToAppend.get(name);
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
    public <T> boolean hasListValuesToPrepend(final ColumnName<List<T>> name) {
        return hasListValuesToPrepend(name.getName());
    }
    
    @Override
    public boolean hasListValuesToPrepend(final String name) {
        return listValuesToPrepend.containsKey(name);
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public <T> ImmutableList<T> getListValuesToPrepend(final ColumnName<List<T>> name) {
        return (ImmutableList<T>) getListValuesToPrepend(name.getName());
    }
    
    @Override
    public ImmutableList<Object> getListValuesToPrepend(final String name) {
        final ImmutableList<Object> values = listValuesToPrepend.get(name);
        if (values == null) {
            return ImmutableList.of();
        } else {
            return values;
        }
    }
    
    @Override
    public <T> boolean hasListValuesToAddOrSet(final ColumnName<List<T>> name) {
        return hasListValuesToAddOrSet(name.getName());
    }
    
    @Override
    public boolean hasListValuesToAddOrSet(final String name) {
        return hasListValuesToAppend(name) || hasListValuesToPrepend(name) || hasValueToMutate(name);
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public <T> ImmutableList<T> getListValuesToAddOrSet(final ColumnName<List<T>> name) {
        return (ImmutableList<T>) getListValuesToAddOrSet(name.getName());
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public ImmutableList<Object> getListValuesToAddOrSet(final String name) {
        final ImmutableList<Object> valuesToAppend = getListValuesToAppend(name);
        final ImmutableList<Object> valuesToPrepend = getListValuesToPrepend(name);
        final ImmutableList<Object> valuesToMutate = (ImmutableList<Object>) getValueToMutate(name);
        
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
    public <T> boolean hasListValuesToRemove(final ColumnName<List<T>> name) {
        return hasListValuesToRemove(name.getName());
    }
    
    @Override
    public boolean hasListValuesToRemove(final String name) {
        return listValuesToRemove.containsKey(name);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> ImmutableList<T> getListValuesToRemove(final ColumnName<List<T>> name) {
        return (ImmutableList<T>) getListValuesToRemove(name.getName());
    }
    
    @Override
    public ImmutableList<Object> getListValuesToRemove(final String name) {
        final ImmutableList<Object> values = listValuesToRemove.get(name);
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
    public <T, V> boolean hasMapValuesToMutate(final ColumnName<Map<T, V>> name) {
        return hasMapValuesToMutate(name.getName());
    }
    
    @Override
    public boolean hasMapValuesToMutate(final String name) {
        return mapValuesToMutate.containsKey(name);
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public <T, V> ImmutableMap<T, Optional<V>> getMapValuesToMutate(final ColumnName<Map<T, V>> name) {
        final Map<T, Optional<V>> result = Maps.newHashMap();
        for (Entry<Object, Optional<Object>> entry : getMapValuesToMutate(name.getName()).entrySet()) {
            result.put((T) entry.getKey(), (Optional<V>) entry.getValue()); 
        }
        return ImmutableMap.copyOf(result);
    }
    
    @Override
    public ImmutableMap<Object, Optional<Object>> getMapValuesToMutate(final String name) {
        final ImmutableMap<Object, Optional<Object>> values = mapValuesToMutate.get(name);
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
    static CompletableFuture<Statement> toStatementAsync(WriteQueryData data, 
                                                         final ExecutionSpec executionSpec, 
                                                         final UDTValueMapper udtValueMapper, 
                                                         final DBSession dbSession) {
        
        if (isKeyOnlyStatement(data)) {
            final Map<String, Optional<Object>> valuesToMUtate = Maps.newHashMap();
            for (Entry<String, Object> entry : data.getKeys().entrySet()) {
                valuesToMUtate.put(entry.getKey(), Optional.of(entry.getValue()));
            }
            
            data = data.valuesToMutate(ImmutableMap.copyOf(valuesToMUtate)).keys(ImmutableMap.<String, Object>of());
        }
        
        
        if ((data.getIfNotExits() != null) || (data.getKeys().isEmpty() && data.getWhereConditions().isEmpty())) {
            return toInsertStatementAsync(data, executionSpec, udtValueMapper, dbSession);
        } else {
            return toUpdateStatementAsync(data, executionSpec, udtValueMapper, dbSession);
        }
    }
    
    
    private static CompletableFuture<Statement> toInsertStatementAsync(final WriteQueryData data, 
                                                                       final ExecutionSpec executionSpec, 
                                                                       final UDTValueMapper udtValueMapper, 
                                                                       final DBSession dbSession) {
        final Insert insert = (data.getTablename().getKeyspacename() == null) ? insertInto(data.getTablename().getTablename()) 
                                                                              : insertInto(data.getTablename().getKeyspacename(), data.getTablename().getTablename());
        
        final List<Object> values = Lists.newArrayList();
        
        for(Entry<String, Optional<Object>> entry : data.getValuesToMutate().entrySet()) {
            insert.value(entry.getKey(), bindMarker());  
            values.add(udtValueMapper.toStatementValue(data.getTablename(), entry.getKey(), entry.getValue().orElse(null))); 
        }
        
        if (data.getIfNotExits() != null) {
            insert.ifNotExists();
            if (executionSpec.getSerialConsistencyLevel() != null) {
                insert.setSerialConsistencyLevel(executionSpec.getSerialConsistencyLevel());
            }
        }

        if (executionSpec.getTtl() != null) {
            insert.using(ttl(bindMarker()));  
            values.add((int) executionSpec.getTtl().getSeconds());
        }

        
        return dbSession.prepareAsync(insert)
                        .thenApply(statement ->   statement.bind(values.toArray()));
    }
    
    
    
    
    private static CompletableFuture<Statement> toUpdateStatementAsync(final WriteQueryData data, 
                                                                       final ExecutionSpec executionSpec, 
                                                                       final UDTValueMapper udtValueMapper, 
                                                                       final DBSession dbSession) {
        
        final com.datastax.driver.core.querybuilder.Update update = (data.getTablename().getKeyspacename() == null) ? update(data.getTablename().getTablename()) 
                                                                                                                    : update(data.getTablename().getKeyspacename(), data.getTablename().getTablename());
        
        for (Clause onlyIfCondition : data.getOnlyIfConditions()) {
            update.onlyIf(onlyIfCondition);
        }

        
        // key-based update
        if (data.getWhereConditions().isEmpty()) {
            final List<Object> values = Lists.newArrayList();
            
            if (executionSpec.getTtl() != null) {
                update.using(QueryBuilder.ttl(bindMarker())); 
                values.add((int) executionSpec.getTtl().getSeconds()); 
            }
            
            for (Entry<String, Optional<Object>> entry : data.getValuesToMutate().entrySet()) {
                final boolean isPrimaryKey = udtValueMapper.getMetadataCatalog().isPrimaryKey(data.getTablename(), entry.getKey());
            	if (!isPrimaryKey) {
            		update.with(set(entry.getKey(), bindMarker())); 
                    values.add(toStatementValue(udtValueMapper, data.getTablename(), entry.getKey(), entry.getValue().orElse(null)));
            	}
            }

            for (Entry<String, ImmutableSet<Object>> entry : data.getSetValuesToAdd().entrySet()) {
                update.with(addAll(entry.getKey(), bindMarker())); 
                values.add(toStatementValue(udtValueMapper, data.getTablename(), entry.getKey(), entry.getValue()));
            }
            for(Entry<String, ImmutableSet<Object>> entry : data.getSetValuesToRemove().entrySet()) {
                update.with(removeAll(entry.getKey(), bindMarker())); 
                values.add(toStatementValue(udtValueMapper, data.getTablename(), entry.getKey(), entry.getValue()));
            }

            for (Entry<String, ImmutableList<Object>> entry : data.getListValuesToPrepend().entrySet()) {
                update.with(prependAll(entry.getKey(), bindMarker())); 
                values.add(toStatementValue(udtValueMapper, data.getTablename(), entry.getKey(), entry.getValue()));
            } 
            for (Entry<String, ImmutableList<Object>> entry : data.getListValuesToAppend().entrySet()) {
                update.with(appendAll(entry.getKey(), bindMarker())); 
                values.add(toStatementValue(udtValueMapper, data.getTablename(), entry.getKey(), entry.getValue()));
            } 
            for (Entry<String, ImmutableList<Object>> entry : data.getListValuesToRemove().entrySet()) {
                update.with(discardAll(entry.getKey(), bindMarker())); 
                values.add(toStatementValue(udtValueMapper, data.getTablename(), entry.getKey(), entry.getValue()));
            } 

            for(Entry<String, ImmutableMap<Object, Optional<Object>>> entry : data.getMapValuesToMutate().entrySet()) {
                update.with(putAll(entry.getKey(), bindMarker())); 
                
                /**
                 * Mike Wislocki - change 1/26/16
                 * this change iterates through the map mutations and rebuids the map of objects 
                 * in order to effectively create UDTValue.  Prior code was passing in the map key as 
                 * opposed to the actual Map of objects to be converted into a UDT in the 
                 * UDTValueMapper.toUdtValue method.  Otherwise a ClassCastException will be thrown at line 378
                 */
                Map<Object, Object> map = new HashMap<Object, Object>();
                for(Entry<Object, Optional<Object>> thisEntry : entry.getValue().entrySet()) {
                	Object object = thisEntry.getValue().isPresent() ? thisEntry.getValue().get() : null;
                	if(object !=null) {
                		map.put(thisEntry.getKey(), object);
                	}
                }
                values.add(udtValueMapper.toStatementValue(data.getTablename(), entry.getKey(), map));
            }
            
            
            for(Entry<String, Object> entry : data.getKeys().entrySet()) {
                update.where(eq(entry.getKey(), bindMarker())); 
                values.add(toStatementValue(udtValueMapper, data.getTablename(), entry.getKey(), entry.getValue())); 
            }
            
   
            
            return dbSession.prepareAsync(update)
                            .thenApply(preparedStatement -> preparedStatement.bind(values.toArray()));
            
        // where condition-based update
        } else {
            for (Entry<String, Optional<Object>> entry : data.getValuesToMutate().entrySet()) {
                update.with(set(entry.getKey(), toStatementValue(udtValueMapper, data.getTablename(), entry.getKey(), entry.getValue().orElse(null))));
            }

            for (Entry<String, ImmutableSet<Object>> entry : data.getSetValuesToAdd().entrySet()) {
                update.with(addAll(entry.getKey(), toStatementValue(udtValueMapper, data.getTablename(), entry.getKey(), entry.getValue())));
            }
            for (Entry<String, ImmutableSet<Object>> entry : data.getSetValuesToRemove().entrySet()) {
                update.with(removeAll(entry.getKey(), toStatementValue(udtValueMapper, data.getTablename(), entry.getKey(), entry.getValue())));
            }
            
            for (Entry<String, ImmutableList<Object>> entry : data.getListValuesToPrepend().entrySet()) {
                update.with(prependAll(entry.getKey(), toStatementValue(udtValueMapper, data.getTablename(), entry.getKey(), entry.getValue())));
            } 
            for (Entry<String, ImmutableList<Object>> entry : data.getListValuesToAppend().entrySet()) {
                update.with(appendAll(entry.getKey(), toStatementValue(udtValueMapper, data.getTablename(), entry.getKey(), entry.getValue())));
            } 
            for (Entry<String, ImmutableList<Object>> entry : data.getListValuesToRemove().entrySet()) {
                update.with(discardAll(entry.getKey(), toStatementValue(udtValueMapper, data.getTablename(), entry.getKey(), entry.getValue())));
            } 

            for(Entry<String, ImmutableMap<Object, Optional<Object>>> entry : data.getMapValuesToMutate().entrySet()) {
                update.with(putAll(entry.getKey(), toStatementValue(udtValueMapper, data.getTablename(), entry.getKey(), entry.getValue())));
            }

            if (executionSpec.getTtl() != null) {
                update.using(QueryBuilder.ttl((int) executionSpec.getTtl().getSeconds()));
            }

            for (Clause whereCondition : data.getWhereConditions()) {
                update.where(whereCondition);
            }
                        
            return CompletableFuture.completedFuture(update);
        }
    }
    
    
    private static boolean isKeyOnlyStatement(final WriteQueryData data) {
        return data.getListValuesToAppend().isEmpty() && 
               data.getListValuesToPrepend().isEmpty() &&
               data.getListValuesToRemove().isEmpty() &&
               data.getMapValuesToMutate().isEmpty() &&
               data.getSetValuesToAdd().isEmpty() &&
               data.getSetValuesToRemove().isEmpty() &&
               data.getValuesToMutate().isEmpty();
    }
    

    private static Object toStatementValue(final UDTValueMapper udtValueMapper,
                                           final Tablename tablename, 
                                           final String name,
                                           final Object value) {
        return udtValueMapper.toStatementValue(tablename, name, value);
    }
    
    
    private static ImmutableSet<Object> toStatementValue(final UDTValueMapper udtValueMapper, 
                                                         final Tablename tablename, 
                                                         final String name, 
                                                         final ImmutableSet<Object> values) {
        return ImmutableSet.copyOf(toStatementValue(udtValueMapper, tablename, name, ImmutableList.copyOf(values))); 
    }

    
    private static ImmutableList<Object> toStatementValue(final UDTValueMapper udtValueMapper, 
                                                          final Tablename tablename, 
                                                          final String name,
                                                          final ImmutableList<Object> values) {
        final List<Object> result = Lists.newArrayList();
        for (Object value : values) {
            result.add(toStatementValue(udtValueMapper, tablename, name, value));
        }
        
        return ImmutableList.copyOf(result);
    }
  
    
    private static Map<Object, Object> toStatementValue(final UDTValueMapper udtValueMapper, 
                                                        final Tablename tablename, 
                                                        final String name, 
                                                        final ImmutableMap<Object, Optional<Object>> map) {
        final Map<Object, Object> m = Maps.newHashMap();
        for (Entry<Object, Optional<Object>> entry : map.entrySet()) {
            m.put(toStatementValue(udtValueMapper, tablename, name, toStatementValue(udtValueMapper, tablename, name, entry.getKey())), toStatementValue(udtValueMapper, tablename, name, entry.getValue().orElse(null)));
        }
        return m;
    } 
}
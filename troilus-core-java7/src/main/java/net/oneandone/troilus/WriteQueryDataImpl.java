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


 
/**
 * write query data implementation
 *
 */
class WriteQueryDataImpl implements WriteQueryData {

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
    WriteQueryDataImpl() {
        this(ImmutableMap.<String, Object>of(),
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

    
    private WriteQueryDataImpl(ImmutableMap<String, Object> keys, 
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
        return new WriteQueryDataImpl(keys, 
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
        return new WriteQueryDataImpl(this.keys, 
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
        return new WriteQueryDataImpl(this.keys, 
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
        return new WriteQueryDataImpl(this.keys, 
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
        return new WriteQueryDataImpl(this.keys, 
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
        return new WriteQueryDataImpl(this.keys, 
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
        return new WriteQueryDataImpl(this.keys, 
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
         return new WriteQueryDataImpl(this.keys, 
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
        return new WriteQueryDataImpl(this.keys, 
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
        return new WriteQueryDataImpl(this.keys, 
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
        return new WriteQueryDataImpl(this.keys, 
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
    public ImmutableMap<String, Object> getKeys() {
        return keys;
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
    public ImmutableMap<String, ImmutableSet<Object>> getSetValuesToAdd() {
        return setValuesToAdd;
    }

    @Override
    public ImmutableMap<String, ImmutableSet<Object>> getSetValuesToRemove() {
        return setValuesToRemove;
    }

    @Override
    public ImmutableMap<String, ImmutableList<Object>> getListValuesToAppend() {
        return listValuesToAppend;
    }

    @Override
    public ImmutableMap<String, ImmutableList<Object>> getListValuesToPrepend() {
        return listValuesToPrepend;
    }

    @Override
    public ImmutableMap<String, ImmutableList<Object>> getListValuesToRemove() {
        return listValuesToRemove;
    }

    @Override
    public ImmutableMap<String, ImmutableMap<Object, Optional<Object>>> getMapValuesToMutate() {
        return mapValuesToMutate;
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
    static Statement toStatement(WriteQueryData data, Context ctx) {
        if ((data.getIfNotExits() != null) || (data.getKeys().isEmpty() && data.getWhereConditions().isEmpty())) {
            return toInsertStatement(data, ctx);
        } else {
            return toUpdateStatement(data, ctx);
        }
    }
    
    
    private static Statement toInsertStatement(WriteQueryData data,Context ctx) {
        Insert insert = insertInto(ctx.getTable());
        
        List<Object> values = Lists.newArrayList();
        
        for(Entry<String, Optional<Object>> entry : data.getValuesToMutate().entrySet()) {
            insert.value(entry.getKey(), bindMarker());  
            values.add(ctx.toStatementValue(entry.getKey(), entry.getValue().orNull())); 
        }
        
        if (data.getIfNotExits() != null) {
            insert.ifNotExists();
            if (ctx.getSerialConsistencyLevel() != null) {
                insert.setSerialConsistencyLevel(ctx.getSerialConsistencyLevel());
            }
        }

        if (ctx.getTtlSec() != null) {
            insert.using(ttl(bindMarker()));  
            values.add(ctx.getTtlSec().intValue());
        }

        PreparedStatement stmt = ctx.prepare(insert);
        return stmt.bind(values.toArray());
    }
    
    
    
    
    private static Statement toUpdateStatement(WriteQueryData data,Context ctx) {
        com.datastax.driver.core.querybuilder.Update update = update(ctx.getTable());
        
        
        for (Clause onlyIfCondition : data.getOnlyIfConditions()) {
            update.onlyIf(onlyIfCondition);
        }

        
        // key-based update
        if (data.getWhereConditions().isEmpty()) {
            List<Object> values = Lists.newArrayList();
            
            for (Entry<String, Optional<Object>> entry : data.getValuesToMutate().entrySet()) {
                update.with(set(entry.getKey(), bindMarker())); 
                values.add(toStatementValue(ctx, entry.getKey(), entry.getValue().orNull()));
            }

            for (Entry<String, ImmutableSet<Object>> entry : data.getSetValuesToAdd().entrySet()) {
                update.with(addAll(entry.getKey(), bindMarker())); 
                values.add(toStatementValue(ctx, entry.getKey(), entry.getValue()));
            }
            for(Entry<String, ImmutableSet<Object>> entry : data.getSetValuesToRemove().entrySet()) {
                update.with(removeAll(entry.getKey(), bindMarker())); 
                values.add(toStatementValue(ctx, entry.getKey(), entry.getValue()));
            }

            for (Entry<String, ImmutableList<Object>> entry : data.getListValuesToPrepend().entrySet()) {
                update.with(prependAll(entry.getKey(), bindMarker())); 
                values.add(toStatementValue(ctx, entry.getKey(), entry.getValue()));
            } 
            for (Entry<String, ImmutableList<Object>> entry : data.getListValuesToAppend().entrySet()) {
                update.with(appendAll(entry.getKey(), bindMarker())); 
                values.add(toStatementValue(ctx, entry.getKey(), entry.getValue()));
            } 
            for (Entry<String, ImmutableList<Object>> entry : data.getListValuesToRemove().entrySet()) {
                update.with(discardAll(entry.getKey(), bindMarker())); 
                values.add(toStatementValue(ctx, entry.getKey(), entry.getValue()));
            } 

            for(Entry<String, ImmutableMap<Object, Optional<Object>>> entry : data.getMapValuesToMutate().entrySet()) {
                update.with(putAll(entry.getKey(), bindMarker())); 
                values.add(toStatementValue(ctx, entry.getKey(), entry.getValue()));
            }
            
            
            for(Entry<String, Object> entry : data.getKeys().entrySet()) {
                update.where(eq(entry.getKey(), bindMarker())); 
                values.add(toStatementValue(ctx, entry.getKey(), entry.getValue())); 
            }
            
            if (ctx.getTtlSec() != null) {
                update.using(QueryBuilder.ttl(bindMarker())); 
                values.add(ctx.getTtlSec().intValue()); 
            }
            
            return ctx.prepare(update).bind(values.toArray());

            
        // where condition-based update
        } else {
            for (Entry<String, Optional<Object>> entry : data.getValuesToMutate().entrySet()) {
                update.with(set(entry.getKey(), toStatementValue(ctx, entry.getKey(), entry.getValue().orNull())));
            }

            for (Entry<String, ImmutableSet<Object>> entry : data.getSetValuesToAdd().entrySet()) {
                update.with(addAll(entry.getKey(), toStatementValue(ctx, entry.getKey(), entry.getValue())));
            }
            for (Entry<String, ImmutableSet<Object>> entry : data.getSetValuesToRemove().entrySet()) {
                update.with(removeAll(entry.getKey(), toStatementValue(ctx, entry.getKey(), entry.getValue())));
            }
            
            for (Entry<String, ImmutableList<Object>> entry : data.getListValuesToPrepend().entrySet()) {
                update.with(prependAll(entry.getKey(), toStatementValue(ctx, entry.getKey(), entry.getValue())));
            } 
            for (Entry<String, ImmutableList<Object>> entry : data.getListValuesToAppend().entrySet()) {
                update.with(appendAll(entry.getKey(), toStatementValue(ctx, entry.getKey(), entry.getValue())));
            } 
            for (Entry<String, ImmutableList<Object>> entry : data.getListValuesToRemove().entrySet()) {
                update.with(discardAll(entry.getKey(), toStatementValue(ctx, entry.getKey(), entry.getValue())));
            } 

            for(Entry<String, ImmutableMap<Object, Optional<Object>>> entry : data.getMapValuesToMutate().entrySet()) {
                update.with(putAll(entry.getKey(), toStatementValue(ctx, entry.getKey(), entry.getValue())));
            }

            if (ctx.getTtlSec() != null) {
                update.using(QueryBuilder.ttl(ctx.getTtlSec().intValue()));
            }

            for (Clause whereCondition : data.getWhereConditions()) {
                update.where(whereCondition);
            }
                        
            return update;
        }
    }
    

    private static Object toStatementValue(Context ctx, String name, Object value) {
        return ctx.toStatementValue(name, value);
    }
    
    
    private static ImmutableSet<Object> toStatementValue(Context ctx, String name, ImmutableSet<Object> values) {
        return ImmutableSet.copyOf(toStatementValue(ctx, name, ImmutableList.copyOf(values))); 
    }

    
    private static ImmutableList<Object> toStatementValue(Context ctx, String name, ImmutableList<Object> values) {
        
        List<Object> result = Lists.newArrayList();

        for (Object value : values) {
            result.add(toStatementValue(ctx, name, value));
        }
        
        return ImmutableList.copyOf(result);
    }
  
    
    private static Map<Object, Object> toStatementValue(Context ctx, String name, ImmutableMap<Object, Optional<Object>> map) {
        Map<Object, Object> m = Maps.newHashMap();
        for (Entry<Object, Optional<Object>> entry : map.entrySet()) {
            m.put(toStatementValue(ctx, name, toStatementValue(ctx, name, entry.getKey())), toStatementValue(ctx, name, entry.getValue().orNull()));
        }
        return m;
    }
}
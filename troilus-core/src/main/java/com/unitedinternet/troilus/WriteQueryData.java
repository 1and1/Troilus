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
import java.util.Optional;
import java.util.Map.Entry;
import java.util.function.Supplier;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;


 
public class WriteQueryData {

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
    private final Optional<Boolean> ifNotExists;
    
    

    WriteQueryData() {
        this(ImmutableMap.of(),
             ImmutableList.of(),
             ImmutableMap.of(),
             ImmutableMap.of(),
             ImmutableMap.of(),
             ImmutableMap.of(),
             ImmutableMap.of(),
             ImmutableMap.of(),
             ImmutableMap.of(),
             ImmutableList.of(),
             Optional.empty());
    }

    
    private WriteQueryData(ImmutableMap<String, Object> keys, 
                            ImmutableList<Clause> whereConditions, 
                            ImmutableMap<String, Optional<Object>> valuesToMutate, 
                            ImmutableMap<String, ImmutableSet<Object>> setValuesToAdd,
                            ImmutableMap<String, ImmutableSet<Object>> setValuesToRemove,
                            ImmutableMap<String, ImmutableList<Object>> listValuesToAppend, 
                            ImmutableMap<String, ImmutableList<Object>> listValuesToPrepend,
                            ImmutableMap<String, ImmutableList<Object>> listValuesToRemove,
                            ImmutableMap<String, ImmutableMap<Object, Optional<Object>>> mapValuesToMutate,
                            ImmutableList<Clause> onlyIfConditions,
                            Optional<Boolean> ifNotExists) {
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
    
    

    public WriteQueryData keys(ImmutableMap<String, Object> keys) {
        return new WriteQueryData(keys, 
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
    
  
    public WriteQueryData whereConditions(ImmutableList<Clause> whereConditions) {
        preCondition(!ifNotExists.isPresent(), IllegalStateException::new);
        
        return new WriteQueryData(this.keys, 
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
    
    public WriteQueryData valuesToMutate(ImmutableMap<String, Optional<Object>> valuesToMutate) {
        return new WriteQueryData(this.keys, 
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
 
    
    public WriteQueryData setValuesToAdd(ImmutableMap<String, ImmutableSet<Object>> setValuesToAdd) {
        preCondition(!ifNotExists.isPresent(), IllegalStateException::new);
        
        return new WriteQueryData(this.keys, 
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
    
    
    public WriteQueryData setValuesToRemove(ImmutableMap<String, ImmutableSet<Object>> setValuesToRemove) {
        preCondition(!ifNotExists.isPresent(), IllegalStateException::new);
        
        return new WriteQueryData(this.keys, 
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
 
    
    public WriteQueryData listValuesToAppend(ImmutableMap<String, ImmutableList<Object>> listValuesToAppend) {
        preCondition(!ifNotExists.isPresent(), IllegalStateException::new);

        return new WriteQueryData(this.keys, 
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
   
    
    public WriteQueryData listValuesToPrepend(ImmutableMap<String, ImmutableList<Object>> listValuesToPrepend) {
        preCondition(!ifNotExists.isPresent(), IllegalStateException::new);

        return new WriteQueryData(this.keys, 
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
 
    
    public WriteQueryData listValuesToRemove(ImmutableMap<String, ImmutableList<Object>> listValuesToRemove) {
        preCondition(!ifNotExists.isPresent(), IllegalStateException::new);

        return new WriteQueryData(this.keys, 
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
 

    public WriteQueryData mapValuesToMutate(ImmutableMap<String, ImmutableMap<Object, Optional<Object>>> mapValuesToMutate) {
        preCondition(!ifNotExists.isPresent(), IllegalStateException::new);

        return new WriteQueryData(this.keys, 
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

    
    public WriteQueryData onlyIfConditions(ImmutableList<Clause> onlyIfConditions) {
        preCondition(!ifNotExists.isPresent(), IllegalStateException::new);

        return new WriteQueryData(this.keys, 
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

    public WriteQueryData ifNotExists(Optional<Boolean> ifNotExists) {
        preCondition(onlyIfConditions.isEmpty(), IllegalStateException::new);
        preCondition(whereConditions.isEmpty(), IllegalStateException::new);
        preCondition(setValuesToAdd.isEmpty(), IllegalStateException::new);
        preCondition(setValuesToRemove.isEmpty(), IllegalStateException::new);
        preCondition(listValuesToAppend.isEmpty(), IllegalStateException::new);
        preCondition(listValuesToPrepend.isEmpty(), IllegalStateException::new);
        preCondition(listValuesToRemove.isEmpty(), IllegalStateException::new);
        preCondition(mapValuesToMutate.isEmpty(), IllegalStateException::new);
        
        return new WriteQueryData(this.keys, 
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
    

    
    private <T extends RuntimeException> void preCondition(boolean condition, Supplier<T> suppier) {
        if (!condition) {
            throw suppier.get();
        }
    }

    
    public ImmutableMap<String, Object> getKeyNameValuePairs() {
        return keys;
    }

    public ImmutableList<Clause> getWhereConditions() {
        return whereConditions;
    }

    public ImmutableMap<String, Optional<Object>> getValuesToMutate() {
        return valuesToMutate;
    }

    public ImmutableMap<String, ImmutableSet<Object>> getSetValuesToAdd() {
        return setValuesToAdd;
    }

    public ImmutableMap<String, ImmutableSet<Object>> getSetValuesToRemove() {
        return setValuesToRemove;
    }

    public ImmutableMap<String, ImmutableList<Object>> getListValuesToAppend() {
        return listValuesToAppend;
    }

    public ImmutableMap<String, ImmutableList<Object>> getListValuesToPrepend() {
        return listValuesToPrepend;
    }

    public ImmutableMap<String, ImmutableList<Object>> getListValuesToRemove() {
        return listValuesToRemove;
    }

    public ImmutableMap<String, ImmutableMap<Object, Optional<Object>>> getMapValuesToMutate() {
        return mapValuesToMutate;
    }

    public ImmutableList<Clause> getOnlyIfConditions() {
        return onlyIfConditions;
    }
    
    public Optional<Boolean> getIfNotExits() {
        return ifNotExists;
    }
    
    
    
    Statement toStatement(Context ctx) {
        if (getIfNotExits().isPresent() || 
            (getKeyNameValuePairs().isEmpty() && getWhereConditions().isEmpty())) {
            return toInsertStatement(ctx);
        } else {
            return toUpdateStatement(ctx);
        }
    }
    
    
    private Statement toInsertStatement(Context ctx) {
        Insert insert = insertInto(ctx.getTable());
        
        List<Object> values = Lists.newArrayList();
        getValuesToMutate().forEach((name, optionalValue) -> { insert.value(name, bindMarker());  values.add(ctx.toStatementValue(name, optionalValue.orElse(null))); } ); 
        
        
        getIfNotExits().ifPresent(ifNotExits -> {
                                                            insert.ifNotExists();
                                                            if (ctx.getSerialConsistencyLevel() != null) {
                                                                insert.setSerialConsistencyLevel(ctx.getSerialConsistencyLevel());
                                                            }
                                                          });

        if (ctx.getTtlSec() != null) {
            insert.using(ttl(bindMarker()));  
            values.add(ctx.getTtlSec().intValue());
        }

        PreparedStatement stmt = ctx.prepare(insert);
        return stmt.bind(values.toArray());
    }
    
    
    
    private Statement toUpdateStatement(Context ctx) {
        com.datastax.driver.core.querybuilder.Update update = update(ctx.getTable());
        
        getOnlyIfConditions().forEach(condition -> update.onlyIf(condition));

        
        // key-based update
        if (getWhereConditions().isEmpty()) {
            List<Object> values = Lists.newArrayList();
            
            getValuesToMutate().forEach((name, optionalValue) -> { update.with(set(name, bindMarker())); values.add(toStatementValue(ctx, name, optionalValue.orElse(null))); });

            getSetValuesToAdd().forEach((name, vals) -> { update.with(addAll(name, bindMarker())); values.add(toStatementValue(ctx, name, vals)); });
            getSetValuesToRemove().forEach((name, vals) -> { update.with(removeAll(name, bindMarker())); values.add(toStatementValue(ctx, name, vals)); });
            
            getListValuesToPrepend().forEach((name, vals) -> { update.with(prependAll(name, bindMarker())); values.add(toStatementValue(ctx, name, vals)); });
            getListValuesToAppend().forEach((name, vals) -> { update.with(appendAll(name, bindMarker())); values.add(toStatementValue(ctx, name, vals)); });
            getListValuesToRemove().forEach((name, vals) -> { update.with(discardAll(name, bindMarker())); values.add(toStatementValue(ctx, name, vals)); });

            getMapValuesToMutate().forEach((name, map) -> { update.with(putAll(name, bindMarker())); values.add(toStatementValue(ctx, name, map)); });
            
            
            getKeyNameValuePairs().keySet().forEach(keyname -> { update.where(eq(keyname, bindMarker())); values.add(toStatementValue(ctx, keyname, getKeyNameValuePairs().get(keyname))); } );
            
            getOnlyIfConditions().forEach(condition -> update.onlyIf(condition));
            
            if (ctx.getTtlSec() != null) {
                update.using(QueryBuilder.ttl(bindMarker())); 
                values.add(ctx.getTtlSec().intValue()); 
            }
            
            return ctx.prepare(update).bind(values.toArray());

            
        // where condition-based update
        } else {
            getValuesToMutate().forEach((name, optionalValue) -> update.with(set(name, toStatementValue(ctx, name, optionalValue.orElse(null)))));
        
            getSetValuesToAdd().forEach((name, vals) -> update.with(addAll(name, toStatementValue(ctx, name, vals))));
            getSetValuesToRemove().forEach((name, vals) -> update.with(removeAll(name, toStatementValue(ctx, name, vals))));

            getListValuesToPrepend().forEach((name, vals) -> update.with(prependAll(name, toStatementValue(ctx, name, vals))));
            getListValuesToAppend().forEach((name, vals) -> update.with(appendAll(name, toStatementValue(ctx, name, vals))));
            getListValuesToRemove().forEach((name, vals) -> update.with(discardAll(name, toStatementValue(ctx, name, vals))));
            
            getMapValuesToMutate().forEach((name, map) -> update.with(putAll(name, toStatementValue(ctx, name, map))));

            if (ctx.getTtlSec() != null) {
                update.using(QueryBuilder.ttl(ctx.getTtlSec().intValue()));
            }

            getWhereConditions().forEach(whereClause -> update.where(whereClause));
            
            return update;
        }
    }
    

    private Object toStatementValue(Context ctx, String name, Object value) {
        return ctx.toStatementValue(name, value);
    }
    
    
    private ImmutableSet<Object> toStatementValue(Context ctx, String name, ImmutableSet<Object> values) {
        return values.stream().map(value -> toStatementValue(ctx, name, value)).collect(Immutables.toSet());
    }

    
    private ImmutableList<Object> toStatementValue(Context ctx, String name, ImmutableList<Object> values) {
        return values.stream().map(value -> toStatementValue(ctx, name, value)).collect(Immutables.toList());
    }
  
    
    private Map<Object, Object> toStatementValue(Context ctx, String name, ImmutableMap<Object, Optional<Object>> map) {
        Map<Object, Object> m = Maps.newHashMap();
        for (Entry<Object, Optional<Object>> entry : map.entrySet()) {
            m.put(toStatementValue(ctx, name, toStatementValue(ctx, name, entry.getKey())), toStatementValue(ctx, name, entry.getValue().orElse(null)));
        }
        return m;
    }
}
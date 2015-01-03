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



import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.unitedinternet.troilus.Dao.Insertion;
import com.unitedinternet.troilus.Dao.Update;
import com.unitedinternet.troilus.Dao.Write;


 
class UpdateQuery extends MutationQuery<Write> implements Write {

    private final ImmutableMap<String, Object> keys;
    
    private final ImmutableMap<String, Optional<Object>> valuesToMutate;
    private final ImmutableMap<String, ImmutableSet<Object>> setValuesToAdd;
    private final ImmutableMap<String, ImmutableSet<Object>> setValuesToRemove;
    private final ImmutableMap<String, ImmutableList<Object>> listValuesToAppend;
    private final ImmutableMap<String, ImmutableList<Object>> listValuesToPrepend;
    private final ImmutableMap<String, ImmutableList<Object>> listValuesToRemove;
    private final ImmutableMap<String, ImmutableMap<Object, Optional<Object>>> mapValuesToMutate;
    
    private final ImmutableList<Clause> ifConditions;
    private final ImmutableList<Clause> whereConditions;


    
    
    public UpdateQuery(Context ctx, 
                       QueryFactory queryFactory,
                       ImmutableMap<String, Object> keys, 
                       ImmutableList<Clause> whereConditions, 
                       ImmutableMap<String, Optional<Object>> valuesToMutate, 
                       ImmutableMap<String, ImmutableSet<Object>> setValuesToAdd,
                       ImmutableMap<String, ImmutableSet<Object>> setValuesToRemove,
                       ImmutableMap<String, ImmutableList<Object>> listValuesToAppend, 
                       ImmutableMap<String, ImmutableList<Object>> listValuesToPrepend,
                       ImmutableMap<String, ImmutableList<Object>> listValuesToRemove,
                       ImmutableMap<String, ImmutableMap<Object, Optional<Object>>> mapValuesToMutate,
                       ImmutableList<Clause> ifConditions) {
        super(ctx, queryFactory);
        this.keys = keys;
        this.whereConditions = whereConditions;
        this.valuesToMutate = valuesToMutate;
        this.setValuesToAdd = setValuesToAdd;
        this.setValuesToRemove = setValuesToRemove;
        this.listValuesToAppend = listValuesToAppend;
        this.listValuesToPrepend = listValuesToPrepend;
        this.listValuesToRemove = listValuesToRemove;
        this.mapValuesToMutate = mapValuesToMutate;
        this.ifConditions = ifConditions;
    }
    
    @Override
    protected UpdateQuery newQuery(Context newContext) {
        return newUpdateQuery(newContext, 
                              keys, 
                              whereConditions,  
                              valuesToMutate,
                              setValuesToAdd,
                              setValuesToRemove,
                              listValuesToAppend,
                              listValuesToPrepend,
                              listValuesToRemove, 
                              mapValuesToMutate,
                              ifConditions);
    }
    
    
    public InsertionQuery entity(Object entity) {
        return newInsertionQuery(toValues(entity), false);
    }
    
        
    
    @Override
    public UpdateQuery value(String name, Object value) {
        return newUpdateQuery(keys, 
                              whereConditions,
                              Immutables.merge(valuesToMutate, name, toOptional(value)), 
                              setValuesToAdd,
                              setValuesToRemove,
                              listValuesToAppend,
                              listValuesToPrepend,
                              listValuesToRemove,
                              mapValuesToMutate,
                              ifConditions);
    }

    
    @Override
    public UpdateQuery values(ImmutableMap<String, Object> nameValuePairsToAdd) {
        return newUpdateQuery(keys,  
                              whereConditions, 
                              Immutables.merge(valuesToMutate, Immutables.transform(nameValuePairsToAdd, name -> name, value -> toOptional(value))), 
                              setValuesToAdd,
                              setValuesToRemove,
                              listValuesToAppend,
                              listValuesToPrepend,
                              listValuesToRemove,
                              mapValuesToMutate,
                              ifConditions);
    }
    
    
    
    @Override
    public UpdateQuery removeSetValue(String name, Object value) {
        ImmutableSet<Object> values = setValuesToRemove.get(name);
        values = (values == null) ? ImmutableSet.of(value) : Immutables.merge(values, value);
        
        return newUpdateQuery(keys, 
                              whereConditions, 
                              valuesToMutate, 
                              setValuesToAdd,
                              Immutables.merge(setValuesToRemove, name, values),
                              listValuesToAppend,
                              listValuesToPrepend,
                              listValuesToRemove,
                              mapValuesToMutate,
                              ifConditions);
    }

    
    @Override
    public UpdateQuery addSetValue(String name, Object value) {
        ImmutableSet<Object> values = setValuesToAdd.get(name);
        values = (values == null) ? ImmutableSet.of(value): Immutables.merge(values, value);
        
        return newUpdateQuery(keys, 
                              whereConditions, 
                              valuesToMutate, 
                              Immutables.merge(setValuesToAdd, name, values),
                              setValuesToRemove,
                              listValuesToAppend,
                              listValuesToPrepend,
                              listValuesToRemove,
                              mapValuesToMutate,
                              ifConditions);
    }
   
    
    @Override
    public Write prependListValue(String name, Object value) {
        ImmutableList<Object> values = listValuesToPrepend.get(name);
        values = (values == null) ? ImmutableList.of(value) : Immutables.merge(values, value);
        
        return newUpdateQuery(keys, 
                              whereConditions, 
                              valuesToMutate, 
                              setValuesToAdd,
                              setValuesToRemove,
                              listValuesToAppend,
                              Immutables.merge(listValuesToPrepend, name, values),
                              listValuesToRemove, 
                              mapValuesToMutate,
                              ifConditions);
    }
    
    

    @Override
    public Write appendListValue(String name, Object value) {
        ImmutableList<Object> values = listValuesToAppend.get(name);
        values = (values == null) ? ImmutableList.of(value) : Immutables.merge(values, value);
        
        return newUpdateQuery(keys, 
                              whereConditions, 
                              valuesToMutate, 
                              setValuesToAdd,
                              setValuesToRemove,
                              Immutables.merge(listValuesToAppend, name, values),
                              listValuesToPrepend,
                              listValuesToRemove,
                              mapValuesToMutate,
                              ifConditions);
    }
    
    
    
    @Override
    public Write removeListValue(String name, Object value) {
        ImmutableList<Object> values = listValuesToRemove.get(name);
        values = (values == null) ? ImmutableList.of(value) : Immutables.merge(values, value);
        
        return newUpdateQuery(keys, 
                              whereConditions, 
                              valuesToMutate, 
                              setValuesToAdd,
                              setValuesToRemove,
                              listValuesToAppend,
                              listValuesToPrepend,
                              Immutables.merge(listValuesToRemove, name, values),
                              mapValuesToMutate,
                              ifConditions);

    }
   
    
    @Override
    public Write putMapValue(String name, Object key, Object value) {
        ImmutableMap<Object, Optional<Object>> values = mapValuesToMutate.get(name);
        values = (values == null) ? ImmutableMap.of(key, toOptional(value)) : Immutables.merge(values, key, toOptional(value));
        
        return newUpdateQuery(keys, 
                              whereConditions, 
                              valuesToMutate, 
                              setValuesToAdd,
                              setValuesToRemove,
                              listValuesToAppend,
                              listValuesToPrepend,
                              listValuesToRemove,
                              Immutables.merge(mapValuesToMutate, name, values),
                              ifConditions);
    }
    
    
    @Override
    public Insertion ifNotExits() {
        return newInsertionQuery(Immutables.merge(valuesToMutate, Immutables.transform(keys, name -> name, value -> toOptional(value))), true);
    }

    @Override
    public Update<Write> onlyIf(Clause... conditions) {
        return newUpdateQuery(keys, 
                              whereConditions, 
                              valuesToMutate, 
                              setValuesToAdd,
                              setValuesToRemove,
                              listValuesToAppend,
                              listValuesToPrepend,
                              listValuesToRemove,
                              mapValuesToMutate,
                              ImmutableList.<Clause>builder().addAll(ifConditions).addAll(ImmutableList.copyOf(conditions)).build());
    }

    
    @Override
    protected Statement getStatement() {
        
        // statement
        com.datastax.driver.core.querybuilder.Update update = update(getTable());
        
        ifConditions.forEach(condition -> update.onlyIf(condition));

        
        // key-based update
        if (whereConditions.isEmpty()) {
            List<Object> values = Lists.newArrayList();
            
            valuesToMutate.forEach((name, optionalValue) -> { update.with(set(name, bindMarker())); values.add(toStatementValue(name, optionalValue.orElse(null))); });

            setValuesToAdd.forEach((name, vals) -> { update.with(addAll(name, bindMarker())); values.add(toStatementValue(name, vals)); });
            setValuesToRemove.forEach((name, vals) -> { update.with(removeAll(name, bindMarker())); values.add(toStatementValue(name, vals)); });
            
            listValuesToPrepend.forEach((name, vals) -> { update.with(prependAll(name, bindMarker())); values.add(toStatementValue(name, vals)); });
            listValuesToAppend.forEach((name, vals) -> { update.with(appendAll(name, bindMarker())); values.add(toStatementValue(name, vals)); });
            listValuesToRemove.forEach((name, vals) -> { update.with(discardAll(name, bindMarker())); values.add(toStatementValue(name, vals)); });

            mapValuesToMutate.forEach((name, map) -> { update.with(putAll(name, bindMarker())); values.add(toStatementValue(name, map)); });
            
            
            keys.keySet().forEach(keyname -> { update.where(eq(keyname, bindMarker())); values.add(keys.get(keyname)); } );
            
            ifConditions.forEach(condition -> update.onlyIf(condition));
            getTtl().ifPresent(ttl-> { update.using(QueryBuilder.ttl(bindMarker())); values.add((int) ttl.getSeconds()); });
            
            return prepare(update).bind(values.toArray());

            
        // where condition-based update
        } else {
            valuesToMutate.forEach((name, optionalValue) -> update.with(set(name, toStatementValue(name, optionalValue.orElse(null)))));
        
            setValuesToAdd.forEach((name, vals) -> update.with(addAll(name, toStatementValue(name, vals))));
            setValuesToRemove.forEach((name, vals) -> update.with(removeAll(name, toStatementValue(name, vals))));
  
            listValuesToPrepend.forEach((name, vals) -> update.with(prependAll(name, toStatementValue(name, vals))));
            listValuesToAppend.forEach((name, vals) -> update.with(appendAll(name, toStatementValue(name, vals))));
            listValuesToRemove.forEach((name, vals) -> update.with(discardAll(name, toStatementValue(name, vals))));
            
            mapValuesToMutate.forEach((name, map) -> update.with(putAll(name, toStatementValue(name, map))));

            getTtl().ifPresent(ttl-> update.using(QueryBuilder.ttl((int) ttl.getSeconds())));
            whereConditions.forEach(whereClause -> update.where(whereClause));
            
            return update;
        }
    }
    
    
    @Override
    public CompletableFuture<Result> executeAsync() {
        return super.executeAsync().thenApply(result ->  {
                                                            // check cas result column '[applied]'
                                                            if (!ifConditions.isEmpty() && !result.wasApplied()) {
                                                                throw new IfConditionException("if condition does not match");  
                                                            } 
                                                            return result;
                                                        });
    }
}


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



import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.set;
import static com.datastax.driver.core.querybuilder.QueryBuilder.update;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.unitedinternet.troilus.Dao.Insertion;
import com.unitedinternet.troilus.Dao.UpdateWithValues;
import com.unitedinternet.troilus.Dao.Update;
import com.unitedinternet.troilus.Dao.Write;


 
class UpdateQuery extends MutationQuery<Write> implements Write {

    private final ImmutableMap<String, Object> keys;
    private final ImmutableMap<String, Optional<Object>> valuesToMutate;
    private final ImmutableList<Clause> ifConditions;
    private final ImmutableList<Clause> whereConditions;


    protected static UpdateWithValues<?> newUpdateQuery(Context ctx, ImmutableList<Clause> whereConditions) {
        return new UpdateQuery(ctx, ImmutableMap.of(), whereConditions, ImmutableMap.of(), ImmutableList.of());
    }
    
    protected static Write newUpdateQuery(Context ctx, ImmutableMap<String, Object> keys) {
        return new UpdateQuery(ctx, keys, ImmutableList.of(), ImmutableMap.of(), ImmutableList.of());
    }
    
    
    public UpdateQuery(Context ctx, ImmutableMap<String, Object> keys, ImmutableList<Clause> whereConditions, ImmutableMap<String, Optional<Object>> valuesToMutate, ImmutableList<Clause> ifConditions) {
        super(ctx);
        this.keys = keys;
        this.whereConditions = whereConditions;
        this.valuesToMutate = valuesToMutate;
        this.ifConditions = ifConditions;
    }
    
    @Override
    protected UpdateQuery newQuery(Context newContext) {
        return new UpdateQuery(newContext, 
                               keys, 
                               whereConditions,  
                               valuesToMutate,  
                               ifConditions);
    }
    
    
    
    @Override
    public UpdateQuery value(String name, Object value) {
        return new UpdateQuery(getContext(), keys, whereConditions, Immutables.merge(valuesToMutate, name, toOptional(value)), ifConditions);
    }

    
    @Override
    public UpdateQuery values(ImmutableMap<String, Object> nameValuePairsToAdd) {
        return new UpdateQuery(getContext(), keys, whereConditions, Immutables.merge(valuesToMutate, Immutables.transform(nameValuePairsToAdd, name -> name, value -> toOptional(value))), ifConditions);
    }
    
    
    @Override
    public UpdateQuery removeValue(String name) {
        return value(name, null);
    }
    
    
    @Override
    public Insertion ifNotExits() {
        return InsertionQuery.newInsertionQuery(getContext(), Immutables.merge(valuesToMutate, Immutables.transform(keys, name -> name, value -> toOptional(value))), true);
    }

    @Override
    public Update<Write> onlyIf(Clause... conditions) {
        return new UpdateQuery(getContext(),
                               keys, 
                               whereConditions, 
                               valuesToMutate, 
                               ImmutableList.<Clause>builder().addAll(ifConditions).addAll(ImmutableList.copyOf(conditions)).build());
    }

    
    @Override
    protected Statement getStatement() {
        
        // statement
        com.datastax.driver.core.querybuilder.Update update = update(getContext().getTable());
        
        ifConditions.forEach(condition -> update.onlyIf(condition));

        // key-based update
        if (whereConditions.isEmpty()) {
            List<Object> values = Lists.newArrayList();
            valuesToMutate.forEach((name, optionalValue) -> { update.with(set(name, bindMarker())); values.add(toStatementValue(name, optionalValue.orElse(null))); });
            
            keys.keySet().forEach(keyname -> { update.where(eq(keyname, bindMarker())); values.add(keys.get(keyname)); } );
            
            
            ifConditions.forEach(condition -> update.onlyIf(condition));
   
            getTtl().ifPresent(ttl-> {
                                        update.using(QueryBuilder.ttl(bindMarker()));
                                        values.add((int) ttl.getSeconds());
                                     });
            
            PreparedStatement stmt = prepare(update);
            return stmt.bind(values.toArray());

            
        // where condition-based update
        } else {
            valuesToMutate.forEach((name, optionalValue) -> update.with(set(name, toStatementValue(name, optionalValue.orElse(null)))));
            
            getTtl().ifPresent(ttl-> update.using(QueryBuilder.ttl((int) ttl.getSeconds())));
            
            com.datastax.driver.core.querybuilder.Update.Where where = null;
            
            for (Clause whereClause : whereConditions) {
                if (where == null) {
                    where = update.where(whereClause);
                } else {
                    where = where.and(whereClause);
                }
            }
            
            return update;
        }
    }
    
    @Override
    public CompletableFuture<Result> executeAsync() {
        return super.executeAsync().thenApply(result ->  {
            if (!ifConditions.isEmpty()) {
                // check cas result column '[applied]'
                if (!result.wasApplied()) {
                    throw new IfConditionException("if condition does not match");  
                }
            } 
            return result;
        });
    }
}


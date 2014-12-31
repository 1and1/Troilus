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



import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Statement;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.unitedinternet.troilus.Dao.Update;
import com.unitedinternet.troilus.Dao.UpdateWithValues;

 
class UpdateQuery extends MutationQuery<UpdateWithValues> implements UpdateWithValues {
    private final QueryFactory queryFactory;
    private final ImmutableMap<String, Object> keys;
    private final ImmutableList<Clause> whereConditions;
    private final ImmutableList<? extends ValueToMutate> valuesToMutate;
    private final ImmutableList<Clause> ifConditions;
    
    public UpdateQuery(Context ctx, QueryFactory queryFactory, ImmutableList<? extends ValueToMutate> valuesToMutate, ImmutableMap<String, Object> keys, ImmutableList<Clause> whereConditions, ImmutableList<Clause> ifConditions) {
        super(ctx, queryFactory);
        this.queryFactory = queryFactory;
        this.valuesToMutate = valuesToMutate;
        this.keys = keys;
        this.whereConditions = whereConditions;
        this.ifConditions = ifConditions;
    }
    
   
    @Override
    protected UpdateWithValues newQuery(Context ctx) {
        return queryFactory.newUpdate(ctx, valuesToMutate, keys, whereConditions, ifConditions);
    }
   
    
    
    @Override
    public Update onlyIf(Clause... conditions) {
        return queryFactory.newUpdate(getContext(), valuesToMutate, keys, whereConditions, ImmutableList.copyOf(conditions)) ;
    }
    
    @Override
    public final  UpdateWithValues values(ImmutableMap<String, ? extends Object> additionalvaluesToMutate) {
        UpdateWithValues write = this;
        for (String name : additionalvaluesToMutate.keySet()) {
            write = write.value(name, additionalvaluesToMutate.get(name));
        }
        return write;
    }
    
 
    @Override
    public UpdateWithValues value(String name, Object value) {
        if (isOptional(value)) {
            Optional<Object> optional = (Optional<Object>) value;
            if (optional.isPresent()) {
                return value(name, optional.get());
            } else {
                return this;
            }
        }
        
        ImmutableList<? extends ValueToMutate> newValuesToInsert;
        
        if (isBuildInType(getColumnMetadata(name).getType())) {
            newValuesToInsert = ImmutableList.<ValueToMutate>builder().addAll(valuesToMutate).add(new BuildinValueToMutate(name, value)).build();
        } else {
            newValuesToInsert = ImmutableList.<ValueToMutate>builder().addAll(valuesToMutate).add(new UDTValueToMutate(name, value)).build();
        }
        
        return queryFactory.newUpdate(getContext(), newValuesToInsert, keys, whereConditions, ifConditions);
    }
       
 
  
    
    @Override
    public Statement getStatement() {
        
        // statement
        com.datastax.driver.core.querybuilder.Update update = update(getContext().getTable());
        
        ifConditions.forEach(condition -> update.onlyIf(condition));

        // key-based update
        if (whereConditions.isEmpty()) {
            List<Object> values = Lists.newArrayList();
            valuesToMutate.forEach(valueToInsert -> values.add(valueToInsert.addPreparedToStatement(update)));
            
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
            valuesToMutate.forEach(valueToInsert -> valueToInsert.addToStatement(update));
            
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

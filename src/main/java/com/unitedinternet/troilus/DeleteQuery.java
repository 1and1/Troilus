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



import java.util.concurrent.CompletableFuture;

import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.Delete;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

import com.datastax.driver.core.Statement;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.unitedinternet.troilus.Dao.Deletion;



 
class DeleteQuery extends MutationQuery<Deletion> implements Deletion {
    
    private final QueryFactory queryFactory;
    private final ImmutableMap<String, Object> keyNameValuePairs;
    private final ImmutableList<Clause> whereConditions;
    private final ImmutableList<Clause> ifConditions;
     
    
    public DeleteQuery(Context ctx, QueryFactory queryFactory, ImmutableMap<String, Object> keyNameValuePairs, ImmutableList<Clause> whereConditions, ImmutableList<Clause> ifConditions) {
        super(ctx, queryFactory);
        this.queryFactory = queryFactory;
        this.keyNameValuePairs = keyNameValuePairs;
        this.whereConditions = whereConditions;
        this.ifConditions = ifConditions;
    }

    @Override
    protected Deletion newQuery(Context newContext) {
        return new DeleteQuery(newContext, queryFactory, keyNameValuePairs, whereConditions, ifConditions);
    }
    
    
    @Override
    public Deletion onlyIf(Clause... conditions) {
        return new DeleteQuery(getContext(), queryFactory, keyNameValuePairs, whereConditions, ImmutableList.copyOf(conditions));
    }
    
 
    @Override
    public Statement getStatement() {
        Delete delete = delete().from(getTable());

        // key-based delete    
        if (whereConditions.isEmpty()) {
            ifConditions.forEach(condition -> delete.onlyIf(condition));
            
            Delete.Where where = null;
            
            if (!keyNameValuePairs.isEmpty()) {
                for (Clause whereClause : keyNameValuePairs.keySet().stream().map(name -> eq(name, bindMarker())).collect(Immutables.toSet())) {
                    if (where == null) {
                        where = delete.where(whereClause);
                    } else {
                        where = where.and(whereClause);
                    }
                }
            }
            
            return prepare(delete).bind(keyNameValuePairs.values().toArray());

            
        // where condition-based delete    
        } else {
            Delete.Where where = null;
            
            ifConditions.forEach(condition -> delete.onlyIf(condition));
            
            for (Clause whereClause : whereConditions) {
                if (where == null) {
                    where = delete.where(whereClause);
                } else {
                    where = where.and(whereClause);
                }
            }
           
            return delete;
        }
    }
    

    @Override
    public CompletableFuture<Result> executeAsync() {
        return super.executeAsync().thenApply(result -> {
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
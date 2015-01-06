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
import static com.datastax.driver.core.querybuilder.QueryBuilder.delete;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;


import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.Delete;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;



 
public class DeleteQueryData extends QueryData {
    
    private final ImmutableMap<String, Object> keyNameValuePairs;
    private final ImmutableList<Clause> whereConditions;
    private final ImmutableList<Clause> onlyIfConditions;
     

    DeleteQueryData(ImmutableMap<String, Object> keyNameValuePairs, 
                    ImmutableList<Clause> whereConditions, 
                    ImmutableList<Clause> onlyIfConditions) {
        this.keyNameValuePairs = keyNameValuePairs;
        this.whereConditions = whereConditions;
        this.onlyIfConditions = onlyIfConditions;
    }

    

    public DeleteQueryData withKeys(ImmutableMap<String, Object> keyNameValuePairs) {
        return new DeleteQueryData(keyNameValuePairs, 
                                   this.whereConditions, 
                                   this.onlyIfConditions);  
    }
    
    public DeleteQueryData withWhereConditions(ImmutableList<Clause> whereConditions) {
        return new DeleteQueryData(this.keyNameValuePairs, 
                                   whereConditions, 
                                   this.onlyIfConditions);  
    }
    
    
    public DeleteQueryData withOnlyIfConditions(ImmutableList<Clause> onlyIfConditions) {
        return new DeleteQueryData(this.keyNameValuePairs, 
                                   this.whereConditions, 
                                   onlyIfConditions);  
    }
    
    
    public ImmutableMap<String, Object> getKeyNameValuePairs() {
        return keyNameValuePairs;
    }

    public ImmutableList<Clause> getWhereConditions() {
        return whereConditions;
    }

    public ImmutableList<Clause> getOnlyIfConditions() {
        return onlyIfConditions;
    }
    
    
    
    Statement toStatement(Context ctx) {
        Delete delete = delete().from(ctx.getTable());

        // key-based delete    
        if (whereConditions.isEmpty()) {
            onlyIfConditions.forEach(condition -> delete.onlyIf(condition));
            
            ImmutableSet<Clause> whereClauses = keyNameValuePairs.keySet().stream().map(name -> eq(name, bindMarker())).collect(Immutables.toSet());
            whereClauses.forEach(whereClause -> delete.where(whereClause));
            
            return ctx.prepare(delete).bind(keyNameValuePairs.values().toArray());

            
        // where condition-based delete    
        } else {
            onlyIfConditions.forEach(condition -> delete.onlyIf(condition));
            whereConditions.forEach(whereClause -> delete.where(whereClause));
           
            return delete;
        }
    }
}
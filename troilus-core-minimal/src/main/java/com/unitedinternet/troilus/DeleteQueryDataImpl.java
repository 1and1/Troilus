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



import static com.datastax.driver.core.querybuilder.QueryBuilder.delete;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;




import java.util.List;
import java.util.Map.Entry;

import com.datastax.driver.core.querybuilder.Clause;


import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.Statement;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.unitedinternet.troilus.interceptor.DeleteQueryData;




 
class DeleteQueryDataImpl implements DeleteQueryData {
    private final ImmutableMap<String, Object> keyNameValuePairs;
    private final ImmutableList<Clause> whereConditions;
    private final ImmutableList<Clause> onlyIfConditions;
     

    DeleteQueryDataImpl() {
        this(ImmutableMap.<String, Object>of(), 
             ImmutableList.<Clause>of(), 
             ImmutableList.<Clause>of());
    }
    
    
    private DeleteQueryDataImpl(ImmutableMap<String, Object> keyNameValuePairs, 
                            ImmutableList<Clause> whereConditions, 
                            ImmutableList<Clause> onlyIfConditions) {   
        this.keyNameValuePairs = keyNameValuePairs;
        this.whereConditions = whereConditions;
        this.onlyIfConditions = onlyIfConditions;
    }

    

    public DeleteQueryDataImpl keys(ImmutableMap<String, Object> keyNameValuePairs) {
        return new DeleteQueryDataImpl(keyNameValuePairs, 
                                   this.whereConditions, 
                                   this.onlyIfConditions);  
    }
    
    public DeleteQueryDataImpl whereConditions(ImmutableList<Clause> whereConditions) {
        return new DeleteQueryDataImpl(this.keyNameValuePairs, 
                                   whereConditions, 
                                   this.onlyIfConditions);  
    }
    
    
    public DeleteQueryDataImpl onlyIfConditions(ImmutableList<Clause> onlyIfConditions) {
        return new DeleteQueryDataImpl(this.keyNameValuePairs, 
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
    
    
    static Statement toStatement(DeleteQueryData data, Context ctx) {
        Delete delete = delete().from(ctx.getTable());

        for (Clause onlyIfCondition : data.getOnlyIfConditions()) {
            delete.onlyIf(onlyIfCondition);
        }

        
        // key-based delete    
        if (data.getWhereConditions().isEmpty()) {
            List<Object> values = Lists.newArrayList();
            
            for (Entry<String, Object> entry : data.getKeyNameValuePairs().entrySet()) {
                Clause keybasedWhereClause = eq(entry.getKey(), bindMarker());
                delete.where(keybasedWhereClause);
                                
                values.add(ctx.toStatementValue(entry.getKey(), entry.getValue()));
            }
            
            return ctx.prepare(delete).bind(values.toArray());

            
        // where condition-based delete    
        } else {
            for (Clause whereCondition : data.getWhereConditions()) {
                delete.where(whereCondition);
            }
           
            return delete;
        }        
    }
}
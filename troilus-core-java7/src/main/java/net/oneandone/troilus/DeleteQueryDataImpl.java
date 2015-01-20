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



import static com.datastax.driver.core.querybuilder.QueryBuilder.delete;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;









import java.util.List;
import java.util.Map.Entry;

import net.oneandone.troilus.interceptor.DeleteQueryData;

import com.datastax.driver.core.querybuilder.Clause;


import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.Statement;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;




/**
 * Delete query data implementation
 */
class DeleteQueryDataImpl implements DeleteQueryData {
 
    private final ImmutableMap<String, Object> keyNameValuePairs;
    private final ImmutableList<Clause> whereConditions;
    private final ImmutableList<Clause> onlyIfConditions;
    private final Boolean ifExists;
     

    /**
     * constructor 
     */
    DeleteQueryDataImpl() {
        this(ImmutableMap.<String, Object>of(), 
             ImmutableList.<Clause>of(), 
             ImmutableList.<Clause>of(),
             null);
    }
    
    private DeleteQueryDataImpl(ImmutableMap<String, Object> keyNameValuePairs, 
                            ImmutableList<Clause> whereConditions, 
                            ImmutableList<Clause> onlyIfConditions,
                            Boolean ifExists) {   
        this.keyNameValuePairs = keyNameValuePairs;
        this.whereConditions = whereConditions;
        this.onlyIfConditions = onlyIfConditions;
        this.ifExists = ifExists;
    }
    
    @Override
    public DeleteQueryDataImpl keys(ImmutableMap<String, Object> keyNameValuePairs) {
        return new DeleteQueryDataImpl(keyNameValuePairs, 
                                       this.whereConditions, 
                                       this.onlyIfConditions,
                                       this.ifExists);  
    }
    
    @Override
    public DeleteQueryDataImpl whereConditions(ImmutableList<Clause> whereConditions) {
        return new DeleteQueryDataImpl(this.keyNameValuePairs, 
                                       whereConditions, 
                                       this.onlyIfConditions,
                                       this.ifExists);  
    }
    
    @Override
    public DeleteQueryDataImpl onlyIfConditions(ImmutableList<Clause> onlyIfConditions) {
        return new DeleteQueryDataImpl(this.keyNameValuePairs, 
                                       this.whereConditions, 
                                       onlyIfConditions,
                                       this.ifExists);  
    }
    
    @Override
    public DeleteQueryDataImpl ifExists(Boolean ifExists) {
        return new DeleteQueryDataImpl(this.keyNameValuePairs, 
                                       this.whereConditions, 
                                       this.onlyIfConditions,
                                       ifExists);  
    }
    
    @Override
    public ImmutableMap<String, Object> getKeys() {
        return keyNameValuePairs;
    }

    @Override
    public ImmutableList<Clause> getWhereConditions() {
        return whereConditions;
    }

    @Override
    public ImmutableList<Clause> getOnlyIfConditions() {
        return onlyIfConditions;
    }
    
    @Override
    public Boolean getIfExists() {
        return ifExists;
    }
    
    /**
     * @param data  the data 
     * @param ctx   the context
     * @return the query data statement
     */
    static Statement toStatement(DeleteQueryData data, Context ctx) {
        Delete delete = delete().from(ctx.getTable());

        for (Clause onlyIfCondition : data.getOnlyIfConditions()) {
            delete.onlyIf(onlyIfCondition);
        }
        
        if ((data.getIfExists() != null) && data.getIfExists()) {
            delete.ifExists();
        }
        
        // key-based delete    
        if (data.getWhereConditions().isEmpty()) {
            List<Object> values = Lists.newArrayList();
            
            for (Entry<String, Object> entry : data.getKeys().entrySet()) {
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
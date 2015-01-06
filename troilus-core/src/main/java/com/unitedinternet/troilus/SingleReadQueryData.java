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
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;

import java.util.Optional;

import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Select.Selection;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;



 
public class SingleReadQueryData extends QueryData {

    final ImmutableMap<String, Object> keyNameValuePairs;
    final Optional<ImmutableMap<String, Boolean>> optionalColumnsToFetch;



    public SingleReadQueryData(ImmutableMap<String, Object> keyNameValuePairs,
                               Optional<ImmutableMap<String, Boolean>> optionalColumnsToFetchs) {
        this.keyNameValuePairs = keyNameValuePairs;
        this.optionalColumnsToFetch = optionalColumnsToFetchs;
    }
    
    

    public SingleReadQueryData withKeys(ImmutableMap<String, Object> keyNameValuePairs) {
        return new SingleReadQueryData(keyNameValuePairs, 
                                       this.optionalColumnsToFetch);  
    }
    

    public SingleReadQueryData withColumnsToFetchKeys(Optional<ImmutableMap<String, Boolean>> optionalColumnsToFetch) {
        return new SingleReadQueryData(this.keyNameValuePairs, 
                                       optionalColumnsToFetch);  
    }
    
    
    
    public ImmutableMap<String, Object> getKeyNameValuePairs() {
        return keyNameValuePairs;
    }

    public Optional<ImmutableMap<String, Boolean>> getColumnsToFetch() {
        return optionalColumnsToFetch;
    }

    


    @Override
    protected Statement toStatement(Context ctx) {
        Selection selection = select();
        
        if (optionalColumnsToFetch.isPresent()) {
            
            optionalColumnsToFetch.get().forEach((columnName, withMetaData) -> selection.column(columnName));
            optionalColumnsToFetch.get().entrySet()
                                        .stream()
                                        .filter(entry -> entry.getValue())
                                        .forEach(entry -> { selection.ttl(entry.getKey()); selection.writeTime(entry.getKey()); });

            // add key columns for paranoia checks
            keyNameValuePairs.keySet()
                             .stream()
                             .filter(columnName -> !optionalColumnsToFetch.get().containsKey(columnName))
                             .forEach(columnName -> selection.column(columnName));  
            
        } else {
            selection.all();
        }
        
        
        
        Select select = selection.from(ctx.getTable());
        
        ImmutableSet<Clause> whereConditions = keyNameValuePairs.keySet().stream().map(name -> eq(name, bindMarker())).collect(Immutables.toSet());
        whereConditions.forEach(whereCondition -> select.where(whereCondition));

        return ctx.prepare(select).bind(keyNameValuePairs.values().toArray());
    }
}
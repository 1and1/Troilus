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

import java.util.List;
import java.util.Map.Entry;

import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Select.Selection;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;



 
public class SingleReadQueryData {

    final ImmutableMap<String, Object> keyNameValuePartPairs;
    final ImmutableMap<String, Boolean> columnsToFetch;


    
    SingleReadQueryData() {
        this(ImmutableMap.<String, Object>of(), 
             ImmutableMap.<String, Boolean>of());
    }
    

    private SingleReadQueryData(ImmutableMap<String, Object> keyNameValuePartPairs,
                                ImmutableMap<String, Boolean> columnsToFetchs) {
        this.keyNameValuePartPairs = keyNameValuePartPairs;
        this.columnsToFetch = columnsToFetchs;
    }
    
    

    public SingleReadQueryData keyParts(ImmutableMap<String, Object> keyNameValuePartPairs) {
        return new SingleReadQueryData(keyNameValuePartPairs, 
                                       this.columnsToFetch);  
    }
    

    public SingleReadQueryData columnsToFetch(ImmutableMap<String, Boolean> columnsToFetchs) {
        return new SingleReadQueryData(this.keyNameValuePartPairs, 
                                       columnsToFetchs);  
    }
    
    
    
    public ImmutableMap<String, Object> getKeyParts() {
        return keyNameValuePartPairs;
    }

    public ImmutableMap<String, Boolean> getColumnsToFetch() {
        return columnsToFetch;
    }


    
    Statement toStatement(Context ctx) {
        Selection selection = select();
        
        
        // set the columns to fetch 
        if (getColumnsToFetch().isEmpty()) {
            selection.all();

        } else {
            for (Entry<String, Boolean> entry : columnsToFetch.entrySet()) {
                selection.column(entry.getKey());
                if (entry.getValue()) {
                    selection.ttl(entry.getKey());
                    selection.writeTime(entry.getKey());
                }
            }

            // add key columns to requested columns (for paranoia checks)
            for (String keyname : getKeyParts().keySet()) {
                if (columnsToFetch.get(keyname) == null) {
                    selection.column(keyname);
                }
            }
        }
        
        
        
        Select select = selection.from(ctx.getTable());
        
        // set the query conditions 
        List<Object> values = Lists.newArrayList();
        for (Entry<String, Object> entry : getKeyParts().entrySet()) {
            select.where(eq(entry.getKey(), bindMarker()));
            values.add(ctx.toStatementValue(entry.getKey(), entry.getValue()));
        }
        

        return ctx.prepare(select).bind(values.toArray());
    }
}
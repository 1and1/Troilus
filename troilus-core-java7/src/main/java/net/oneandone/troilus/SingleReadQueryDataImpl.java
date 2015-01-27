/*
 * Copyright 1&1 Internet AG, https://github.com/1and1/
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.oneandone.troilus;



import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;

import java.util.List;
import java.util.Map.Entry;

import net.oneandone.troilus.interceptor.SingleReadQueryData;

import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Select.Selection;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;


/**
 * Read data query implementation
 */
class SingleReadQueryDataImpl implements SingleReadQueryData {

    final ImmutableMap<String, Object> keyNameValuePartPairs;
    final ImmutableMap<String, Boolean> columnsToFetch;


    /**
     * constructor 
     */
    SingleReadQueryDataImpl() {
        this(ImmutableMap.<String, Object>of(), 
             ImmutableMap.<String, Boolean>of());
    }
    

    private SingleReadQueryDataImpl(ImmutableMap<String, Object> keyNameValuePartPairs,
                                    ImmutableMap<String, Boolean> columnsToFetchs) {
        this.keyNameValuePartPairs = keyNameValuePartPairs;
        this.columnsToFetch = columnsToFetchs;
    }
    
    @Override
    public SingleReadQueryDataImpl key(ImmutableMap<String, Object> keyNameValuePartPairs) {
        return new SingleReadQueryDataImpl(keyNameValuePartPairs, 
                                           this.columnsToFetch);  
    }
    
    @Override
    public SingleReadQueryDataImpl columnsToFetch(ImmutableMap<String, Boolean> columnsToFetchs) {
        return new SingleReadQueryDataImpl(this.keyNameValuePartPairs, 
                                           columnsToFetchs);  
    }
    
    @Override
    public ImmutableMap<String, Object> getKey() {
        return keyNameValuePartPairs;
    }

    @Override
    public ImmutableMap<String, Boolean> getColumnsToFetch() {
        return columnsToFetch;
    }
    
  
    /**
     * @param data   the query data
     * @param ctx    the context 
     * @return the query as statement 
     */
    static Statement toStatement(SingleReadQueryData data, Context ctx) {
        Selection selection = select();
        
        // set the columns to fetch 
        if (data.getColumnsToFetch().isEmpty()) {
            selection.all();

        } else {
            for (Entry<String, Boolean> entry : data.getColumnsToFetch().entrySet()) {
                selection.column(entry.getKey());
                if (entry.getValue()) {
                    selection.ttl(entry.getKey());
                    selection.writeTime(entry.getKey());
                }
            }

            // add key columns to requested columns (for paranoia checks)
            for (String keyname : data.getKey().keySet()) {
                if (data.getColumnsToFetch().get(keyname) == null) {
                    selection.column(keyname);
                }
            }
        }
        
        
        
        Select select = selection.from(ctx.getDbSession().getTablename());
        
        // set the query conditions 
        List<Object> values = Lists.newArrayList();
        for (Entry<String, Object> entry : data.getKey().entrySet()) {
            select.where(eq(entry.getKey(), bindMarker()));
            values.add(ctx.toStatementValue(entry.getKey(), entry.getValue()));
        }
        

        return ctx.getDbSession().prepare(select).bind(values.toArray());
    }

}
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
import static com.datastax.driver.core.querybuilder.QueryBuilder.in;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;

import java.util.List;
import java.util.Map.Entry;

import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.Select;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import com.unitedinternet.troilus.minimal.ListReadQueryData;


 
class ListReadQueryDataImpl implements ListReadQueryData {

    /*
     * THIS WILL BE REMOVED BY GIVING UP JAVA 7 SUPPORT
     */
    
    private final ImmutableMap<String, ImmutableList<Object>> keys;
    private final ImmutableSet<Clause> whereClauses;
    private final ImmutableMap<String, Boolean> columnsToFetch;
    private final Integer limit;
    private final Boolean allowFiltering;
    private final Integer fetchSize;
    private final Boolean distinct;

    
    ListReadQueryDataImpl() {
        this(ImmutableMap.<String, ImmutableList<Object>>of(),
             ImmutableSet.<Clause>of(), 
             ImmutableMap.<String, Boolean>of(),
             null,
             null,
             null,
             null);
    }

    
    private ListReadQueryDataImpl(ImmutableMap<String, ImmutableList<Object>> keys,
                                     ImmutableSet<Clause> whereClauses, 
                                     ImmutableMap<String, Boolean> columnsToFetch, 
                                     Integer limit, 
                                     Boolean allowFiltering,
                                     Integer fetchSize,
                                     Boolean distinct) {
        this.keys = keys;
        this.whereClauses = whereClauses;
        this.columnsToFetch = columnsToFetch;
        this.limit = limit;
        this.allowFiltering = allowFiltering;
        this.fetchSize = fetchSize;
        this.distinct = distinct;
    }
    

    

    public ListReadQueryDataImpl keys(ImmutableMap<String, ImmutableList<Object>> keys) {
        return new ListReadQueryDataImpl(keys,
                                            this.whereClauses,
                                            this.columnsToFetch,
                                            this.limit,
                                            this.allowFiltering,
                                            this.fetchSize,
                                            this.distinct);  
    }
    
    public ListReadQueryDataImpl whereConditions(ImmutableSet<Clause> whereClauses) {
        return new ListReadQueryDataImpl(this.keys,
                                            whereClauses,
                                            this.columnsToFetch,
                                            this.limit,
                                            this.allowFiltering,
                                            this.fetchSize,
                                            this.distinct);  
    }

    
    public ListReadQueryDataImpl columnsToFetch(ImmutableMap<String, Boolean> columnsToFetch) {
        return new ListReadQueryDataImpl(this.keys,
                                            this.whereClauses,
                                            columnsToFetch,
                                            this.limit,
                                            this.allowFiltering,
                                            this.fetchSize,
                                            this.distinct);  
    }

    
    public ListReadQueryDataImpl limit(Integer limit) {
        return new ListReadQueryDataImpl(this.keys,
                                            this.whereClauses,
                                            this.columnsToFetch,
                                            limit,
                                            this.allowFiltering,
                                            this.fetchSize,
                                            this.distinct);  
    }

    
    public ListReadQueryDataImpl allowFiltering(Boolean allowFiltering) {
        return new ListReadQueryDataImpl(this.keys,
                                            this.whereClauses,
                                            this.columnsToFetch,
                                            this.limit,
                                            allowFiltering,
                                            this.fetchSize,
                                            this.distinct);  
    }

    
    public ListReadQueryDataImpl fetchSize(Integer fetchSize) {
        return new ListReadQueryDataImpl(this.keys,
                                            this.whereClauses,
                                            this.columnsToFetch,
                                            this.limit,
                                            this.allowFiltering,
                                            fetchSize,
                                            this.distinct);  
    }

    
    public ListReadQueryDataImpl distinct(Boolean distinct) {
        return new ListReadQueryDataImpl(this.keys,
                                            this.whereClauses,
                                            this.columnsToFetch,
                                            this.limit,
                                            this.allowFiltering,
                                            this.fetchSize,
                                            distinct);  
    }
    
    
    public ImmutableMap<String, ImmutableList<Object>> getKeys() {
        return keys;
    }
    
    public ImmutableSet<Clause> getWhereClauses() {
        return whereClauses;
    }

    public ImmutableMap<String, Boolean> getColumnsToFetch() {
        return columnsToFetch;
    }

    public Integer getLimit() {
        return limit;
    }

    public Boolean getAllowFiltering() {
        return allowFiltering;
    }

    public Integer getFetchSize() {
        return fetchSize;
    }

    public Boolean getDistinct() {
        return distinct;
    }
    
     
    static Statement toStatement(ListReadQueryData data, Context ctx) {
        Select.Selection selection = select();

        if ((data.getDistinct() != null) && data.getDistinct()) {
            selection.distinct(); 
        }

        
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
        }
        
        Select select = selection.from(ctx.getTable());
  
        if (data.getLimit() != null) {
            select.limit(data.getLimit());
        }

        if ((data.getAllowFiltering() != null) && data.getAllowFiltering()) {
            select.allowFiltering();
        } 

        if (data.getFetchSize() != null) {
            select.setFetchSize(data.getFetchSize());
        }
        
        
        
        // where-based selection
        if (data.getKeys().isEmpty()) {
            for (Clause whereClause : data.getWhereClauses()) {
                select.where(whereClause);
            }
            
            return select;

            
        // key-based selection    
        } else {
            List<Object> values = Lists.newArrayList();

            for (Entry<String, ImmutableList<Object>> entry : data.getKeys().entrySet()) {
                select.where(in(entry.getKey(), bindMarker()));
                values.add(ctx.toStatementValues(entry.getKey(), entry.getValue()));
                
            }

            return ctx.prepare(select).bind(values.toArray());
        }
    }
}
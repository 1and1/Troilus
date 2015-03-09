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



import static com.datastax.driver.core.querybuilder.QueryBuilder.in;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;

import java.util.List;
import java.util.Map.Entry;

import net.oneandone.troilus.java7.interceptor.ReadQueryData;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.Select;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;


 
/**
 * List query data implementation
 */
class ReadQueryDataImpl implements ReadQueryData {
    
    private final ImmutableMap<String, ImmutableList<Object>> keys;
    private final ImmutableSet<Clause> whereClauses;
    private final ImmutableMap<String, Boolean> columnsToFetch;
    private final Integer limit;
    private final Boolean allowFiltering;
    private final Integer fetchSize;
    private final Boolean distinct;

    
    /**
     * Constructor
     */
    ReadQueryDataImpl() {
        this(ImmutableMap.<String, ImmutableList<Object>>of(),
             ImmutableSet.<Clause>of(), 
             ImmutableMap.<String, Boolean>of(),
             null,
             null,
             null,
             null);
    }

    
    private ReadQueryDataImpl(ImmutableMap<String, ImmutableList<Object>> keys,
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
    

    @Override
    public ReadQueryDataImpl keys(ImmutableMap<String, ImmutableList<Object>> keys) {
        return new ReadQueryDataImpl(keys,
                                         this.whereClauses,
                                         this.columnsToFetch,
                                         this.limit,
                                         this.allowFiltering,
                                         this.fetchSize,
                                         this.distinct);  
    }

    @Override
    public ReadQueryDataImpl whereConditions(ImmutableSet<Clause> whereClauses) {
        return new ReadQueryDataImpl(this.keys,
                                         whereClauses,
                                         this.columnsToFetch,
                                         this.limit,
                                         this.allowFiltering,
                                         this.fetchSize,
                                         this.distinct);  
    }

    @Override
    public ReadQueryDataImpl columnsToFetch(ImmutableMap<String, Boolean> columnsToFetch) {
        return new ReadQueryDataImpl(this.keys,
                                         this.whereClauses,
                                         columnsToFetch,
                                         this.limit,
                                         this.allowFiltering,
                                         this.fetchSize,
                                         this.distinct);  
    }

    @Override
    public ReadQueryDataImpl limit(Integer limit) {
        return new ReadQueryDataImpl(this.keys,
                                         this.whereClauses,
                                         this.columnsToFetch,
                                         limit,
                                         this.allowFiltering,
                                         this.fetchSize,
                                         this.distinct);  
    }

    @Override
    public ReadQueryDataImpl allowFiltering(Boolean allowFiltering) {
        return new ReadQueryDataImpl(this.keys,
                                         this.whereClauses,
                                         this.columnsToFetch,
                                         this.limit,
                                         allowFiltering,
                                         this.fetchSize,
                                         this.distinct);  
    }

    @Override
    public ReadQueryDataImpl fetchSize(Integer fetchSize) {
        return new ReadQueryDataImpl(this.keys,
                                         this.whereClauses,
                                         this.columnsToFetch,
                                         this.limit,
                                         this.allowFiltering,
                                         fetchSize,
                                         this.distinct);  
    }

    @Override
    public ReadQueryDataImpl distinct(Boolean distinct) {
        return new ReadQueryDataImpl(this.keys,
                                         this.whereClauses,
                                         this.columnsToFetch,
                                         this.limit,
                                         this.allowFiltering,
                                         this.fetchSize,
                                         distinct);  
    }
    
    @Override
    public ImmutableMap<String, ImmutableList<Object>> getKeys() {
        return keys;
    }
    
    @Override
    public ImmutableSet<Clause> getWhereConditions() {
        return whereClauses;
    }

    @Override
    public ImmutableMap<String, Boolean> getColumnsToFetch() {
        return columnsToFetch;
    }

    @Override
    public Integer getLimit() {
        return limit;
    }

    @Override
    public Boolean getAllowFiltering() {
        return allowFiltering;
    }

    @Override
    public Integer getFetchSize() {
        return fetchSize;
    }

    @Override
    public Boolean getDistinct() {
        return distinct;
    }
    
     
    /**
     * @param data   the query data
     * @param ctx    the context
     * @return  the query as statement
     */
    static ListenableFuture<Statement> toStatementAsync(ReadQueryData data, Context ctx) {
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

            // key-based selection    
            if (!data.getKeys().isEmpty()) {
                // add key columns to requested columns (for paranoia checks)
                for (String keyname : data.getKeys().keySet()) {
                    if (data.getColumnsToFetch().get(keyname) == null) {
                        selection.column(keyname);
                    }
                }
            }
        }
        
        Select select = selection.from(ctx.getDbSession().getTablename());
  
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
            for (Clause whereClause : data.getWhereConditions()) {
                select.where(whereClause);
            }
            
            return Futures.<Statement>immediateFuture(select);

            
        // key-based selection    
        } else {
            List<Object> values = Lists.newArrayList();
            
            for (Entry<String, ImmutableList<Object>> entry : data.getKeys().entrySet()) {
                if (entry.getValue().size() == 1) {
                    select.where(eq(entry.getKey(), bindMarker()));
                    values.add(ctx.toStatementValue(entry.getKey(), entry.getValue().get(0)));
                } else {
                    select.where(in(entry.getKey(), bindMarker()));
                    values.add(ctx.toStatementValues(entry.getKey(), entry.getValue()));
                }
                
            }
            

            ListenableFuture<PreparedStatement> preparedStatementFuture = ctx.getDbSession().prepare(select);
            return ctx.getDbSession().bind(preparedStatementFuture, values.toArray());
        }
    }   
}
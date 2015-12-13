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
import static com.datastax.driver.core.querybuilder.QueryBuilder.in;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;

import java.util.List;
import java.util.Map.Entry;

import net.oneandone.troilus.java7.interceptor.ReadQueryData;

import com.datastax.driver.core.PagingState;
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
 * 
 * @author Jason Westra - edited original
 * 12-13-2015: pagination APIs - pagingState(), getPagingState() 
 * 			   Explicit comment about NOT setting PagingState in toStatementAsync() 
 * 			   because of issues with DataStax driver hash() check
 */
class ReadQueryDataImpl implements ReadQueryData {
    
    private final Tablename tablename;
    private final ImmutableMap<String, ImmutableList<Object>> keys;
    private final ImmutableSet<Clause> whereClauses;
    private final ImmutableMap<String, Boolean> columnsToFetch;
    private final Integer limit;
    private final Boolean allowFiltering;
    private final Integer fetchSize;
    private final Boolean distinct;
    private final PagingState pagingState;
    
    /**
     * Constructor
     */
    ReadQueryDataImpl(Tablename tablename) {
        this(tablename,
             ImmutableMap.<String, ImmutableList<Object>>of(),
             ImmutableSet.<Clause>of(), 
             ImmutableMap.<String, Boolean>of(),
             null,
             null,
             null,
             null,
             null);
    }

    
    private ReadQueryDataImpl(Tablename tablename,
                              ImmutableMap<String, ImmutableList<Object>> keys,
                              ImmutableSet<Clause> whereClauses, 
                              ImmutableMap<String, Boolean> columnsToFetch, 
                              Integer limit, 
                              Boolean allowFiltering,
                              Integer fetchSize,
                              Boolean distinct,
                              PagingState pagingState) {
        this.tablename = tablename;
        this.keys = keys;
        this.whereClauses = whereClauses;
        this.columnsToFetch = columnsToFetch;
        this.limit = limit;
        this.allowFiltering = allowFiltering;
        this.fetchSize = fetchSize;
        this.distinct = distinct;
        this.pagingState = pagingState;
    }
    

    @Override
    public ReadQueryDataImpl keys(ImmutableMap<String, ImmutableList<Object>> keys) {
        return new ReadQueryDataImpl(this.tablename,
                                     keys,
                                     this.whereClauses,
                                     this.columnsToFetch,
                                     this.limit,
                                     this.allowFiltering,
                                     this.fetchSize,
                                     this.distinct,
                                     this.pagingState); 
    }

    @Override
    public ReadQueryDataImpl whereConditions(ImmutableSet<Clause> whereClauses) {
        return new ReadQueryDataImpl(this.tablename,
                                     this.keys,
                                     whereClauses,
                                     this.columnsToFetch,
                                     this.limit,
                                     this.allowFiltering,
                                     this.fetchSize,
                                     this.distinct,
                                     this.pagingState);  
    }

    @Override
    public ReadQueryDataImpl columnsToFetch(ImmutableMap<String, Boolean> columnsToFetch) {
        return new ReadQueryDataImpl(this.tablename,
                                     this.keys,
                                     this.whereClauses,
                                     columnsToFetch,
                                     this.limit,
                                     this.allowFiltering,
                                     this.fetchSize,
                                     this.distinct,
                                     this.pagingState);  
    }

    @Override
    public ReadQueryDataImpl limit(Integer limit) {
        return new ReadQueryDataImpl(this.tablename,
                                     this.keys,
                                     this.whereClauses,
                                     this.columnsToFetch,
                                     limit,
                                     this.allowFiltering,
                                     this.fetchSize,
                                     this.distinct,
                                     this.pagingState);  
    }

    @Override
    public ReadQueryDataImpl allowFiltering(Boolean allowFiltering) {
        return new ReadQueryDataImpl(this.tablename,
                                     this.keys,
                                     this.whereClauses,
                                     this.columnsToFetch,
                                     this.limit,
                                     allowFiltering,
                                     this.fetchSize,
                                     this.distinct,
                                     this.pagingState);  
    }

    @Override
    public ReadQueryDataImpl fetchSize(Integer fetchSize) {
        return new ReadQueryDataImpl(this.tablename,
                                     this.keys,
                                     this.whereClauses,
                                     this.columnsToFetch,
                                     this.limit,
                                     this.allowFiltering,
                                     fetchSize,
                                     this.distinct,
                                     this.pagingState); 
    }

    @Override
    public ReadQueryDataImpl distinct(Boolean distinct) {
        return new ReadQueryDataImpl(this.tablename,
                                     this.keys,
                                     this.whereClauses,
                                     this.columnsToFetch,
                                     this.limit,
                                     this.allowFiltering,
                                     this.fetchSize,
                                     distinct,
                                     this.pagingState);  
    }
    
    @Override
    public ReadQueryDataImpl pagingState(PagingState pagingState) {
        return new ReadQueryDataImpl(this.tablename,
                                     this.keys,
                                     this.whereClauses,
                                     this.columnsToFetch,
                                     this.limit,
                                     this.allowFiltering,
                                     this.fetchSize,
                                     this.distinct,
                                     pagingState);  
    }
    
    @Override
    public Tablename getTablename() {
        return tablename;
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
    static ListenableFuture<Statement> toStatementAsync(ReadQueryData data, UDTValueMapper udtValueMapper, DBSession dbSession) {
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
        
        Select select = (data.getTablename().getKeyspacename() == null) ? selection.from(data.getTablename().getTablename())
                                                                        : selection.from(data.getTablename().getKeyspacename(), data.getTablename().getTablename());
  
        if (data.getLimit() != null) {
            select.limit(data.getLimit());
        }

        if ((data.getAllowFiltering() != null) && data.getAllowFiltering()) {
            select.allowFiltering();
        } 

        if (data.getFetchSize() != null) {
            select.setFetchSize(data.getFetchSize());
        }
        
        // begin: jwestra: 12-10-2015 - Pagination
        // NOTE:
        // This results in a PagingStateException because a Select is a RegularStatement
        // while the ResultSet.getPagingState() has the final BoundStatement in it.
        // So, their hash() is different and the driver throws the PagingStateException
        // @see PagingState.hash()
    	//if (data.getPagingState() != null) {
    	//	select.setPagingState(data.getPagingState());
    	//}
    	// end: changes for Pagination
        
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
                    values.add(udtValueMapper.toStatementValue(data.getTablename(), entry.getKey(), entry.getValue().get(0)));
                } else {
                    select.where(in(entry.getKey(), bindMarker()));
                    values.add(udtValueMapper.toStatementValues(data.getTablename(), entry.getKey(), entry.getValue()));
                }
                
            }
            

            ListenableFuture<PreparedStatement> preparedStatementFuture = dbSession.prepareAsync(select);
            return dbSession.bindAsync(preparedStatementFuture, values.toArray());
        }
    }   
    
    @Override
	public PagingState getPagingState() {
		return pagingState;
	}   
}
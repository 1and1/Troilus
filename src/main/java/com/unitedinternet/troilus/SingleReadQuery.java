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

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Select.Selection;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.unitedinternet.troilus.Dao.SingleRead;
import com.unitedinternet.troilus.Dao.SingleReadWithColumns;
import com.unitedinternet.troilus.Dao.SingleReadWithUnit;



 

public class SingleReadQuery extends AbstractQuery<SingleReadQuery> implements SingleReadWithUnit<Optional<Record>> {
    private static final Logger LOG = LoggerFactory.getLogger(SingleReadQuery.class);

    final ImmutableMap<String, Object> keyNameValuePairs;
    final Optional<ImmutableMap<String, Boolean>> optionalColumnsToFetch;
     
    
    public SingleReadQuery(Context ctx, QueryFactory queryFactory, ImmutableMap<String, Object> keyNameValuePairs, Optional<ImmutableMap<String, Boolean>> optionalColumnsToFetch) {
        super(ctx, queryFactory);
        this.keyNameValuePairs = keyNameValuePairs;
        this.optionalColumnsToFetch = optionalColumnsToFetch;
    }
   
    
    @Override
    protected SingleReadQuery newQuery(Context newContext) {
        return newSingleReadQuery(newContext, keyNameValuePairs, optionalColumnsToFetch);
    }
    
    @Override
    public SingleRead<Optional<Record>> all() {
        return newSingleReadQuery(keyNameValuePairs, Optional.empty());
    }
    
    @Override
    public <E> SingleEntityReadQuery<E> asEntity(Class<E> objectClass) {
        return new SingleEntityReadQuery(getContext(), getQueryFactory(), this, objectClass);
    }
    
    @Override
    public SingleReadQuery column(String name) {
        return newSingleReadQuery(keyNameValuePairs, Immutables.merge(optionalColumnsToFetch, name, false));
    }

    @Override
    public SingleReadQuery columnWithMetadata(String name) {
        return newSingleReadQuery(keyNameValuePairs, Immutables.merge(optionalColumnsToFetch, name, true));
    }
    
    @Override
    public SingleReadQuery columns(String... names) {
        return columns(ImmutableSet.copyOf(names));
    }
    
    @Override 
    public SingleReadQuery columns(ImmutableCollection<String> namesToRead) {
        SingleReadQuery read = this;
        for (String columnName : namesToRead) {
            read = read.column(columnName);
        }
        return read;
    }
  

    
    
    @Override
    public CompletableFuture<Optional<Record>> executeAsync() {
        
        Selection selection = select();
        
        if (optionalColumnsToFetch.isPresent()) {
            optionalColumnsToFetch.get().forEach((columnName, withMetaData) -> {
                                                                                    selection.column(columnName);
                                                                                    if (withMetaData) {
                                                                                        selection.ttl(columnName);                                                                
                                                                                        selection.writeTime(columnName);
                                                                                    }
                                                                               });

            // add key columns for paranoia checks
            keyNameValuePairs.keySet().forEach(columnName -> { if(!optionalColumnsToFetch.get().containsKey(columnName)) selection.column(columnName); });  
            
        } else {
            selection.all();
        }
        
        
        
        Select select = selection.from(getTable());
        Select.Where where = null;
        for (Clause whereClause : keyNameValuePairs.keySet().stream().map(name -> eq(name, bindMarker())).collect(Immutables.toSet())) {
            if (where == null) {
                where = select.where(whereClause);
            } else {
                where = where.and(whereClause);
            }
        }

        Statement statement = prepare(select).bind(keyNameValuePairs.values().toArray());
        
        
        return performAsync(statement)
                  .thenApply(resultSet -> {
                                              Row row = resultSet.one();
                                              if (row == null) {
                                                  return Optional.empty();
                                                  
                                              } else {
                                                  Record record = newRecord(Result.newResult(resultSet), row);
                                                  
                                                  // paranoia check
                                                  keyNameValuePairs.forEach((name, value) -> { 
                                                                                              ByteBuffer in = DataType.serializeValue(value, getProtocolVersion());
                                                                                              ByteBuffer out = record.getBytesUnsafe(name).get();
                                                      
                                                                                              if (in.compareTo(out) != 0) {
                                                                                                   LOG.warn("Dataswap error for " + name);
                                                                                                   throw new ProtocolErrorException("Dataswap error for " + name); 
                                                                                              }
                                                                                             });
                                                  
                                                  if (!resultSet.isExhausted()) {
                                                      throw new TooManyResultsException("more than one record exists");
                                                  }
                                                  
                                                  return Optional.of(record); 
                                              }
                  });
    }
    
    
    
    
    
    
    
    private static class SingleEntityReadQuery<E> extends AbstractQuery<SingleEntityReadQuery<E>> implements SingleRead<Optional<E>> {
        private final Class<E> clazz;
        private final SingleReadQuery read;
        
        public SingleEntityReadQuery(Context ctx, QueryFactory queryFactory, SingleReadQuery read, Class<E> clazz) {
            super(ctx, queryFactory);
            this.read = read;
            this.clazz = clazz;
        }
        

        @Override
        protected SingleEntityReadQuery<E> newQuery(Context newContext) {
            return newSingleReadQuery(newContext, read.keyNameValuePairs, read.optionalColumnsToFetch).asEntity(clazz); 
        }
        
        
        @Override
        public CompletableFuture<Optional<E>> executeAsync() {
            return read.executeAsync().thenApply(optionalRecord -> optionalRecord.map(record -> fromValues(clazz, record.getAccessor())));
        }        
    }
}

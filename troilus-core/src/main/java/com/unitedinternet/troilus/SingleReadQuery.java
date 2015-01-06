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

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableSet;
import com.unitedinternet.troilus.Dao.SingleRead;
import com.unitedinternet.troilus.Dao.SingleReadWithUnit;



 

class SingleReadQuery extends AbstractQuery<SingleReadQuery> implements SingleReadWithUnit<Optional<Record>> {
    private static final Logger LOG = LoggerFactory.getLogger(SingleReadQuery.class);

    final SingleReadQueryData data;
     
    
    public SingleReadQuery(Context ctx, SingleReadQueryData data) {
        super(ctx);
        this.data = data;
    }
   
    
    @Override
    protected SingleReadQuery newQuery(Context newContext) {
        return new SingleReadQuery(newContext, data);
    }
    
    @Override
    public SingleRead<Optional<Record>> all() {
        return new SingleReadQuery(getContext(), data.withColumnsToFetchKeys(Optional.empty()));
    }
    
    @Override
    public <E> SingleEntityReadQuery<E> asEntity(Class<E> objectClass) {
        return new SingleEntityReadQuery<>(getContext(), this, objectClass);
    }
    
    @Override
    public SingleReadQuery column(String name) {
        return new SingleReadQuery(getContext(), data.withColumnsToFetchKeys(Immutables.merge(data.getColumnsToFetch(), name, false)));
    }

    @Override
    public SingleReadQuery columnWithMetadata(String name) {
        return new SingleReadQuery(getContext(), data.withColumnsToFetchKeys(Immutables.merge(data.getColumnsToFetch(), name, true)));
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
        Statement statement = data.toStatement(getContext());
        
        return performAsync(statement)
                  .thenApply(resultSet -> {
                                              Row row = resultSet.one();
                                              if (row == null) {
                                                  return Optional.empty();
                                                  
                                              } else {
                                                  Record record = newRecord(new ResultImpl(resultSet), row);
                                                  
                                                  // paranoia check
                                                  data.getKeyNameValuePairs().forEach((name, value) -> { 
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
        
        public SingleEntityReadQuery(Context ctx, SingleReadQuery read, Class<E> clazz) {
            super(ctx);
            this.read = read;
            this.clazz = clazz;
        }
        

        @Override
        protected SingleEntityReadQuery<E> newQuery(Context newContext) {
            return new SingleReadQuery(newContext, read.data).asEntity(clazz); 
        }
        
        
        @Override
        public CompletableFuture<Optional<E>> executeAsync() {
            return read.executeAsync().thenApply(optionalRecord -> optionalRecord.map(record -> fromValues(clazz, record.getAccessor())));
        }        
    }
}

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
import java.util.List;




import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.unitedinternet.troilus.interceptor.SingleReadQueryData;
import com.unitedinternet.troilus.interceptor.SingleReadQueryPreInterceptor;
import com.unitedinternet.troilus.minimal.MinimalDao.SingleRead;
import com.unitedinternet.troilus.minimal.MinimalDao.SingleReadWithUnit;
import com.unitedinternet.troilus.minimal.Record;
import com.unitedinternet.troilus.minimal.SingleReadQueryPostInterceptor;



 

class SingleReadQuery extends AbstractQuery<SingleReadQuery> implements SingleReadWithUnit<Record> {
    
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
    public SingleReadQuery all() {
        return new SingleReadQuery(getContext(), data.columnsToFetch(ImmutableMap.<String, Boolean>of()));
    }
    
   
    @Override
    public SingleReadQuery column(String name) {
        return new SingleReadQuery(getContext(), data.columnsToFetch(Immutables.merge(data.getColumnsToFetch(), name, false)));
    }

    @Override
    public SingleReadQuery columnWithMetadata(String name) {
        return new SingleReadQuery(getContext(), data.columnsToFetch(Immutables.merge(data.getColumnsToFetch(), name, true)));
    }
    
    @Override
    public SingleReadQuery columns(String... names) {
        return columns(ImmutableList.copyOf(names));
    }

    
    private SingleReadQuery columns(ImmutableList<String> names) {
        SingleReadQuery read = this;
        for (String columnName : names) {
            read = read.column(columnName);
        }
        return read;
    }

    
    @Override
    public SingleReadQuery column(Name<?> name) {
        return column(name.getName());
    }

    @Override
    public SingleReadQuery columnWithMetadata(Name<?> name) {
        return column(name.getName());
    }
    
    
    @Override
    public SingleReadQuery columns(Name<?>... names) {
        List<String> ns = Lists.newArrayList();
        for (Name<?> name : names) {
            ns.add(name.getName());
        }
        return columns(ImmutableList.copyOf(ns));
    }
    
    
    @Override
    public <E> MinimalSingleEntityReadQuery<E> asEntity(Class<E> objectClass) {
        return new MinimalSingleEntityReadQuery<E>(getContext(), this, objectClass);
    }
    

    
    
    private SingleReadQueryData getPreprocessedData(Context ctx) {
        SingleReadQueryData queryData = data;
        for (SingleReadQueryPreInterceptor interceptor : ctx.getInterceptorRegistry().getInterceptors(SingleReadQueryPreInterceptor.class)) {
            queryData = interceptor.onPreSingleRead(queryData);
        }
        
        return queryData;
    }
    
    
    
    @Override
    public Record execute() {
        return getUninterruptibly(executeAsync());
    }
    
    
    @Override
    public ListenableFuture<Record> executeAsync() {
        final SingleReadQueryData preprocessedData = getPreprocessedData(getContext()); 
        Statement statement = SingleReadQueryDataImpl.toStatement(preprocessedData, getContext());
        
        ResultSetFuture future = performAsync(statement);
        
        
        Function<ResultSet, Record> mapEntity = new Function<ResultSet, Record>() {
            
            @Override
            public Record apply(ResultSet resultSet) {
                Row row = resultSet.one();
                
                if (row == null) {
                    return null; 
                } else {
                    if (!resultSet.isExhausted()) {
                        throw new TooManyResultsException("more than one record exists");
                    }
                    Record record = new RecordImpl(getContext(), newResult(resultSet), row);
                    paranoiaCheck(record);
                    return processPostInterceptor(preprocessedData, record);
                }
            }
        };
        
        return Futures.transform(future, mapEntity);
    }
    
    
    private Record paranoiaCheck(Record record) {
        
        for (Entry<String, Object> entry : data.getKeyParts().entrySet()) {
            ByteBuffer in = DataType.serializeValue(entry.getValue(), getContext().getProtocolVersion());
            ByteBuffer out = record.getBytesUnsafe(entry.getKey());

            if (in.compareTo(out) != 0) {
                 LOG.warn("Dataswap error for " + entry.getKey());
                 throw new ProtocolErrorException("Dataswap error for " + entry.getKey()); 
            }
        }
        
        return record; 
    }
    
    private Record processPostInterceptor(SingleReadQueryData preprocessedData, Record record) {
        for (SingleReadQueryPostInterceptor interceptor : getContext().getInterceptorRegistry().getInterceptors(SingleReadQueryPostInterceptor.class)) {
            record = interceptor.onPostSingleRead(preprocessedData, record);
        }
        
        return record;
    }
    
    
    
    static class MinimalSingleEntityReadQuery<E> extends AbstractQuery<MinimalSingleEntityReadQuery<E>> implements SingleRead<E> {
        private final Class<E> clazz;
        private final SingleReadQuery read;
        
        public MinimalSingleEntityReadQuery(Context ctx, SingleReadQuery read, Class<E> clazz) {
            super(ctx);
            this.read = read;
            this.clazz = clazz;
        }
        
        @Override
        protected MinimalSingleEntityReadQuery<E> newQuery(Context newContext) {
            return new SingleReadQuery(newContext, read.data).<E>asEntity(clazz); 
        }
        
        @Override
        public E execute() {
            return getUninterruptibly(executeAsync());
        }
        
        @Override
        public ListenableFuture<E> executeAsync() {
            ListenableFuture<Record> future = read.executeAsync();
            
            Function<Record, E> mapEntity = new Function<Record, E>() {
                @Override
                public E apply(Record record) {
                    return getContext().getBeanMapper().fromValues(clazz, RecordImpl.toPropertiesSource(record));
                }
            };
            
            return Futures.transform(future, mapEntity);
        }
    }
}

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
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.unitedinternet.troilus.interceptor.SingleReadQueryData;
import com.unitedinternet.troilus.minimal.MinimalDao.SingleRead;
import com.unitedinternet.troilus.minimal.MinimalDao.SingleReadWithColumns;
import com.unitedinternet.troilus.minimal.MinimalDao.SingleReadWithUnit;
import com.unitedinternet.troilus.minimal.Record;
import com.unitedinternet.troilus.minimal.SingleReadQueryPostInterceptor;
import com.unitedinternet.troilus.minimal.SingleReadQueryPreInterceptor;



 

class MinimalSingleReadQuery extends AbstractQuery<MinimalSingleReadQuery> implements SingleReadWithUnit<Record> {
    
    private static final Logger LOG = LoggerFactory.getLogger(MinimalSingleReadQuery.class);

    
    final SingleReadQueryData data;
    
    public MinimalSingleReadQuery(Context ctx, SingleReadQueryData data) {
        super(ctx);
        this.data = data;
    }
   
    
    @Override
    protected MinimalSingleReadQuery newQuery(Context newContext) {
        return new MinimalSingleReadQuery(newContext, data);
    }
    
    @Override
    public SingleRead<Record> all() {
        return new MinimalSingleReadQuery(getContext(), data.columnsToFetch(ImmutableMap.<String, Boolean>of()));
    }
    
   
    @Override
    public MinimalSingleReadQuery column(String name) {
        return new MinimalSingleReadQuery(getContext(), data.columnsToFetch(Immutables.merge(data.getColumnsToFetch(), name, false)));
    }

    @Override
    public MinimalSingleReadQuery columnWithMetadata(String name) {
        return new MinimalSingleReadQuery(getContext(), data.columnsToFetch(Immutables.merge(data.getColumnsToFetch(), name, true)));
    }
    
    @Override
    public MinimalSingleReadQuery columns(String... names) {
        return columns(ImmutableList.copyOf(names));
    }

    
    private MinimalSingleReadQuery columns(ImmutableList<String> names) {
        MinimalSingleReadQuery read = this;
        for (String columnName : names) {
            read = read.column(columnName);
        }
        return read;
    }

    
    @Override
    public SingleReadWithColumns<Record> column(Name<?> name) {
        return column(name.getName());
    }

    @Override
    public SingleReadWithColumns<Record> columnWithMetadata(Name<?> name) {
        return column(name.getName());
    }
    
    
    @Override
    public SingleReadWithColumns<Record> columns(Name<?>... names) {
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
        SingleReadQueryData preprocessedData = getPreprocessedData(getContext()); 
        Statement statement = SingleReadQueryDataImpl.toStatement(preprocessedData, getContext());

        ResultSet rs = performAsync(statement).getUninterruptibly();
        Row row = rs.one();
        
        if (row == null) {
            return null; 
            
        } else {
            if (!rs.isExhausted()) {
                throw new TooManyResultsException("more than one record exists");
            }
            Record record = new RecordImpl(getContext(), newResult(rs), row);
            paranoiaCheck(record);
            return processPostInterceptor(preprocessedData, record);
        }
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
    
    
    
    private static class MinimalSingleEntityReadQuery<E> extends AbstractQuery<MinimalSingleEntityReadQuery<E>> implements SingleRead<E> {
        private final Class<E> clazz;
        private final MinimalSingleReadQuery read;
        
        public MinimalSingleEntityReadQuery(Context ctx, MinimalSingleReadQuery read, Class<E> clazz) {
            super(ctx);
            this.read = read;
            this.clazz = clazz;
        }
        

        @Override
        protected MinimalSingleEntityReadQuery<E> newQuery(Context newContext) {
            return new MinimalSingleReadQuery(newContext, read.data).<E>asEntity(clazz); 
        }
        
        
        @Override
        public E execute() {
            Record record = read.execute();
            if (record == null) {
                return null;
            } else {
                return  getContext().getBeanMapper().fromValues(clazz, RecordImpl.toPropertiesSource(record));
            }
        }
    }
}

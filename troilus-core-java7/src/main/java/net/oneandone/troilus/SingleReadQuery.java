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
package net.oneandone.troilus;

import java.nio.ByteBuffer;
import java.util.List;




import java.util.Map.Entry;

import net.oneandone.troilus.interceptor.SingleReadQueryData;
import net.oneandone.troilus.java7.Record;
import net.oneandone.troilus.java7.Dao.SingleRead;
import net.oneandone.troilus.java7.Dao.SingleReadWithUnit;
import net.oneandone.troilus.java7.interceptor.SingleReadQueryRequestInterceptor;
import net.oneandone.troilus.java7.interceptor.SingleReadQueryResponseInterceptor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;


/**
 * Read query implementation
 */
class SingleReadQuery extends AbstractQuery<SingleReadQuery> implements SingleReadWithUnit<Record> {
    
    private static final Logger LOG = LoggerFactory.getLogger(SingleReadQuery.class);
    private final SingleReadQueryData data;
    
    /**
     * @param ctx   the context 
     * @param data  the data
     */
    SingleReadQuery(Context ctx, SingleReadQueryData data) {
        super(ctx);
        this.data = data;
    }
   
    
    @Override
    protected SingleReadQuery newQuery(Context newContext) {
        return new SingleReadQuery(newContext, data);
    }
    
    @Override
    public SingleReadQuery all() {
        return new SingleReadQuery(getContext(), 
                                   data.columnsToFetch(ImmutableMap.<String, Boolean>of()));
    }
    
   
    @Override
    public SingleReadQuery column(String name) {
        return new SingleReadQuery(getContext(), 
                                   data.columnsToFetch(Immutables.merge(data.getColumnsToFetch(), name, false)));
    }

    @Override
    public SingleReadQuery columnWithMetadata(String name) {
        return new SingleReadQuery(getContext(), 
                                   data.columnsToFetch(Immutables.merge(data.getColumnsToFetch(), name, true)));
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
    public <E> SingleEntityReadQuery<E> asEntity(Class<E> objectClass) {
        return new SingleEntityReadQuery<E>(getContext(), this, objectClass);
    }
    
    @Override
    public Record execute() {
        return ListenableFutures.getUninterruptibly(executeAsync());
    }
    
    @Override
    public ListenableFuture<Record> executeAsync() {
        // perform request executors
        ListenableFuture<SingleReadQueryData> queryDataFuture = executeRequestInterceptorsAsync(Futures.<SingleReadQueryData>immediateFuture(data)); 
        
        // execute query asnyc
        Function<SingleReadQueryData, ListenableFuture<Record>> queryExecutor = new Function<SingleReadQueryData, ListenableFuture<Record>>() {
            @Override
            public ListenableFuture<Record> apply(SingleReadQueryData querData) {
                return executeAsync(querData);
            }
        };
        return ListenableFutures.transform(queryDataFuture, queryExecutor, MoreExecutors.directExecutor());
    }
    
    private ListenableFuture<Record> executeAsync(SingleReadQueryData queryData) {
        // perform query
        ListenableFuture<ResultSet> resultSetFuture = performAsync(SingleReadQueryDataImpl.toStatement(queryData, getContext()));        
        
        // result set to record mapper
        Function<ResultSet, Record> resultSetToRecord = new Function<ResultSet, Record>() {
            
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
                    return record;
                }
            }
        };
        ListenableFuture<Record> recordFuture = Futures.transform(resultSetFuture, resultSetToRecord);
        
        // perform response interceptor
        return executeResponseInterceptorsAsync(queryData, recordFuture);
    }

    
    private ListenableFuture<SingleReadQueryData> executeRequestInterceptorsAsync(ListenableFuture<SingleReadQueryData> queryDataFuture) {
        for (SingleReadQueryRequestInterceptor interceptor : getContext().getInterceptorRegistry().getInterceptors(SingleReadQueryRequestInterceptor.class).reverse()) {
            final SingleReadQueryRequestInterceptor icptor = interceptor;
            
            Function<SingleReadQueryData, ListenableFuture<SingleReadQueryData>> mapperFunction = new Function<SingleReadQueryData, ListenableFuture<SingleReadQueryData>>() {
                @Override
                public ListenableFuture<SingleReadQueryData> apply(SingleReadQueryData queryData) {
                    return icptor.onSingleReadRequest(queryData);
                }
            };
            
            queryDataFuture = ListenableFutures.transform(queryDataFuture, mapperFunction, getContext().getTaskExecutor());
        }

        return queryDataFuture;
    }

    
    private ListenableFuture<Record> executeResponseInterceptorsAsync(final SingleReadQueryData queryData, ListenableFuture<Record> recordFuture) {
        
        for (SingleReadQueryResponseInterceptor interceptor : getContext().getInterceptorRegistry().getInterceptors(SingleReadQueryResponseInterceptor.class).reverse()) {
            final SingleReadQueryResponseInterceptor icptor = interceptor;
            
            Function<Record, ListenableFuture<Record>> mapperFunction = new Function<Record, ListenableFuture<Record>>() {
                @Override
                public ListenableFuture<Record> apply(Record record) {
                    return icptor.onSingleReadResponse(queryData, record);
                }
            };
            
            recordFuture = ListenableFutures.transform(recordFuture, mapperFunction, getContext().getTaskExecutor());
        }

        return recordFuture;
    }

    
    
    private Record paranoiaCheck(Record record) {
        
        for (Entry<String, Object> entry : data.getKey().entrySet()) {
            ByteBuffer in = DataType.serializeValue(entry.getValue(), getContext().getProtocolVersion());
            ByteBuffer out = record.getBytesUnsafe(entry.getKey());

            if (in.compareTo(out) != 0) {
                 LOG.warn("Dataswap error for " + entry.getKey());
                 throw new ProtocolErrorException("Dataswap error for " + entry.getKey()); 
            }
        }
        
        return record; 
    }
    
    
    
    /**
     * Entity read query 
     * @param <E> the entity type
     */
    static class SingleEntityReadQuery<E> extends AbstractQuery<SingleEntityReadQuery<E>> implements SingleRead<E> {
        private final Class<E> clazz;
        private final SingleReadQuery query;
        
        
        /**
         * @param ctx    the context
         * @param query  the underlying query  
         * @param clazz  the entity type
         */
        SingleEntityReadQuery(Context ctx, SingleReadQuery query, Class<E> clazz) {
            super(ctx);
            this.query = query;
            this.clazz = clazz;
        }
        
        @Override
        protected SingleEntityReadQuery<E> newQuery(Context newContext) {
            return new SingleReadQuery(newContext, query.data).<E>asEntity(clazz); 
        }
        
        @Override
        public E execute() {
            return ListenableFutures.getUninterruptibly(executeAsync());
        }
        
        @Override
        public ListenableFuture<E> executeAsync() {
            ListenableFuture<Record> future = query.executeAsync();
            
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
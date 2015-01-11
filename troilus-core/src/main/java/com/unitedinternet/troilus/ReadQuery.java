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


import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.UDTValue;
import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;



 
/**
 * REad query base class
 * 
 * @param <Q> the concrete query 
 */
abstract class ReadQuery<Q> extends AbstractQuery<Q>  {
    
    
    /**
     * @param ctx the context 
     */
    ReadQuery(Context ctx) {
        super(ctx);
    }
    
    
    
    protected Record newRecord(Result result, Row row) {
        return new RecordImpl(result, row);
    }
    
    protected RecordList newRecordList(ResultSet resultSet) {
        return new RecordListImpl(resultSet);
    }
    
    protected <E> EntityList<E> newEntityList(RecordList recordList, Class<E> clazz) {
        return EntityList.newEntityList(getContext(), recordList, clazz); 
    }
    

  
    
    private final class RecordImpl extends Record {
        
        public RecordImpl(Result result, Row row) {
            super(result, row);
        }
  
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public <T> Optional<T> getObject(String name, Class<T> elementsClass) {
            if (isNull(name)) {
                return Optional.empty();
            }

            DataType datatype = getColumnDefinitions().getType(name);
            
            if (datatype != null) {
                
                // build-in
                if (UDTValueMapper.isBuildInType(datatype)) {
                    Optional<T> optionalObject = (Optional<T>) getBytesUnsafe(name).map(bytes -> datatype.deserialize(bytes, getProtocolVersion())); getBytesUnsafe(name).map(bytes -> datatype.deserialize(bytes, getProtocolVersion()));
                    
                    // enum
                    if (optionalObject.isPresent() && getContext().isTextDataType(datatype) && Enum.class.isAssignableFrom(elementsClass)) {
                        String obj = (String) optionalObject.get();
                        optionalObject = (Optional<T>) Optional.of(Enum.valueOf((Class<Enum>) elementsClass, obj));
                    }
                    
                    return optionalObject;
                 
                // udt
                } else {
                    return Optional.ofNullable(getContext().getUDTValueMapper().fromUdtValue(datatype, getUDTValue(name).get(), elementsClass));
                }
            }
            
            return Optional.empty();
        }
        
        
        public <T> Optional<ImmutableSet<T>> getSet(String name, Class<T> elementsClass) {
            if (isNull(name)) {
                return Optional.empty();
            }

            DataType datatype = getContext().getColumnMetadata(name).getType();
            if (UDTValueMapper.isBuildInType(datatype)) {
                return Optional.of(getRow().getSet(name, elementsClass)).map(set -> ImmutableSet.copyOf(set));
            } else {
                return Optional.of(getRow().getSet(name, UDTValue.class)).map(udtValues -> (ImmutableSet<T>) getContext().getUDTValueMapper().fromUdtValues(datatype.getTypeArguments().get(0), ImmutableSet.copyOf(udtValues), elementsClass));
            }
        }
        
     
        public <T> Optional<ImmutableList<T>> getList(String name, Class<T> elementsClass) {
            if (isNull(name)) {
                return Optional.empty();
            }
            
            DataType datatype = getContext().getColumnMetadata(name).getType();
            if (UDTValueMapper.isBuildInType(datatype)) {
                return Optional.ofNullable(getRow().getList(name, elementsClass)).map(list -> ImmutableList.copyOf(list));
            } else {
                return Optional.of(getRow().getList(name, UDTValue.class)).map(udtValues -> (ImmutableList<T>) getContext().getUDTValueMapper().fromUdtValues(datatype.getTypeArguments().get(0), ImmutableList.copyOf(udtValues), elementsClass));
            }
        }
        
        
        public <K, V> Optional<ImmutableMap<K, V>> getMap(String name, Class<K> keysClass, Class<V> valuesClass) {
            if (isNull(name)) {
                return Optional.empty();
            }
            
            DataType datatype = getContext().getColumnMetadata(name).getType();
            if (UDTValueMapper.isBuildInType(datatype)) {
                return Optional.ofNullable(getRow().getMap(name, keysClass, valuesClass)).map(map -> ImmutableMap.copyOf(map));
                
            } else {
                if (UDTValueMapper.isBuildInType(datatype.getTypeArguments().get(0))) {
                    return Optional.of(getRow().getMap(name, keysClass, UDTValue.class)).map(udtValues -> (ImmutableMap<K, V>) getContext().getUDTValueMapper().fromUdtValues(datatype.getTypeArguments().get(0), datatype.getTypeArguments().get(1), ImmutableMap.copyOf(udtValues), keysClass, valuesClass));

                } else if (UDTValueMapper.isBuildInType(datatype.getTypeArguments().get(1))) {
                    return Optional.of(getRow().getMap(name, UDTValue.class, valuesClass)).map(udtValues -> (ImmutableMap<K, V>) getContext().getUDTValueMapper().fromUdtValues(datatype.getTypeArguments().get(0), datatype.getTypeArguments().get(1), ImmutableMap.copyOf(udtValues), keysClass, valuesClass));
                    
                } else {
                    return isNull(name) ? Optional.empty() : Optional.of(getRow().getMap(name, UDTValue.class, UDTValue.class)).map(udtValues -> (ImmutableMap<K, V>) getContext().getUDTValueMapper().fromUdtValues(datatype.getTypeArguments().get(0), datatype.getTypeArguments().get(1), ImmutableMap.copyOf(udtValues), keysClass, valuesClass));
                }
            }
        }
        
        
        public String toString() {
            ToStringHelper toStringHelper = MoreObjects.toStringHelper(this);
            getRow().getColumnDefinitions().asList()
                                      .forEach(definition -> toString(definition.getName(), definition.getType()).ifPresent(value -> toStringHelper.add(definition.getName(), value)));
            return toStringHelper.toString();
        }
        
        
        private Optional<String> toString(String name, DataType dataType) {
            if (isNull(name)) {
                return Optional.empty();
            } else {
                StringBuilder builder = new StringBuilder();
                builder.append(dataType.deserialize(getRow().getBytesUnsafe(name), getProtocolVersion()));

                return Optional.of(builder.toString());
            }
        }
    }
    
    
    private final class RecordListImpl extends RecordList {
        private final ResultSet rs;

        private final Iterator<Row> iterator;
        private final AtomicReference<DatabaseSubscription> subscriptionRef = new AtomicReference<>();
        
        public RecordListImpl(ResultSet rs) {
            this.rs = rs;
            this.iterator = rs.iterator();
        }
        
        @Override
        public ExecutionInfo getExecutionInfo() {
            return rs.getExecutionInfo();
        }
        
        @Override
        public ImmutableList<ExecutionInfo> getAllExecutionInfo() {
            return ImmutableList.copyOf(rs.getAllExecutionInfo());
        }

        @Override
        public boolean wasApplied() {
            return rs.wasApplied();
        }
        
        
        @Override
        public Iterator<Record> iterator() {
            
            return new Iterator<Record>() {

                @Override
                public boolean hasNext() {
                    return iterator.hasNext();
                }
                
                @Override
                public Record next() {
                    return new RecordImpl(RecordListImpl.this, iterator.next());
                }
            };
        }
        
        
        @Override
        public void subscribe(Subscriber<? super Record> subscriber) {
            synchronized (subscriptionRef) {
                if (subscriptionRef.get() == null) {
                    DatabaseSubscription subscription = new DatabaseSubscription(subscriber);
                    subscriptionRef.set(subscription);
                    subscriber.onSubscribe(subscription);
                } else {
                    subscriber.onError(new IllegalStateException("subription alreday exists. Multi-subscribe is not supported")); 
                }
            }
        }
        
        
        private final class DatabaseSubscription implements Subscription {
            private final Subscriber<? super Record> subscriber;
            
            private final Iterator<? extends Record> it;
            
            private final AtomicLong numPendingReads = new AtomicLong();
            private final AtomicReference<Runnable> runningDatabaseQuery = new AtomicReference<>();
            
            public DatabaseSubscription(Subscriber<? super Record> subscriber) {
                this.subscriber = subscriber;
                this.it = RecordListImpl.this.iterator();
            }
            
            public void request(long n) {
                if (n > 0) {
                    numPendingReads.addAndGet(n);
                    processReadRequests();
                }
            }
            
            
            private void processReadRequests() {
                synchronized (this) {
                    long available = rs.getAvailableWithoutFetching();
                    long numToRead = numPendingReads.get();

                    // no records available?
                    if (available == 0) {
                        requestDatabaseForMoreRecords();
                      
                    // all requested available 
                    } else if (available >= numToRead) {
                        numPendingReads.addAndGet(-numToRead);
                        for (int i = 0; i < numToRead; i++) {
                            subscriber.onNext(it.next());
                        }                    
                        
                    // requested partly available                        
                    } else {
                        requestDatabaseForMoreRecords();
                        numPendingReads.addAndGet(-available);
                        for (int i = 0; i < available; i++) {
                            subscriber.onNext(it.next());
                        }
                    }
                }
            }
            
            
            private void requestDatabaseForMoreRecords() {
                if (rs.isFullyFetched()) {
                    cancel();
                }
                
                synchronized (this) {
                    if (runningDatabaseQuery.get() == null) {
                        Runnable databaseRequest = () -> { runningDatabaseQuery.set(null); processReadRequests(); };
                        runningDatabaseQuery.set(databaseRequest);
                        
                        ListenableFuture<Void> future = rs.fetchMoreResults();
                        future.addListener(databaseRequest, ForkJoinPool.commonPool());
                    }
                }
            }
       
            
            @Override
            public void cancel() {
                subscriber.onComplete();
            }
        }
    }
}


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


import java.time.Duration;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.policies.RetryPolicy;
import com.datastax.driver.core.querybuilder.BuiltStatement;
import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListenableFuture;
import com.unitedinternet.troilus.utils.Exceptions;
import com.unitedinternet.troilus.utils.Immutables;

 
abstract class AbstractQuery<Q> {
    
    private final Context ctx;
    
    public AbstractQuery(Context ctx) {
        this.ctx = ctx;
    }

    abstract protected Q newQuery(Context newContext);
    

    
    
    ////////////////////////
    // default implementations
  
    public Q withConsistency(ConsistencyLevel consistencyLevel) {
        return newQuery(ctx.withConsistency(consistencyLevel));
    }

    public Q withWritetime(long microsSinceEpoch) {
        return newQuery(ctx.withWritetime(microsSinceEpoch));
    }

    public Q withTtl(Duration ttl) {
        return newQuery(ctx.withTtl(ttl));
    }
    
    public Q withEnableTracking() {
        return newQuery(ctx.withEnableTracking());
    }
    
    public Q withDisableTracking() {
        return newQuery(ctx.withDisableTracking());
    }
    
    public Q withRetryPolicy(RetryPolicy policy) {
        return newQuery(ctx.withRetryPolicy(policy));
    }
    

    protected Optional<ConsistencyLevel> getConsistencyLevel() {
        return ctx.getConsistencyLevel();
    }

    protected Optional<ConsistencyLevel> getSerialConsistencyLevel() {
        return ctx.getSerialConsistencyLevel();
    }


    protected Optional<Duration> getTtl() {
        return ctx.getTtl();
    }
    
    

    
    
    ////////////////////////
    // utility methods
    protected Context getContext() {
        return ctx; 
    }
    
    protected ProtocolVersion getProtocolVersion() {
        return ctx.getSession().getCluster().getConfiguration().getProtocolOptions().getProtocolVersionEnum();
    }
    
    protected Object toStatementValue(String name, Object value) {
        return ctx.toStatementValue(name, value);
    }
    
    protected ImmutableSet<Object> toStatementValue(String name, ImmutableSet<Object> values) {
        return values.stream().map(value -> toStatementValue(name, value)).collect(Immutables.toSet());
    }

    protected ImmutableList<Object> toStatementValue(String name, ImmutableList<Object> values) {
        return values.stream().map(value -> toStatementValue(name, value)).collect(Immutables.toList());
    }
  


    protected Map<Object, Object> toStatementValue(String name, ImmutableMap<Object, Optional<Object>> map) {
        Map<Object, Object> m = Maps.newHashMap();
        for (Entry<Object, Optional<Object>> entry : map.entrySet()) {
            m.put(toStatementValue(name, toStatementValue(name, entry.getKey())), toStatementValue(name, entry.getValue().orElse(null)));
        }
        return m;
    }
    
        
    protected PreparedStatement prepare(BuiltStatement statement) {
        try {
            return ctx.getPreparedStatementsCache().get(statement.getQueryString(), () -> ctx.getSession().prepare(statement));
        } catch (ExecutionException e) {
            throw Exceptions.unwrapIfNecessary(e);
        }
    }
    

    protected CompletableFuture<ResultSet> performAsync(Statement statement) {
        ctx.getConsistencyLevel().ifPresent(cl -> statement.setConsistencyLevel(cl));
        ctx.getWritetime().ifPresent(writetimeMicrosSinceEpoch -> statement.setDefaultTimestamp(writetimeMicrosSinceEpoch));
        ctx.getEnableTracing().ifPresent(enable -> {
                                                        if (enable) {
                                                            statement.enableTracing();
                                                        } else {
                                                            statement.disableTracing(); 
                                                        }
                                                   });
        ctx.getRetryPolicy().ifPresent(policy -> statement.setRetryPolicy(policy));
        
        ResultSetFuture rsFuture = ctx.getSession().executeAsync(statement);
        return new CompletableDbFuture(rsFuture);
    }
    
    
    
    private static class CompletableDbFuture extends CompletableFuture<ResultSet> {
        
        public CompletableDbFuture(ResultSetFuture rsFuture) {
            
            Runnable resultHandler = () -> { 
                try {
                    complete(rsFuture.get());
                    
                } catch (ExecutionException ee) {
                    completeExceptionally(ee.getCause());
                    
                } catch (InterruptedException | RuntimeException e) {
                    completeExceptionally(e);
                }
            };
            rsFuture.addListener(resultHandler, ForkJoinPool.commonPool());
        }
    }   


    
   
    protected Record newRecord(Result result, Row row) {
        return new RecordImpl(result, row);
    }
    
    protected RecordList newRecordList(ResultSet resultSet) {
        return new RecordListImpl(resultSet);
    }
    
    protected <E> EntityList<E> newEntityList(RecordList recordList, Class<E> clazz) {
        return EntityList.newEntityList(ctx, recordList, clazz); 
    }
    
    
    protected class ResultImpl implements Result {
        private final ResultSet rs;
        
        public ResultImpl(ResultSet rs) {
            this.rs = rs;
        }
        
        @Override
        public boolean wasApplied() {
            return rs.wasApplied();
        }
        
        @Override
        public ExecutionInfo getExecutionInfo() {
            return rs.getExecutionInfo();
        }
        
        @Override
        public ImmutableList<ExecutionInfo> getAllExecutionInfo() {
            return ImmutableList.copyOf(rs.getAllExecutionInfo());
        }
        

        public String toString() {
            StringBuilder builder = new StringBuilder(); 
            for (ExecutionInfo info : getAllExecutionInfo())  {

                builder.append("queried=" + info.getQueriedHost());
                builder.append("\r\ntried=")
                       .append(Joiner.on(",").join(info.getTriedHosts()));


                if (info.getAchievedConsistencyLevel() != null) {
                    builder.append("\r\nachievedConsistencyLevel=" + info.getAchievedConsistencyLevel());
                }
                
                if (info.getQueryTrace() != null) {
                    builder.append("\r\ntraceid=" + info.getQueryTrace().getTraceId());
                    builder.append("\r\nevents:\r\n" + Joiner.on("\r\n").join(info.getQueryTrace().getEvents()));
                }
            }
            return builder.toString();
        }
    }
    
    
    private final class RecordImpl extends Record {
        
        public RecordImpl(Result result, Row row) {
            super(result, row);
        }
  
        @SuppressWarnings("unchecked")
        public <T> Optional<T> getObject(String name, Class<T> elementsClass) {
            if (isNull(name)) {
                return Optional.empty();
            }

            DataType datatype = getColumnDefinitions().getType(name);
            
            if (datatype != null) {
                if (ctx.isBuildInType(datatype)) {
                    return (Optional<T>) getBytesUnsafe(name).map(bytes -> datatype.deserialize(bytes, getProtocolVersion()));
                } else {
                    return Optional.ofNullable(ctx.getUDTValueMapper().fromUdtValue(datatype, getUDTValue(name).get(), elementsClass));
                }
            }
            
            return Optional.empty();
        }
        
        
        public <T> Optional<ImmutableSet<T>> getSet(String name, Class<T> elementsClass) {
            if (isNull(name)) {
                return Optional.empty();
            }

            DataType datatype = ctx.getColumnMetadata(name).getType();
            if (ctx.isBuildInType(datatype)) {
                return Optional.of(getRow().getSet(name, elementsClass)).map(set -> ImmutableSet.copyOf(set));
            } else {
                return Optional.of(getRow().getSet(name, UDTValue.class)).map(udtValues -> (ImmutableSet<T>) ctx.getUDTValueMapper().fromUdtValues(datatype.getTypeArguments().get(0), ImmutableSet.copyOf(udtValues), elementsClass));
            }
        }
        
     
        public <T> Optional<ImmutableList<T>> getList(String name, Class<T> elementsClass) {
            if (isNull(name)) {
                return Optional.empty();
            }
            
            DataType datatype = ctx.getColumnMetadata(name).getType();
            if (ctx.isBuildInType(datatype)) {
                return Optional.ofNullable(getRow().getList(name, elementsClass)).map(list -> ImmutableList.copyOf(list));
            } else {
                return Optional.of(getRow().getList(name, UDTValue.class)).map(udtValues -> (ImmutableList<T>) ctx.getUDTValueMapper().fromUdtValues(datatype.getTypeArguments().get(0), ImmutableList.copyOf(udtValues), elementsClass));
            }
        }
        
        
        public <K, V> Optional<ImmutableMap<K, V>> getMap(String name, Class<K> keysClass, Class<V> valuesClass) {
            if (isNull(name)) {
                return Optional.empty();
            }
            
            DataType datatype = ctx.getColumnMetadata(name).getType();
            if (ctx.isBuildInType(datatype)) {
                return Optional.ofNullable(getRow().getMap(name, keysClass, valuesClass)).map(map -> ImmutableMap.copyOf(map));
                
            } else {
                if (ctx.isBuildInType(datatype.getTypeArguments().get(0))) {
                    return Optional.of(getRow().getMap(name, keysClass, UDTValue.class)).map(udtValues -> (ImmutableMap<K, V>) ctx.getUDTValueMapper().fromUdtValues(datatype.getTypeArguments().get(0), datatype.getTypeArguments().get(1), ImmutableMap.copyOf(udtValues), keysClass, valuesClass));

                } else if (ctx.isBuildInType(datatype.getTypeArguments().get(1))) {
                    return Optional.of(getRow().getMap(name, UDTValue.class, valuesClass)).map(udtValues -> (ImmutableMap<K, V>) ctx.getUDTValueMapper().fromUdtValues(datatype.getTypeArguments().get(0), datatype.getTypeArguments().get(1), ImmutableMap.copyOf(udtValues), keysClass, valuesClass));
                    
                } else {
                    return isNull(name) ? Optional.empty() : Optional.of(getRow().getMap(name, UDTValue.class, UDTValue.class)).map(udtValues -> (ImmutableMap<K, V>) ctx.getUDTValueMapper().fromUdtValues(datatype.getTypeArguments().get(0), datatype.getTypeArguments().get(1), ImmutableMap.copyOf(udtValues), keysClass, valuesClass));
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
    
    
    private final class RecordListImpl implements RecordList {
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


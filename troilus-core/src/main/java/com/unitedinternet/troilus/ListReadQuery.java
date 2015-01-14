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

import static com.datastax.driver.core.querybuilder.QueryBuilder.select;

import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.Select;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import com.unitedinternet.troilus.Dao.EntityList;
import com.unitedinternet.troilus.Dao.ListRead;
import com.unitedinternet.troilus.Dao.ListReadWithUnit;
import com.unitedinternet.troilus.Dao.RecordList;
import com.unitedinternet.troilus.DaoImpl.ListReadQueryDataAdapter;
import com.unitedinternet.troilus.interceptor.ListReadQueryData;
import com.unitedinternet.troilus.interceptor.ListReadQueryPostInterceptor;
import com.unitedinternet.troilus.interceptor.ListReadQueryPreInterceptor;




 

class ListReadQuery extends AbstractQuery<ListReadQuery> implements ListReadWithUnit<RecordList> {
    
    final ListReadQueryData data;

    
    public ListReadQuery(Context ctx, ListReadQueryData data) {
        super(ctx);
        this.data = data;
    }

    
    
    @Override
    protected ListReadQuery newQuery(Context newContext) {
        return new ListReadQuery(newContext, data);
    }
    
    
    @Override
    public ListReadQuery all() {
        return new ListReadQuery(getContext(), data.columnsToFetch(ImmutableMap.of()));
    }
    
 
    private ListReadQuery columns(ImmutableCollection<String> namesToRead) {
        ListReadQuery read = this;
        for (String columnName : namesToRead) {
            read = read.column(columnName);
        }
        return read;
    }
    
    
    @Override
    public ListReadQuery column(String name) {
        return new ListReadQuery(getContext(), data.columnsToFetch(Java8Immutables.merge(data.getColumnsToFetch(), name, false)));
    }

    
    @Override
    public ListReadQuery columnWithMetadata(String name) {
        return new ListReadQuery(getContext(), data.columnsToFetch(Java8Immutables.merge(data.getColumnsToFetch(), name, true)));
    }
    
    
    @Override
    public ListReadWithUnit<RecordList> columns(String... names) {
        return columns(ImmutableSet.copyOf(names));
    }
    
    
    @Override
    public ListReadWithUnit<RecordList> column(Name<?> name) {
        return column(name.getName());
    }
    
    @Override
    public ListReadWithUnit<RecordList> columnWithMetadata(Name<?> name) {
        return columnWithMetadata(name.getName());
    }
    
    @Override
    public ListReadWithUnit<RecordList> columns(Name<?>... names) {
        return columns(ImmutableList.copyOf(names).stream().map(name -> name.getName()).collect(Java8Immutables.toList()));
    }

    @Override
    public ListReadQuery withLimit(int limit) {
        return new ListReadQuery(getContext(), data.limit(Optional.of(limit)));
    }
    
    @Override
    public ListReadQuery withAllowFiltering() {
        return new ListReadQuery(getContext(), data.allowFiltering(Optional.of(true)));
    }

    @Override
    public ListReadQuery withFetchSize(int fetchSize) {
        return new ListReadQuery(getContext(), data.fetchSize(Optional.of(fetchSize)));
    }
    
    @Override
    public ListReadQuery withDistinct() {
        return new ListReadQuery(getContext(), data.distinct(Optional.of(true)));
    }
    
   
    @Override
    public ListRead<Count> count() {
        return new CountReadQuery(getContext(), new CountReadQueryData().whereClauses(data.getWhereClauses())
                                                                        .limit(data.getLimit())
                                                                        .fetchSize(data.getFetchSize())
                                                                        .allowFiltering(data.getAllowFiltering())
                                                                        .distinct(data.getDistinct()));
    }
    
    @Override
    public <E> ListEntityReadQuery<E> asEntity(Class<E> objectClass) {
        return new ListEntityReadQuery<>(getContext(), this, objectClass) ;
    }

    

    private ListReadQueryData getPreprocessedData() {
        ListReadQueryData queryData = data;
        for (ListReadQueryPreInterceptor interceptor : getContext().getInterceptorRegistry().getInterceptors(ListReadQueryPreInterceptor.class)) {
            queryData = interceptor.onPreListRead(queryData);
        }
        
        return queryData;
    }
    
    

    @Override
    public CompletableFuture<RecordList> executeAsync() {
        ListReadQueryData preprocessedData = getPreprocessedData(); 
        Statement statement = ListReadQueryDataAdapter.toStatement(preprocessedData, getContext());
        
        return new CompletableDbFuture(performAsync(statement))
                  .<RecordList>thenApply(resultSet -> new RecordListImpl(getContext(), resultSet))
                  .thenApply(recordList -> {
                                              for (ListReadQueryPostInterceptor interceptor : getContext().getInterceptorRegistry().getInterceptors(ListReadQueryPostInterceptor.class)) {
                                                  recordList = interceptor.onPostListRead(preprocessedData, recordList);
                                              }
                                              return recordList;
                                           });
    }        
    
 
    
    
    
     private class ListEntityReadQuery<E> extends AbstractQuery<ListEntityReadQuery<E>> implements ListRead<EntityList<E>> {
        private final ListReadQuery read;
        private final Class<E> clazz;
        
        public ListEntityReadQuery(Context ctx, ListReadQuery read, Class<E> clazz) {
            super(ctx);
            this.read = read;
            this.clazz = clazz;
        }

        @Override
        protected ListEntityReadQuery<E> newQuery(Context newContext) {
            return new ListReadQuery(newContext, read.data).asEntity(clazz);
        }

        @Override
        public ListRead<EntityList<E>> withDistinct() {
            return read.withDistinct().asEntity(clazz);
        }
        
        @Override
        public ListRead<EntityList<E>> withFetchSize(int fetchSize) {
            return read.withFetchSize(fetchSize).asEntity(clazz);
        }
        
        @Override
        public ListRead<EntityList<E>> withAllowFiltering() {
            return read.withAllowFiltering().asEntity(clazz);
        }
        
        @Override
        public ListRead<EntityList<E>> withLimit(int limit) {
            return read.withLimit(limit).asEntity(clazz);
        }

        @Override
        public CompletableFuture<EntityList<E>> executeAsync() {
            return read.executeAsync().thenApply(recordList -> new EntityListImpl<>(getContext(), recordList, clazz));
        }
    }
    
    
     
    private static class RecordListImpl implements RecordList {
         private final Context ctx;
         private final ResultSet rs;

         private final Iterator<Row> iterator;
         private final AtomicReference<DatabaseSubscription> subscriptionRef = new AtomicReference<>();
         
         public RecordListImpl(Context ctx, ResultSet rs) {
             this.ctx = ctx;
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
                     return new RecordAdapter(new RecordImpl(ctx, RecordListImpl.this, iterator.next()));
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
    
    
    private static class EntityListImpl<F> implements EntityList<F> {
        private final Context ctx;
        private final RecordList recordList;
        private final Class<F> clazz;
    
        
        public EntityListImpl(Context ctx, RecordList recordList, Class<F> clazz) {
            this.ctx = ctx;
            this.recordList = recordList;
            this.clazz = clazz;
        }
    
        @Override
        public ExecutionInfo getExecutionInfo() {
            return recordList.getExecutionInfo();
        }
        
        @Override
        public ImmutableList<ExecutionInfo> getAllExecutionInfo() {
            return recordList.getAllExecutionInfo();
        }
        
        @Override
        public boolean wasApplied() {
            return recordList.wasApplied();
        }
    
        @Override
        public Iterator<F> iterator() {
    
            return new Iterator<F>() {
                private final Iterator<Record> recordIt = recordList.iterator();
                
                @Override
                public boolean hasNext() {
                    return recordIt.hasNext();
                }
            
                
                @Override
                public F next() {
                    return ctx.getBeanMapper().fromValues(clazz, RecordAdapter.toPropertiesSource(recordIt.next()));
                }
            };
        }
        
         
        @Override
        public void subscribe(Subscriber<? super F> subscriber) {
            recordList.subscribe(new MappingSubscriber<F>(ctx, clazz, subscriber));
        }
        
        private final class MappingSubscriber<G> implements Subscriber<Record> {
            private final Context ctx;
            private final Class<?> clazz;
            
            private Subscriber<? super G> subscriber;
            
            public MappingSubscriber(Context ctx, Class<?> clazz, Subscriber<? super G> subscriber) {
                this.ctx = ctx;
                this.clazz = clazz;
                this.subscriber = subscriber;
            }
            
            @Override
            public void onSubscribe(Subscription subscription) {
                subscriber.onSubscribe(subscription);
            }
            
            @Override
            public void onNext(Record record) {
                subscriber.onNext(ctx.getBeanMapper().fromValues(clazz, RecordAdapter.toPropertiesSource(record)));
            }
    
            @Override
            public void onError(Throwable t) {
                subscriber.onError(t);
            }
            
            @Override
            public void onComplete() {
                subscriber.onComplete();
            }
        }
    }
    

    
    private static final class CountReadQueryData {
        final ImmutableSet<Clause> whereClauses;
        final Optional<Integer> optionalLimit;
        final Optional<Boolean> optionalAllowFiltering;
        final Optional<Integer> optionalFetchSize;
        final Optional<Boolean> optionalDistinct;

        
        
        public CountReadQueryData() {
            this(ImmutableSet.of(),
                 Optional.empty(),
                 Optional.empty(),
                 Optional.empty(),
                 Optional.empty());
        }
        
        private CountReadQueryData(ImmutableSet<Clause> whereClauses, 
                                   Optional<Integer> optionalLimit, 
                                   Optional<Boolean> optionalAllowFiltering,
                                   Optional<Integer> optionalFetchSize,
                                   Optional<Boolean> optionalDistinct) {
            this.whereClauses = whereClauses;
            this.optionalLimit = optionalLimit;
            this.optionalAllowFiltering = optionalAllowFiltering;
            this.optionalFetchSize = optionalFetchSize;
            this.optionalDistinct = optionalDistinct;
        }
        

        
        public CountReadQueryData whereClauses(ImmutableSet<Clause> whereClauses) {
            return new CountReadQueryData(whereClauses,
                                          this.optionalLimit,
                                          this.optionalAllowFiltering,
                                          this.optionalFetchSize,
                                          this.optionalDistinct);  
        }


        
        public CountReadQueryData limit(Optional<Integer> optionalLimit) {
            return new CountReadQueryData(this.whereClauses,
                                          optionalLimit,
                                          this.optionalAllowFiltering,
                                          this.optionalFetchSize,
                                          this.optionalDistinct);  
        }

        
        public CountReadQueryData allowFiltering(Optional<Boolean> optionalAllowFiltering) {
            return new CountReadQueryData(this.whereClauses,
                                          this.optionalLimit,
                                          optionalAllowFiltering,
                                          this.optionalFetchSize,
                                          this.optionalDistinct);  
        }

        
        public CountReadQueryData fetchSize(Optional<Integer> optionalFetchSize) {
            return new CountReadQueryData(this.whereClauses,
                                          this.optionalLimit,
                                          this.optionalAllowFiltering,
                                          optionalFetchSize,
                                          this.optionalDistinct);  
        }

        
        public CountReadQueryData distinct(Optional<Boolean> optionalDistinct) {
            return new CountReadQueryData(this.whereClauses,
                                          this.optionalLimit,
                                          this.optionalAllowFiltering,
                                          this.optionalFetchSize,
                                          optionalDistinct);  
        }
        
        
        public ImmutableSet<Clause> getWhereClauses() {
            return whereClauses;
        }

        public Optional<Integer> getLimit() {
            return optionalLimit;
        }

        public Optional<Boolean> getAllowFiltering() {
            return optionalAllowFiltering;
        }

        public Optional<Integer> getFetchSize() {
            return optionalFetchSize;
        }

        public Optional<Boolean> getDistinct() {
            return optionalDistinct;
        }
    }


    
    private static class CountReadQuery extends AbstractQuery<CountReadQuery> implements ListRead<Count> {
        
        private final CountReadQueryData data;
    
    
        public CountReadQuery(Context ctx, CountReadQueryData data) {
            super(ctx);
            this.data = data;
        }
    
        
        @Override
        protected CountReadQuery newQuery(Context newContext) {
            return new CountReadQuery(newContext, data);
        }
        
        @Override
        public ListRead<Count> withLimit(int limit) {
            return new CountReadQuery(getContext(),
                                      data.limit(Optional.of(limit))); 
        }
        
        
        @Override
        public ListRead<Count> withAllowFiltering() {
            return new CountReadQuery(getContext(),
                                      data.allowFiltering(Optional.of(true))); 
        }
    
        @Override
        public ListRead<Count> withFetchSize(int fetchSize) {
            return new CountReadQuery(getContext(),
                                      data.fetchSize(Optional.of(fetchSize)));
        }
        
        @Override
        public ListRead<Count> withDistinct() {
            return new CountReadQuery(getContext(),
                                      data.distinct(Optional.of(true)));
        }
    
    
        
        private Statement toStatement(CountReadQueryData queryData) {
            Select.Selection selection = select();
            
            queryData.getDistinct().ifPresent(distinct -> { if (distinct) selection.distinct(); });
    
     
            selection.countAll();
            
            Select select = selection.from(getContext().getTable());
            
            queryData.getWhereClauses().forEach(whereClause -> select.where(whereClause));
            
            queryData.getLimit().ifPresent(limit -> select.limit(limit));
            queryData.getAllowFiltering().ifPresent(allowFiltering -> { if (allowFiltering)  select.allowFiltering(); });
            queryData.getFetchSize().ifPresent(fetchSize -> select.setFetchSize(fetchSize));
            
            return select;
        }

        
        public CompletableFuture<Count> executeAsync() {
            Statement statement = toStatement(data);
            return new CompletableDbFuture(performAsync(statement))
                        .thenApply(resultSet -> Count.newCountResult(resultSet));
        }        
    }  
}
    
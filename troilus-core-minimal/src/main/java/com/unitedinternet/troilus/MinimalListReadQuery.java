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
import java.util.List;
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
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.unitedinternet.troilus.minimal.ListReadQueryData;
import com.unitedinternet.troilus.minimal.ListReadQueryPostInterceptor;
import com.unitedinternet.troilus.minimal.ListReadQueryPreInterceptor;
import com.unitedinternet.troilus.minimal.MinimalDao.EntityList;
import com.unitedinternet.troilus.minimal.MinimalDao.ListRead;
import com.unitedinternet.troilus.minimal.MinimalDao.ListReadWithUnit;
import com.unitedinternet.troilus.minimal.MinimalDao.RecordList;
import com.unitedinternet.troilus.minimal.Record;




 

class MinimalListReadQuery extends AbstractQuery<MinimalListReadQuery> implements ListReadWithUnit<RecordList> {
    
    final ListReadQueryDataImpl data;

    
    public MinimalListReadQuery(Context ctx, ListReadQueryDataImpl data) {
        super(ctx);
        this.data = data;
    }

    
    
    @Override
    protected MinimalListReadQuery newQuery(Context newContext) {
        return new MinimalListReadQuery(newContext, data);
    }
    
    
    @Override
    public MinimalListReadQuery all() {
        return new MinimalListReadQuery(getContext(), data.columnsToFetch(ImmutableMap.<String, Boolean>of()));
    }
    
 
    private MinimalListReadQuery columns(ImmutableCollection<String> namesToRead) {
        MinimalListReadQuery read = this;
        for (String columnName : namesToRead) {
            read = read.column(columnName);
        }
        return read;
    }
    
    
    @Override
    public MinimalListReadQuery column(String name) {
        return new MinimalListReadQuery(getContext(), data.columnsToFetch(Immutables.merge(data.getColumnsToFetch(), name, false)));
    }

    
    @Override
    public MinimalListReadQuery columnWithMetadata(String name) {
        return new MinimalListReadQuery(getContext(), data.columnsToFetch(Immutables.merge(data.getColumnsToFetch(), name, true)));
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
        List<String> ns = Lists.newArrayList();
        for (Name<?> name : names) {
            ns.add(name.getName());
        }
        return columns(ImmutableList.copyOf(ns));
    }

    @Override
    public MinimalListReadQuery withLimit(int limit) {
        return new MinimalListReadQuery(getContext(), data.limit(limit));
    }
    
    @Override
    public MinimalListReadQuery withAllowFiltering() {
        return new MinimalListReadQuery(getContext(), data.allowFiltering(true));
    }

    @Override
    public MinimalListReadQuery withFetchSize(int fetchSize) {
        return new MinimalListReadQuery(getContext(), data.fetchSize(fetchSize));
    }
    
    @Override
    public MinimalListReadQuery withDistinct() {
        return new MinimalListReadQuery(getContext(), data.distinct(true));
    }
    
   

    @Override
    public ListRead<Count> count() {
        return new CountReadQuery(getContext(), new CountReadQueryData().whereConditions(data.getWhereClauses())
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
    public RecordList execute() {
        ListReadQueryData preprocessedData = getPreprocessedData(); 
        Statement statement = ListReadQueryDataImpl.toStatement(preprocessedData, getContext());
        
        RecordList recordList = new RecordListImpl(getContext(), performAsync(statement).getUninterruptibly());
        
        return processPostInterceptor(preprocessedData, recordList);
    }
    

    private RecordList processPostInterceptor(ListReadQueryData preprocessedData, RecordList recordList) {
        for (ListReadQueryPostInterceptor interceptor : getContext().getInterceptorRegistry().getInterceptors(ListReadQueryPostInterceptor.class)) {
            recordList = interceptor.onPostListRead(preprocessedData, recordList);
        }
        return recordList;
    }
        
 
    
    
    
     private class ListEntityReadQuery<E> extends AbstractQuery<ListEntityReadQuery<E>> implements ListRead<EntityList<E>> {
        private final MinimalListReadQuery read;
        private final Class<E> clazz;
        
        public ListEntityReadQuery(Context ctx, MinimalListReadQuery read, Class<E> clazz) {
            super(ctx);
            this.read = read;
            this.clazz = clazz;
        }

        @Override
        protected ListEntityReadQuery<E> newQuery(Context newContext) {
            return new MinimalListReadQuery(newContext, read.data).asEntity(clazz);
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
        public EntityList<E> execute() {
            return new EntityListImpl<>(getContext(), read.execute(), clazz);
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
                     return new RecordImpl(ctx, RecordListImpl.this, iterator.next());
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
                         Runnable databaseRequest = new Runnable() {
                                                                        @Override
                                                                        public void run() {
                                                                            runningDatabaseQuery.set(null); processReadRequests();                                 
                                                                        }
                                                                   };
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
                    return ctx.getBeanMapper().fromValues(clazz, RecordImpl.toPropertiesSource(recordIt.next()));
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
                subscriber.onNext((G) ctx.getBeanMapper().fromValues(clazz, RecordImpl.toPropertiesSource(record)));
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
        final Integer limit;
        final Boolean allowFiltering;
        final Integer fetchSize;
        final Boolean distinct;

        
        
        public CountReadQueryData() {
            this(ImmutableSet.<Clause>of(),
                 null,
                 null,
                 null,
                 null);
        }
        
        private CountReadQueryData(ImmutableSet<Clause> whereClauses, 
                                   Integer limit, 
                                   Boolean allowFiltering,
                                   Integer fetchSize,
                                   Boolean distinct) {
            this.whereClauses = whereClauses;
            this.limit = limit;
            this.allowFiltering = allowFiltering;
            this.fetchSize = fetchSize;
            this.distinct = distinct;
        }
        

        
        public CountReadQueryData whereConditions(ImmutableSet<Clause> whereClauses) {
            return new CountReadQueryData(whereClauses,
                                          this.limit,
                                          this.allowFiltering,
                                          this.fetchSize,
                                          this.distinct);  
        }


        
        public CountReadQueryData limit(Integer limit) {
            return new CountReadQueryData(this.whereClauses,
                    limit,
                                          this.allowFiltering,
                                          this.fetchSize,
                                          this.distinct);  
        }

        
        public CountReadQueryData allowFiltering(Boolean allowFiltering) {
            return new CountReadQueryData(this.whereClauses,
                                          this.limit,
                                          allowFiltering,
                                          this.fetchSize,
                                          this.distinct);  
        }

        
        public CountReadQueryData fetchSize(Integer fetchSize) {
            return new CountReadQueryData(this.whereClauses,
                                          this.limit,
                                          this.allowFiltering,
                                          fetchSize,
                                          this.distinct);  
        }

        
        public CountReadQueryData distinct(Boolean distinct) {
            return new CountReadQueryData(this.whereClauses,
                                          this.limit,
                                          this.allowFiltering,
                                          this.fetchSize,
                                          distinct);  
        }
        
        
        public ImmutableSet<Clause> getWhereConditions() {
            return whereClauses;
        }

        public Integer getLimit() {
            return limit;
        }

        public Boolean getAllowFiltering() {
            return allowFiltering;
        }

        public Integer getFetchSize() {
            return fetchSize;
        }

        public Boolean getDistinct() {
            return distinct;
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
                                      data.limit(limit)); 
        }
        
        
        @Override
        public ListRead<Count> withAllowFiltering() {
            return new CountReadQuery(getContext(),
                                      data.allowFiltering(true)); 
        }
    
        @Override
        public ListRead<Count> withFetchSize(int fetchSize) {
            return new CountReadQuery(getContext(),
                                      data.fetchSize(fetchSize));
        }
        
        @Override
        public ListRead<Count> withDistinct() {
            return new CountReadQuery(getContext(),
                                      data.distinct(true));
        }
    
    
        
        private Statement toStatement(CountReadQueryData queryData) {
            Select.Selection selection = select();
            
            if (queryData.getDistinct() != null) {
                if (queryData.getDistinct()) {
                    selection.distinct(); 
                };
            }
    
     
            selection.countAll();
            
            Select select = selection.from(getContext().getTable());
            
            for (Clause whereCondition : queryData.getWhereConditions()) {
                select.where(whereCondition);
            }
            
            if (queryData.getLimit() != null) {
                select.limit(queryData.getLimit());
            }
            
            if (queryData.getAllowFiltering() != null) {
                if (queryData.getAllowFiltering()) {
                    select.allowFiltering();
                }
            }
            
            if (queryData.getFetchSize() != null) {
                select.setFetchSize(queryData.getFetchSize());
            }
            
            return select;
        }

        
        @Override
        public Count execute() {
            Statement statement = toStatement(data);
            return Count.newCountResult(performAsync(statement).getUninterruptibly());
        }        
    }  
}
    
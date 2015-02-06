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

import static com.datastax.driver.core.querybuilder.QueryBuilder.select;




import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import net.oneandone.troilus.java7.ListRead;
import net.oneandone.troilus.java7.ListReadWithUnit;
import net.oneandone.troilus.java7.Record;
import net.oneandone.troilus.java7.RecordList;
import net.oneandone.troilus.java7.interceptor.ListReadQueryData;
import net.oneandone.troilus.java7.interceptor.ListReadQueryRequestInterceptor;
import net.oneandone.troilus.java7.interceptor.ListReadQueryResponseInterceptor;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.Select;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;




 
/**
 * The list read query implementation
 *
 */
class ListReadQuery extends AbstractQuery<ListReadQuery> implements ListReadWithUnit<RecordList> {
    
    final ListReadQueryDataImpl data;

    
    /**
     * @param ctx   the context 
     * @param data  the data
     */
    ListReadQuery(Context ctx, ListReadQueryDataImpl data) {
        super(ctx);
        this.data = data;
    }
    
    @Override
    protected ListReadQuery newQuery(Context newContext) {
        return new ListReadQuery(newContext, data);
    }
    
    @Override
    public ListReadQuery all() {
        return new ListReadQuery(getContext(), 
                                 data.columnsToFetch(ImmutableMap.<String, Boolean>of()));
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
        return new ListReadQuery(getContext(), 
                                 data.columnsToFetch(Immutables.merge(data.getColumnsToFetch(), name, false)));
    }
    
    @Override
    public ListReadQuery columnWithMetadata(String name) {
        return new ListReadQuery(getContext(), 
                                 data.columnsToFetch(Immutables.merge(data.getColumnsToFetch(), name, true)));
    }
    
    @Override
    public ListReadQuery columns(String... names) {
        return columns(ImmutableSet.copyOf(names));
    }
    
    @Override
    public ListReadQuery column(ColumnName<?> name) {
        return column(name.getName());
    }
    
    @Override
    public ListReadQuery columnWithMetadata(ColumnName<?> name) {
        return columnWithMetadata(name.getName());
    }
    
    @Override
    public ListReadQuery columns(ColumnName<?>... names) {
        List<String> ns = Lists.newArrayList();
        for (ColumnName<?> name : names) {
            ns.add(name.getName());
        }
        return columns(ImmutableList.copyOf(ns));
    }

    @Override
    public ListReadQuery withLimit(int limit) {
        return new ListReadQuery(getContext(), data.limit(limit));
    }
    
    @Override
    public ListReadQuery withAllowFiltering() {
        return new ListReadQuery(getContext(), data.allowFiltering(true));
    }

    @Override
    public ListReadQuery withFetchSize(int fetchSize) {
        return new ListReadQuery(getContext(), data.fetchSize(fetchSize));
    }
    
    @Override
    public ListReadQuery withDistinct() {
        return new ListReadQuery(getContext(), data.distinct(true));
    }
    
    @Override
    public CountReadQuery count() {
        return new CountReadQuery(getContext(), new CountReadQueryData().whereConditions(data.getWhereConditions())
                                                                        .limit(data.getLimit())
                                                                        .fetchSize(data.getFetchSize())
                                                                        .allowFiltering(data.getAllowFiltering())
                                                                        .distinct(data.getDistinct()));
    }
    
    @Override
    public <E> ListEntityReadQuery<E> asEntity(Class<E> objectClass) {
        return new ListEntityReadQuery<>(getContext(), this, objectClass) ;
    }

    @Override
    public RecordList execute() {
        return ListenableFutures.getUninterruptibly(executeAsync());
    }
    
    @Override
    public ListenableFuture<RecordList> executeAsync() {
        // perform request executors
        ListenableFuture<ListReadQueryData> queryDataFuture = executeRequestInterceptorsAsync(Futures.<ListReadQueryData>immediateFuture(data));  

        // execute query asnyc
        Function<ListReadQueryData, ListenableFuture<RecordList>> queryExecutor = new Function<ListReadQueryData, ListenableFuture<RecordList>>() {
            @Override
            public ListenableFuture<RecordList> apply(ListReadQueryData querData) {
                return executeAsync(querData);
            }
        };
        return ListenableFutures.transform(queryDataFuture, queryExecutor, MoreExecutors.directExecutor());
    }

    
    private ListenableFuture<RecordList> executeAsync(ListReadQueryData queryData) {
        // perform query
        ListenableFuture<ResultSet> resultSetFuture = performAsync(ListReadQueryDataImpl.toStatement(queryData, getContext()));        
        
        // result set to record list mapper
        Function<ResultSet, RecordList> resultSetToRecordList = new Function<ResultSet, RecordList>() {
            
            @Override
            public RecordList apply(ResultSet resultSet) {
                return new RecordListImpl(getContext(), resultSet);
            }
        };
        ListenableFuture<RecordList> recordListFuture =  Futures.transform(resultSetFuture, resultSetToRecordList); 
        
        // perform response interceptor
        return executeResponseInterceptorsAsync(queryData, recordListFuture);
    }

    
    private ListenableFuture<ListReadQueryData> executeRequestInterceptorsAsync(ListenableFuture<ListReadQueryData> queryDataFuture) {

        for (ListReadQueryRequestInterceptor interceptor : getContext().getInterceptorRegistry().getInterceptors(ListReadQueryRequestInterceptor.class).reverse()) {
            final ListReadQueryRequestInterceptor icptor = interceptor;
            
            Function<ListReadQueryData, ListenableFuture<ListReadQueryData>> mapperFunction = new Function<ListReadQueryData, ListenableFuture<ListReadQueryData>>() {
                @Override
                public ListenableFuture<ListReadQueryData> apply(ListReadQueryData queryData) {
                    return icptor.onListReadRequestAsync(queryData);
                }
            };
            
            queryDataFuture = ListenableFutures.transform(queryDataFuture, mapperFunction, getContext().getTaskExecutor());
        }

        return queryDataFuture;
    }
    
    
    private ListenableFuture<RecordList> executeResponseInterceptorsAsync(final ListReadQueryData queryData, ListenableFuture<RecordList> recordFuture) {
    
        for (ListReadQueryResponseInterceptor interceptor : getContext().getInterceptorRegistry().getInterceptors(ListReadQueryResponseInterceptor.class).reverse()) {
            final ListReadQueryResponseInterceptor icptor = interceptor;
            
            Function<RecordList, ListenableFuture<RecordList>> mapperFunction = new Function<RecordList, ListenableFuture<RecordList>>() {
                @Override
                public ListenableFuture<RecordList> apply(RecordList recordList) {
                    return icptor.onListReadResponseAsync(queryData, recordList);
                }
            };
            
            recordFuture = ListenableFutures.transform(recordFuture, mapperFunction, getContext().getTaskExecutor());
        }

        return recordFuture;
    }
    
    
    /**
     * The entity list read implementation
     * @param <E> the entity type
     */
    static class ListEntityReadQuery<E> extends AbstractQuery<ListEntityReadQuery<E>> implements ListRead<EntityList<E>> {
        private final ListReadQuery query;
        private final Class<E> clazz;
        
        /**
         * @param ctx   the context
         * @param query the query 
         * @param clazz the entity type
         */
        ListEntityReadQuery(Context ctx, ListReadQuery query, Class<E> clazz) {
            super(ctx);
            this.query = query;
            this.clazz = clazz;
        }

        @Override
        protected ListEntityReadQuery<E> newQuery(Context newContext) {
            return new ListReadQuery(newContext, query.data).asEntity(clazz);
        }

        @Override
        public ListEntityReadQuery<E> withDistinct() {
            return query.withDistinct().asEntity(clazz);
        }
        
        @Override
        public ListEntityReadQuery<E> withFetchSize(int fetchSize) {
            return query.withFetchSize(fetchSize).asEntity(clazz);
        }
        
        @Override
        public ListEntityReadQuery<E> withAllowFiltering() {
            return query.withAllowFiltering().asEntity(clazz);
        }
        
        @Override
        public ListEntityReadQuery<E> withLimit(int limit) {
            return query.withLimit(limit).asEntity(clazz);
        }
        
        @Override
        public EntityList<E> execute() {
            return ListenableFutures.getUninterruptibly(executeAsync());
        }

        @Override
        public ListenableFuture<EntityList<E>> executeAsync() {
            ListenableFuture<RecordList> future = query.executeAsync();
            
            Function<RecordList, EntityList<E>> mapEntity = new Function<RecordList, EntityList<E>>() {
                @Override
                public EntityList<E> apply(RecordList recordList) {
                    return new EntityListImpl<>(getContext(), recordList, clazz);
                }
            };
            
            return Futures.transform(future, mapEntity);
        }
    }
    
    
     
    private static class RecordListImpl implements RecordList {
         private static final Logger LOG = LoggerFactory.getLogger(RecordListImpl.class);
        
         private final Context ctx;
         private final ResultSet rs;

         private final Iterator<Row> iterator;
         private boolean subscribed = false; // true after first subscribe
         
         RecordListImpl(Context ctx, ResultSet rs) {
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
             synchronized (this) {
                 if (subscribed == true) {
                     subscriber.onError(new IllegalStateException("subscription already exists. Multi-subscribe is not supported"));  // only one allowed
                 } else {
                     subscribed = true;
                     subscriber.onSubscribe(new DatabaseSubscription(subscriber));
                 }
             }
         }
         
         
         private final class DatabaseSubscription implements Subscription {
             
             private final Object subscriberCallbackLock = new Object();
             private final Object dbQueryLock = new Object();
             
             private final Subscriber<? super Record> subscriber;
             private final Iterator<? extends Record> it;
             
             private final AtomicLong numPendingReads = new AtomicLong();
             
             private Runnable runningDatabaseQuery = null;
             private boolean isOpen = true;

             
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
                 processAvailableDatabaseRecords();

                 // more db records required? 
                 if (numPendingReads.get() > 0) {
                     // [synchronization note] under some circumstances the method requestDatabaseForMoreRecords()
                     // will be executed without the need of more records. However, it does not matter
                     requestDatabaseForMoreRecords();
                 }
             }
             
             
             private void processAvailableDatabaseRecords() {
                 synchronized (subscriberCallbackLock) {
                     if (isOpen) {
                         while (it.hasNext() && numPendingReads.get() > 0) {
                             try {
                                 numPendingReads.decrementAndGet();
                                 subscriber.onNext(it.next());
                             } catch (RuntimeException rt) {
                                 LOG.warn("processing error occured", rt);
                                 teminateWithError(rt);
                             }
                         }

                     } else { 
                         subscriber.onError(new IllegalStateException("already closed"));
                     }
                 }
             }
             
             
             private void requestDatabaseForMoreRecords() {
                 // no more data to fetch?
                 if (rs.isFullyFetched()) {
                     terminateRegularly();
                     return;
                 } 
                 
                 synchronized (dbQueryLock) {
                     if (runningDatabaseQuery == null) {
                         Runnable databaseRequest = new Runnable() {
                                                             @Override
                                                             public void run() {
                                                                 synchronized (dbQueryLock) {
                                                                     runningDatabaseQuery = null; 
                                                                 }
                                                                 processReadRequests();
                                                             }                                                                           
                                                    };
                         runningDatabaseQuery = databaseRequest;
                         
                         ListenableFuture<Void> future = rs.fetchMoreResults();
                         future.addListener(databaseRequest, ctx.getTaskExecutor());
                     }
                 }
             }
        
             
             @Override
             public void cancel() {
                 terminateRegularly();
             }

             

             ////////////
             // terminate methods: Once a terminal state has been signaled (onError, onComplete) it is REQUIRED that no further signals occur
             
             private void terminateRegularly() {
                 synchronized (subscriberCallbackLock) {
                     if (isOpen) {
                         isOpen = false;
                         subscriber.onComplete();
                     }
                 }
             }
             
             private void teminateWithError(Throwable t) {
                 synchronized (subscriberCallbackLock) {
                     if (isOpen) {
                         isOpen = false;
                         subscriber.onError(t);
                     }
                 }
             }
         }
     } 
    
    
    private static class EntityListImpl<F> extends ResultAdapter implements EntityList<F> {
        private final Context ctx;
        private final RecordList recordList;
        private final Class<F> clazz;
    
        EntityListImpl(Context ctx, RecordList recordList, Class<F> clazz) {
            super(recordList);
            this.ctx = ctx;
            this.recordList = recordList;
            this.clazz = clazz;
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
                    return ctx.getBeanMapper().fromValues(clazz, RecordImpl.toPropertiesSource(recordIt.next()), ctx.getDbSession().getColumnNames());
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
            
            @SuppressWarnings("unchecked")
            @Override
            public void onNext(Record record) {
                subscriber.onNext((G) ctx.getBeanMapper().fromValues(clazz, RecordImpl.toPropertiesSource(record), ctx.getDbSession().getColumnNames()));
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


    
    static class CountReadQuery extends AbstractQuery<CountReadQuery> implements ListRead<Count> {
        
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
        public CountReadQuery withLimit(int limit) {
            return new CountReadQuery(getContext(),
                                      data.limit(limit)); 
        }
        
        
        @Override
        public CountReadQuery withAllowFiltering() {
            return new CountReadQuery(getContext(),
                                      data.allowFiltering(true)); 
        }
    
        @Override
        public CountReadQuery withFetchSize(int fetchSize) {
            return new CountReadQuery(getContext(),
                                      data.fetchSize(fetchSize));
        }
        
        @Override
        public CountReadQuery withDistinct() {
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
            
            Select select = selection.from(getContext().getDbSession().getTablename());
            
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
            return ListenableFutures.getUninterruptibly(executeAsync());
        }      
        
        
        @Override
        public ListenableFuture<Count> executeAsync() {
            ListenableFuture<ResultSet> future = performAsync(toStatement(data));
            
            Function<ResultSet, Count> mapEntity = new Function<ResultSet, Count>() {
                @Override
                public Count apply(ResultSet resultSet) {
                    return Count.newCountResult(resultSet);
                }
            };
            
            return Futures.transform(future, mapEntity);
        }
    }  
}
    
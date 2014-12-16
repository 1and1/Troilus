/*
 * Copyright (c) 2014 Gregor Roth
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
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BatchStatement.Type;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Select.Selection;
import com.datastax.driver.core.ConsistencyLevel;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListenableFuture;

 

@SuppressWarnings("rawtypes")
class DaoImpl implements Dao {
    private final DaoContext ctx;
    
    
    DaoImpl(DaoContext ctx) {
        this.ctx = ctx;
    }
 
    
    @Override
    public Dao withConsistency(ConsistencyLevel consistencyLevel) {
        return new DaoImpl(ctx.withConsistency(consistencyLevel));
    }
    
    
    
    
    ///////////////////////////////
    // INSERT
    

    @Override
    public InsertionWithUnit insert() {
        return new InsertQuery(ctx, ImmutableMap.of());
    }
    
    @Override
    public Insertion insertObject(Object persistenceObject) {
        return new InsertQuery(ctx, persistenceObject);
    }
    
    @Override
    public Insertion insertValues(String name1, Object value1, String name2, Object value2) {
        return insert().values(ImmutableMap.of(name1, value1, name2, value2));
    }
    
    
    @Override
    public Insertion insertValues(String name1, Object value1, String name2, Object value2, String name3, Object value3) {
        return insert().values(ImmutableMap.of(name1, value1, name2, value2, name3, value3));
    }
    
    
    @Override
    public Insertion insertValues(String name1, Object value1, String name2, Object value2, String name3, Object value3, String name4, Object value4) {
        return insert().values(ImmutableMap.of(name1, value1, name2, value2, name3, value3, name4, value4));
    }
    
    
    @Override
    public Insertion insertValues(String name1, Object value1, String name2, Object value2, String name3, Object value3, String name4, Object value4, String name5, Object value5) {
        return insert().values(ImmutableMap.of(name1, value1, name2, value2, name3, value3, name4, value4, name5, value5));
    }
  
    
    private static class InsertQuery extends MutationQueryImpl implements InsertionWithUnit {
        private final ImmutableMap<String, Object> nameValuePairs;
        
        public InsertQuery(DaoContext ctx, Object persistenceObject) {
           this(ctx, ctx.getPropertiesMapper(persistenceObject.getClass()).toValues(persistenceObject));
        }
       
         
        public InsertQuery(DaoContext ctx, ImmutableMap<String, Object> nameValuePairs) {
            super(ctx);
            this.nameValuePairs = nameValuePairs;
        }
        
        @Override
        public Insertion entity(Object persistenceObject) {
            return new InsertQuery(getContext(), persistenceObject);
        }
        
        @Override
        public InsertionWithValues value(String name, Object value) {
            if (value instanceof Optional) {
                if (((Optional) value).isPresent()) {
                    value = ((Optional) value).get();
                } else {
                    return this;
                }
            }
            
            return new InsertQuery(getContext(), Immutables.merge(nameValuePairs, name, value));
        }

        
        @SuppressWarnings("unchecked")
        @Override
        public InsertionWithValues values(ImmutableMap<String , Object> nameValuePairsToAdd) {
            
            // convert optional
            Map<String, Object> pairs = Maps.newHashMap();
            nameValuePairsToAdd.forEach((name, value) -> {
                                                            if (value instanceof Optional) {
                                                                ((Optional) value).ifPresent(v -> pairs.put(name, v));
                                                            } else {
                                                                pairs.put(name, value);
                                                            }
                                                        });
            
            return new InsertQuery(getContext(), Immutables.merge(nameValuePairs, ImmutableMap.copyOf(pairs)));
        }
        
        
        
        @Override
        public Insertion withConsistency(ConsistencyLevel consistencyLevel) {
            return new InsertQuery(getContext().withConsistency(consistencyLevel), nameValuePairs);
        }
        
        @Override
        public Insertion ifNotExits() {
            return new InsertQuery(getContext().ifNotExits(), nameValuePairs);
        }
        
        @Override
        public Insertion withTtl(Duration ttl) {
            return new InsertQuery(getContext().withTtl(ttl), nameValuePairs);
        }

        @Override
        public Insertion withWritetime(long writetimeMicrosSinceEpoch) {
            return new InsertQuery(getContext().withWritetime(writetimeMicrosSinceEpoch), nameValuePairs);
        }
        
        @Override
        public BatchMutation combinedWith(Mutation other) {
            return new MutationBatch(getContext(), Type.LOGGED, ImmutableList.of(this, other));
        }
      
        
        @Override
        protected Statement getStatement() {
            
            // statement
            Insert insert = insertInto(getContext().getTable());
            nameValuePairs.keySet().forEach(name -> insert.value(name, bindMarker()));
            
            if (getContext().getIfNotExits()) {
                insert.ifNotExists();
            }

            if (getContext().getTtl().isPresent())  {
                insert.using(QueryBuilder.ttl(bindMarker()));
            }
            
            
            PreparedStatement stmt = getContext().prepare(insert);

            
            // bind variables
            ImmutableList<Object> values = ImmutableList.copyOf(nameValuePairs.values());
            if (getContext().getTtl().isPresent()) {
                values = ImmutableList.<Object>builder().addAll(values).add((int) getContext().getTtl().get().getSeconds()).build();
            }
            
            return stmt.bind(values.toArray());
        }
        
        
        
        @Override
        public CompletableFuture<Void> executeAsync() {
            return getContext().performAsync(getStatement()).thenApply(resultSet -> {
                    if (getContext().getIfNotExits()) {
                        // check cas result column '[applied]'
                        if (!resultSet.wasApplied()) {
                            throw new AlreadyExistsConflictException("duplicated entry");  
                        }
                    } 
                    return null;
                });
        }
    }


    
    ///////////////////////////////
    // DELETE
    
    @Override
    public Deletion deleteWithKey(String keyName, Object keyValue) {
        return new DeleteQuery(ctx, ImmutableMap.of(keyName, keyValue));
    }

    @Override
    public Deletion deleteWithKey(String keyName1, Object keyValue1, String keyName2, Object keyValue2) {
        return new DeleteQuery(ctx, ImmutableMap.of(keyName1, keyValue1, keyName2, keyValue2));
    }
    
    @Override
    public Deletion deleteWithKey(String keyName1, Object keyValue1, String keyName2, Object keyValue2, String keyName3, Object keyValue3) {
        return new DeleteQuery(ctx, ImmutableMap.of(keyName1, keyValue1, keyName2, keyValue2, keyName3, keyValue3));
    }
    
    @Override
    public Deletion deleteWithKey(String keyName1, Object keyValue1, String keyName2, Object keyValue2, String keyName3, Object keyValue3, String keyName4, Object keyValue4) {
        return new DeleteQuery(ctx, ImmutableMap.of(keyName1, keyValue1, keyName2, keyValue2, keyName3, keyValue3, keyName4, keyValue4));
    }
    
    
    private static class DeleteQuery extends MutationQueryImpl implements Deletion {
        private final ImmutableMap<String, Object> keyNameValuePairs;
        
        public DeleteQuery(DaoContext ctx, ImmutableMap<String, Object> keyNameValuePairs) {
            super(ctx);
            this.keyNameValuePairs = keyNameValuePairs;
        }
        
        @Override
        public Deletion withConsistency(ConsistencyLevel consistencyLevel) {
            return new DeleteQuery(getContext().withConsistency(consistencyLevel), keyNameValuePairs);
        }
        
        
        @Override
        public BatchMutation combinedWith(Mutation other) {
            return new MutationBatch(getContext(), Type.LOGGED, ImmutableList.of(this, other));
        }
        
        
        @Override
        protected Statement getStatement() {
            Delete delete = delete().from(getContext().getTable());

            Delete.Where where = null;
            for (Clause whereClause : keyNameValuePairs.keySet().stream().map(name -> eq(name, bindMarker())).collect(Immutables.toSet())) {
                if (where == null) {
                    where = delete.where(whereClause);
                } else {
                    where = where.and(whereClause);
                }
            }
            
            return getContext().prepare(delete).bind(keyNameValuePairs.values().toArray());
        }
    }

     
    private static final class MutationBatch extends MutationQueryImpl implements BatchMutation {
        private final ImmutableList<Mutation<?>> mutations;
        private final Type type;  
        
        public MutationBatch(DaoContext ctx, Type type, ImmutableList<Mutation<?>> mutations) {
            super(ctx);
            this.type = type;
            this.mutations = mutations;
        }
                
        
        @Override
        public Query<Void> withLockedBatchType() {
            return new MutationBatch(getContext(), Type.LOGGED, mutations);
        }
        
        @Override
        public Query<Void> withUnlockedBatchType() {
            return new MutationBatch(getContext(), Type.UNLOGGED, mutations);
        }
        
         
        @Override
        public BatchMutation combinedWith(Mutation<?> other) {
            return new MutationBatch(getContext(), type, Immutables.merge(mutations, other));
        }
        
        @Override
        protected Statement getStatement() {
            BatchStatement batchStmt = new BatchStatement(type);
            mutations.forEach(mutation -> batchStmt.add(((MutationQueryImpl) mutation).getStatement()));
            return batchStmt;
        }
        
        public CompletableFuture<Void> executeAsync() {
            return getContext().performAsync(getStatement()).thenApply(resultSet -> null);
        }
    }
    
    
    
    
    

    
    ///////////////////////////////
    // READ
    

    @Override
    public ReadWithUnit<Optional<Record>> readWithKey(String keyName, Object keyValue) {
        return new SingleReadQuery(ctx, ImmutableMap.of(keyName, keyValue), Optional.empty());
    }
    
    @Override
    public ReadWithUnit<Optional<Record>> readWithKey(String keyName1, Object keyValue1, String keyName2, Object keyValue2) {
        return new SingleReadQuery(ctx, ImmutableMap.of(keyName1, keyValue1, keyName2, keyValue2), Optional.of(ImmutableSet.of()));
    }
    
    @Override
    public ReadWithUnit<Optional<Record>> readWithKey(String keyName1, Object keyValue1, String keyName2, Object keyValue2, String keyName3, Object keyValue3) {
        return new SingleReadQuery(ctx, ImmutableMap.of(keyName1, keyValue1, keyName2, keyValue2, keyName3, keyValue3), Optional.of(ImmutableSet.of()));
    }
    
    @Override
    public ReadWithUnit<Optional<Record>> readWithKey(String keyName1, Object keyValue1, String keyName2, Object keyValue2, String keyName3, Object keyValue3, String keyName4, Object keyValue4) {
        return new SingleReadQuery(ctx, ImmutableMap.of(keyName1, keyValue1, keyName2, keyValue2, keyName3, keyValue3, keyName4, keyValue4), Optional.of(ImmutableSet.of()));
    }
    

    private static class SingleReadQuery extends QueryImpl<Optional<Record>> implements ReadWithUnit<Optional<Record>> {
        private final ImmutableMap<String, Object> keyNameValuePairs;
        private final Optional<ImmutableSet<ColumnToFetch>> columnsToFetch;
         
        
        public SingleReadQuery(DaoContext ctx, ImmutableMap<String, Object> keyNameValuePairs, Optional<ImmutableSet<ColumnToFetch>> columnsToFetch) {
            super(ctx);
            this.keyNameValuePairs = keyNameValuePairs;
            this.columnsToFetch = columnsToFetch;
        }
         
        
        
        @Override
        public <E> Read<Optional<E>> entity(Class<E> objectClass) {
            return new SingleEntityReadQuery<E>(getContext(), this, objectClass);
        }
        
        @Override
        public ReadWithUnit<Optional<Record>> column(String name) {
            return column(name, false, false);
        }

        @Override
        public ReadWithUnit<Optional<Record>> column(String name, boolean isFetchWritetime, boolean isFetchTtl) {
            return new SingleReadQuery(getContext(), keyNameValuePairs, Immutables.merge(columnsToFetch, ColumnToFetch.create(name, isFetchWritetime, isFetchTtl)));
        }
        
        @Override
        public ReadWithUnit<Optional<Record>> columns(String... names) {
            return columns(ImmutableSet.copyOf(names));
        }
        
        @Override 
        public ReadWithUnit<Optional<Record>> columns(ImmutableCollection<String> namesToRead) {
            return new SingleReadQuery(getContext(), keyNameValuePairs, Immutables.merge(columnsToFetch, ColumnToFetch.create(namesToRead)));
        }
        
        @Override
        public Read<Optional<Record>> withConsistency(ConsistencyLevel consistencyLevel) {
            return new SingleReadQuery(getContext().withConsistency(consistencyLevel), keyNameValuePairs, columnsToFetch);
        }
       
        
        @Override
        public CompletableFuture<Optional<Record>> executeAsync() {
            Selection selection = select();
            
            if (columnsToFetch.isPresent()) {
                columnsToFetch.get().forEach(column -> column.accept(selection));
            } else {
                selection.all();
            }
            
            Select select = selection.from(getContext().getTable());
            Select.Where where = null;
            for (Clause whereClause : keyNameValuePairs.keySet().stream().map(name -> eq(name, bindMarker())).collect(Immutables.toSet())) {
                if (where == null) {
                    where = select.where(whereClause);
                } else {
                    where = where.and(whereClause);
                }
            }

            Statement statement = getContext().prepare(select).bind(keyNameValuePairs.values().toArray());
            
            
            return getContext().performAsync(statement)
                               .thenApply(resultSet -> {
                                                            Record record = null; 
                                                            Row row = resultSet.one();
                                                            if (row != null) {
                                                                record = new Record(getContext().getProtocolVersion(), row);
                                                                
                                                                if (!resultSet.isExhausted()) {
                                                                    throw new TooManyResultsException("more than one record exists");
                                                                }
                                                            }
                                            
                                                            return Optional.ofNullable(record);
                                           });
        }
    }
     
    
    
    private static class SingleEntityReadQuery<E> extends QueryImpl<Optional<E>> implements Read<Optional<E>> {
   
        private final Read<Optional<Record>> read;
        private final Class<?> clazz;
        
        public SingleEntityReadQuery(DaoContext ctx, Read<Optional<Record>> read, Class<?> clazz) {
            super(ctx);
            this.read = read;
            this.clazz = clazz;
        }
        
        @Override
        public Read<Optional<E>> withConsistency(ConsistencyLevel consistencyLevel) {
            return new SingleEntityReadQuery<E>(getContext(), read.withConsistency(consistencyLevel), clazz);
        }
        
        @Override
        public CompletableFuture<Optional<E>> executeAsync() {
            return read.executeAsync().thenApply(optionalRecord -> optionalRecord.map(record -> getContext().getPropertiesMapper(clazz).fromValues(record)));
        }        
    }
     
   

    
    private static class ColumnToFetch implements Consumer<Selection> {
        private final String name;
        private final boolean isFetchWritetime;
        private final boolean isFetchTtl;
        
        private ColumnToFetch(String name, boolean isFetchWritetime, boolean isFetchTtl) {
            this.name = name;
            this.isFetchWritetime = isFetchWritetime;
            this.isFetchTtl = isFetchTtl;
        }
        
        public static ColumnToFetch create(String name, boolean isFetchWritetime, boolean isFetchTtl) {
            return new ColumnToFetch(name, isFetchWritetime, isFetchTtl);
        }
        
        public static ImmutableSet<ColumnToFetch> create(ImmutableCollection<String> names) {
            return names.stream().map(name -> new ColumnToFetch(name, false, false)).collect(Immutables.toSet());
        }

        @Override
        public void accept(Selection selection) {
             selection.column(name);

             if (isFetchTtl) {
                 selection.ttl(name);
             }

             if (isFetchWritetime) {
                 selection.writeTime(name);
             }
        }
    }
    
    
    
    @Override
    public ReadListWithUnit<Result<Record>> readWithPartialKey(String keyName, Object keyValue) {
        return new ListReadQuery(ctx, ImmutableMap.of(keyName, keyValue), Optional.of(ImmutableSet.of()), Optional.empty());
    }
    
    @Override
    public ReadListWithUnit<Result<Record>> readWithPartialKey(String keyName1, Object keyValue1, String keyName2, Object keyValue2) {
        return new ListReadQuery(ctx, ImmutableMap.of(keyName1, keyValue1, keyName2, keyValue2), Optional.of(ImmutableSet.of()), Optional.empty());
    }
    
    @Override
    public ReadListWithUnit<Result<Record>> readWithPartialKey(String keyName1, Object keyValue1, String keyName2, Object keyValue2, String keyName3, Object keyValue3) {
        return new ListReadQuery(ctx, ImmutableMap.of(keyName1, keyValue1, keyName2, keyValue2, keyName3, keyValue3), Optional.of(ImmutableSet.of()), Optional.empty());
    }
    
    @Override
    public ReadListWithUnit<Result<Record>> read() {
        return new ListReadQuery(ctx, ImmutableMap.of(), Optional.of(ImmutableSet.of()), Optional.empty());
    }
    
    
    
    
    private static class ListReadQuery extends QueryImpl<Result<Record>> implements ReadListWithUnit<Result<Record>> {
        private final ImmutableMap<String, Object> keyNameValuePairs;
        private final Optional<ImmutableSet<ColumnToFetch>> columnsToFetch;
        private final Optional<Integer> limit;


        public ListReadQuery(DaoContext ctx, ImmutableMap<String, Object> keyNameValuePairs, Optional<ImmutableSet<ColumnToFetch>> columnsToFetch, Optional<Integer> limit) {
            super(ctx);
            this.keyNameValuePairs = keyNameValuePairs;
            this.columnsToFetch = columnsToFetch;
            this.limit = limit;
        }

        
        @Override
        public ReadList<Result<Record>> withLimit(int limit) {
            return new ListReadQuery(getContext(), keyNameValuePairs, columnsToFetch, Optional.of(limit));
        }
        
        @Override
        public ReadList<Result<Record>> withConsistency(ConsistencyLevel consistencyLevel) {
            return new ListReadQuery(getContext().withConsistency(consistencyLevel), keyNameValuePairs, columnsToFetch, limit);
        }
        
      
        @Override
        public <E> ReadList<Result<E>> entity(Class<E> objectClass) {
            return new ListEntityReadQuery<E>(getContext(), this, objectClass) ;
        }
        
        @Override
        public ReadListWithUnit<Result<Record>> column(String name) {
            return column(name, false, false);
        }

        @Override
        public ReadListWithUnit<Result<Record>> column(String name, boolean isFetchWritetime, boolean isFetchTtl) {
            return new ListReadQuery(getContext(), keyNameValuePairs,  Immutables.merge(columnsToFetch, ColumnToFetch.create(name, isFetchWritetime, isFetchTtl)), limit);
        }
        
        @Override
        public ReadListWithUnit<Result<Record>> columns(String... names) {
            return columns(ImmutableSet.copyOf(names));
        }
        
        @Override 
        public ReadListWithUnit<Result<Record>> columns(ImmutableCollection<String> namesToRead) {
            return new ListReadQuery(getContext(), keyNameValuePairs, Immutables.merge(columnsToFetch, ColumnToFetch.create(namesToRead)), limit);
        }
        

        
        @Override
        public CompletableFuture<Result<Record>> executeAsync() {
   
            Selection selection = select();
            
            if (columnsToFetch.isPresent()) {
                columnsToFetch.get().forEach(column -> column.accept(selection));
            } else {
                selection.all();
            }
            
            Select select = selection.from(getContext().getTable());
            Select.Where where = null;
            for (Clause whereClause : keyNameValuePairs.keySet().stream().map(name -> eq(name, bindMarker())).collect(Immutables.toSet())) {
                if (where == null) {
                    where = select.where(whereClause);
                } else {
                    where = where.and(whereClause);
                }
            }
            
            limit.ifPresent(limit -> select.limit(limit));
            Statement statement = getContext().prepare(select).bind(keyNameValuePairs.values().toArray());
            
            
            return getContext().performAsync(statement)
                               .thenApply(resultSet -> new RecordsImpl(getContext().getProtocolVersion(), resultSet));
        }        
        
        
        private static final class RecordsImpl implements Result<Record> {
            private final ProtocolVersion protocolVersion;
            private final Iterator<Row> iterator;
            private final ResultSet rs;
            private final AtomicReference<DatabaseSubscription> subscriptionRef = new AtomicReference<>();
            
            public RecordsImpl(ProtocolVersion protocolVersion, ResultSet rs) {
                this.protocolVersion = protocolVersion;
                this.rs = rs;
                this.iterator = rs.iterator();
            }
            
          
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }
            
            @Override
            public Record next() {
                return new Record(protocolVersion, iterator.next());
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
                
                private final AtomicLong numPendingReads = new AtomicLong();
                private final AtomicReference<Runnable> runningDatabaseQuery = new AtomicReference<>();
                
                public DatabaseSubscription(Subscriber<? super Record> subscriber) {
                    this.subscriber = subscriber;
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
                                subscriber.onNext(next());
                            }                    
                            
                        // requested partly available                        
                        } else {
                            requestDatabaseForMoreRecords();
                            numPendingReads.addAndGet(-available);
                            for (int i = 0; i < available; i++) {
                                subscriber.onNext(next());
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
    
    
    
    private static class ListEntityReadQuery<E> extends QueryImpl<Result<E>> implements ReadList<Result<E>> {
        private final ReadList<Result<Record>> read;
        private final Class<?> clazz;
        
        public ListEntityReadQuery(DaoContext ctx, ReadList<Result<Record>> read, Class<?> clazz) {
            super(ctx);
            this.read = read;
            this.clazz = clazz;
        }
    
    
        @Override
        public Read<Result<E>> withConsistency( ConsistencyLevel consistencyLevel) {
            return new ListEntityReadQuery<E>(getContext().withConsistency(consistencyLevel), read, clazz);
        }
        
        
        @Override
        public ReadList<Result<E>> withLimit(int limit) {
            return new ListEntityReadQuery<E>(getContext(), read.withLimit(limit), clazz);
        }

        
        @Override
        public CompletableFuture<Result<E>> executeAsync() {
            return read.executeAsync().thenApply(recordIterator -> new ResultIteratorImpl<>(getContext(), recordIterator, clazz));
        }
        
        
        
        private static final class ResultIteratorImpl<E> implements Result<E> {
            private final DaoContext ctx;
            private final Result<Record> recordIterator;
            private final Class<?> clazz;

            
            public ResultIteratorImpl(DaoContext ctx, Result<Record> recordIterator, Class<?> clazz) {
                this.ctx = ctx;
                this.recordIterator = recordIterator;
                this.clazz = clazz;
            }
            
            
            @Override
            public boolean hasNext() {
                return recordIterator.hasNext();
            }
        
            
            @Override
            public E next() {
                return ctx.getPropertiesMapper(clazz).fromValues(recordIterator.next());
            }
            
          
            @Override
            public void subscribe(Subscriber<? super E> subscriber) {
                recordIterator.subscribe(new MappingSubscriber<E>(ctx, clazz, subscriber));
            }
            
            private static final class MappingSubscriber<E> implements Subscriber<Record> {
                private final DaoContext ctx;
                private final Class<?> clazz;
                
                private Subscriber<? super E> subscriber;
                
                public MappingSubscriber(DaoContext ctx, Class<?> clazz, Subscriber<? super E> subscriber) {
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
                    subscriber.onNext(ctx.getPropertiesMapper(clazz).fromValues(record));
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
    }
}


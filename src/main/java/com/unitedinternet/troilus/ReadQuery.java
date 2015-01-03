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

import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Select.Selection;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.unitedinternet.troilus.Dao.ListRead;
import com.unitedinternet.troilus.Dao.ListReadWithColumns;
import com.unitedinternet.troilus.Dao.ListReadWithUnit;
import com.unitedinternet.troilus.Dao.SingleRead;
import com.unitedinternet.troilus.Dao.SingleReadWithColumns;
import com.unitedinternet.troilus.Dao.SingleReadWithUnit;




 

abstract class ReadQuery<Q extends ReadQuery<?>> extends AbstractQuery<Q> {
     
    
    public ReadQuery(Context ctx, QueryFactory queryFactory) {
        super(ctx, queryFactory);
    }
    
    
    
     
    static class SingleReadQuery extends ReadQuery<SingleReadQuery> implements SingleReadWithUnit<Optional<Record>> {
        private static final Logger LOG = LoggerFactory.getLogger(SingleReadQuery.class);

        private final ImmutableMap<String, Object> keyNameValuePairs;
        private final Optional<ImmutableSet<ColumnToFetch>> optionalColumnsToFetch;
         
        
        public SingleReadQuery(Context ctx, QueryFactory queryFactory, ImmutableMap<String, Object> keyNameValuePairs, Optional<ImmutableSet<ColumnToFetch>> optionalColumnsToFetch) {
            super(ctx, queryFactory);
            this.keyNameValuePairs = keyNameValuePairs;
            this.optionalColumnsToFetch = optionalColumnsToFetch;
        }
       
        
        @Override
        protected SingleReadQuery newQuery(Context newContext) {
            return newSingleReadQuery(newContext, keyNameValuePairs, optionalColumnsToFetch);
        }
        
        @Override
        public SingleRead<Optional<Record>> all() {
            return newSingleReadQuery(keyNameValuePairs, Optional.empty());
        }
        
        @Override
        public <E> SingleRead<Optional<E>> asEntity(Class<E> objectClass) {
            return newSingleEntityReadQuery(this, objectClass);
        }
        
        @Override
        public SingleReadWithUnit<Optional<Record>> column(String name) {
            return newSingleReadQuery(keyNameValuePairs, Immutables.merge(optionalColumnsToFetch, ColumnToFetch.create(name, false, false)));
        }
    
        @Override
        public SingleReadWithColumns<Optional<Record>> columnWithMetadata(String name) {
            return newSingleReadQuery(keyNameValuePairs, Immutables.merge(optionalColumnsToFetch, ColumnToFetch.create(name, true, true)));
        }
        
        @Override
        public SingleReadWithUnit<Optional<Record>> columns(String... names) {
            return columns(ImmutableSet.copyOf(names));
        }
        
        @Override 
        public SingleReadWithUnit<Optional<Record>> columns(ImmutableCollection<String> namesToRead) {
            return newSingleReadQuery(keyNameValuePairs, Immutables.merge(optionalColumnsToFetch, ColumnToFetch.create(namesToRead)));
        }
      
       
        
        @Override
        public Optional<Record> execute() {
            try {
                return executeAsync().get(Long.MAX_VALUE, TimeUnit.DAYS);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                throw Exceptions.unwrapIfNecessary(e);
            }
        }
        
        
        @Override
        public CompletableFuture<Optional<Record>> executeAsync() {
            
            Selection selection = select();
            
            if (optionalColumnsToFetch.isPresent()) {
                optionalColumnsToFetch.get().forEach(column -> column.accept(selection));
    
                // add key columns for paranoia checks
                keyNameValuePairs.keySet().forEach(name -> { if(!optionalColumnsToFetch.get().contains(name))  ColumnToFetch.create(name, false, false).accept(selection); });  
                
            } else {
                selection.all();
            }
            
            
            
            Select select = selection.from(getTable());
            Select.Where where = null;
            for (Clause whereClause : keyNameValuePairs.keySet().stream().map(name -> eq(name, bindMarker())).collect(Immutables.toSet())) {
                if (where == null) {
                    where = select.where(whereClause);
                } else {
                    where = where.and(whereClause);
                }
            }
    
            Statement statement = prepare(select).bind(keyNameValuePairs.values().toArray());
            
            
            return performAsync(statement)
                      .thenApply(resultSet -> {
                                                  Row row = resultSet.one();
                                                  if (row == null) {
                                                      return Optional.empty();
                                                      
                                                  } else {
                                                      Record record = new Record(getContext(), Result.newResult(resultSet), row);
                                                      
                                                      // paranoia check
                                                      keyNameValuePairs.forEach((name, value) -> { 
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
    }
    
    
    
    static class SingleEntityReadQuery<E> extends ReadQuery<SingleEntityReadQuery<E>> implements SingleRead<Optional<E>> {
        private final Class<?> clazz;
        private final SingleReadWithUnit<Optional<Record>> read;
        
        public SingleEntityReadQuery(Context ctx, QueryFactory queryFactory, SingleReadWithUnit<Optional<Record>> read, Class<?> clazz) {
            super(ctx, queryFactory);
            this.read = read;
            this.clazz = clazz;
        }
        

        @Override
        protected SingleEntityReadQuery<E> newQuery(Context newContext) {
            return newSingleEntityReadQuery(newContext, read, clazz);
        }
        
            
        @Override
        public Optional<E> execute() {
            try {
                return executeAsync().get(Long.MAX_VALUE, TimeUnit.DAYS);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                throw Exceptions.unwrapIfNecessary(e);
            } 
        }
        
        
        @Override
        public CompletableFuture<Optional<E>> executeAsync() {
            return read.executeAsync().thenApply(optionalRecord -> optionalRecord.map(record -> fromValues(clazz, record.getAccessor())));
        }        
    }
    
    
    
    
    static class ListReadQuery extends ReadQuery<ListReadQuery> implements ListReadWithUnit<RecordList> {
        private final ImmutableSet<Clause> clauses;
        private final Optional<ImmutableSet<ColumnToFetch>> columnsToFetch;
        private final Optional<Integer> optionalLimit;
        private final Optional<Boolean> optionalAllowFiltering;
        private final Optional<Integer> optionalFetchSize;
        private final Optional<Boolean> optionalDistinct;


        public ListReadQuery(Context ctx,
                             QueryFactory queryFactory,
                             ImmutableSet<Clause> clauses, 
                             Optional<ImmutableSet<ColumnToFetch>> columnsToFetch, 
                             Optional<Integer> optionalLimit, 
                             Optional<Boolean> optionalAllowFiltering,
                             Optional<Integer> optionalFetchSize,
                             Optional<Boolean> optionalDistinct) {
            super(ctx, queryFactory);
            this.clauses = clauses;
            this.columnsToFetch = columnsToFetch;
            this.optionalLimit = optionalLimit;
            this.optionalAllowFiltering = optionalAllowFiltering;
            this.optionalFetchSize = optionalFetchSize;
            this.optionalDistinct = optionalDistinct;
        }

        
        
        @Override
        protected ListReadQuery newQuery(Context newContext) {
            return newListReadQuery(clauses, 
                                    columnsToFetch,
                                    optionalLimit, 
                                    optionalAllowFiltering, 
                                    optionalFetchSize,
                                    optionalDistinct);
        }
        
        
        @Override
        public ListRead<RecordList> all() {
            return newListReadQuery(clauses, 
                                    Optional.empty(),
                                    optionalLimit, 
                                    optionalAllowFiltering, 
                                    optionalFetchSize,
                                    optionalDistinct);
        }
        
     
        @Override 
        public ListReadWithUnit<RecordList> columns(ImmutableCollection<String> namesToRead) {
            return newListReadQuery(clauses, 
                                    Immutables.merge(columnsToFetch, ColumnToFetch.create(namesToRead)), 
                                    optionalLimit, 
                                    optionalAllowFiltering, 
                                    optionalFetchSize,
                                    optionalDistinct);
        }
        
        
        
        @Override
        public ListReadWithUnit<RecordList> column(String name) {
            return newListReadQuery(clauses,  
                                    Immutables.merge(columnsToFetch, ColumnToFetch.create(name, false, false)), 
                                    optionalLimit, 
                                    optionalAllowFiltering,
                                    optionalFetchSize,
                                    optionalDistinct);
        }

        
        @Override
        public ListReadWithColumns<RecordList> columnWithMetadata(String name) {
            return newListReadQuery(clauses,  
                                    Immutables.merge(columnsToFetch, ColumnToFetch.create(name, true, true)), 
                                    optionalLimit, 
                                    optionalAllowFiltering,
                                    optionalFetchSize,
                                    optionalDistinct);
        }
        

        @Override
        public ListRead<RecordList> withLimit(int limit) {
            return newListReadQuery(clauses, 
                                    columnsToFetch, 
                                    Optional.of(limit), 
                                    optionalAllowFiltering, 
                                    optionalFetchSize,
                                    optionalDistinct);
        }
        
        @Override
        public ListRead<RecordList> withAllowFiltering() {
            return newListReadQuery(clauses, 
                                    columnsToFetch, 
                                    optionalLimit, 
                                    Optional.of(true), 
                                    optionalFetchSize,
                                    optionalDistinct);
        }

        @Override
        public ListRead<RecordList> withFetchSize(int fetchSize) {
            return newListReadQuery(clauses, 
                                    columnsToFetch, 
                                    optionalLimit,  
                                    optionalAllowFiltering, 
                                    Optional.of(fetchSize),
                                    optionalDistinct);
        }
        
        @Override
        public ListRead<RecordList> withDistinct() {
            return newListReadQuery(clauses, 
                                    columnsToFetch, 
                                    optionalLimit, 
                                    optionalAllowFiltering, 
                                    optionalFetchSize,
                                    Optional.of(true));
        }
        
       
        @Override
        public ListRead<Count> count() {
            return newCountReadQuery(clauses, 
                                     optionalLimit, 
                                     optionalAllowFiltering, 
                                     optionalFetchSize, 
                                     optionalDistinct);
        }
        
      
        @Override
        public <E> ListRead<EntityList<E>> asEntity(Class<E> objectClass) {
            return newListEntityReadQuery(this, objectClass) ;
        }

        
        @Override
        public ListReadWithUnit<RecordList> columns(String... names) {
            return columns(ImmutableSet.copyOf(names));
        }
        
        
                @Override
        public RecordList execute() {
            try {
                return executeAsync().get(Long.MAX_VALUE, TimeUnit.DAYS);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                throw Exceptions.unwrapIfNecessary(e);
            }
        }


        @Override
        public CompletableFuture<RecordList> executeAsync() {

            Select.Selection selection = select();

            optionalDistinct.ifPresent(distinct -> { if (distinct) selection.distinct(); });

            
            if (columnsToFetch.isPresent()) {
                columnsToFetch.get().forEach(column -> column.accept(selection));
            } else {
                selection.all();
            }
            
            Select select = selection.from(getTable());
            Select.Where where = null;
            for (Clause clause : clauses) {
                if (where == null) {
                    where = select.where(clause);
                } else {
                    where = where.and(clause);
                }
            }

            optionalLimit.ifPresent(limit -> select.limit(limit));
            optionalAllowFiltering.ifPresent(allowFiltering -> { if (allowFiltering)  select.allowFiltering(); });
            optionalFetchSize.ifPresent(fetchSize -> select.setFetchSize(fetchSize));
            
            return performAsync(select)
                      .thenApply(resultSet -> RecordList.newRecordList(getContext(), resultSet));
        }        
    }  
    
    
    
    static class ListEntityReadQuery<E> extends ReadQuery<ListEntityReadQuery<E>> implements ListRead<EntityList<E>> {
        private final ListRead<RecordList> read;
        private final Class<?> clazz;
        
        public ListEntityReadQuery(Context ctx, QueryFactory queryFactory, ListRead<RecordList> read, Class<?> clazz) {
            super(ctx, queryFactory);
            this.read = read;
            this.clazz = clazz;
        }

        @Override
        protected ListEntityReadQuery<E> newQuery(Context newContext) {
            return newListEntityReadQuery(newContext, read, clazz);
        }

        @Override
        public ListRead<EntityList<E>> withDistinct() {
            return newListEntityReadQuery(read.withDistinct(), clazz);
        }
        
        @Override
        public ListRead<EntityList<E>> withFetchSize(int fetchSize) {
            return newListEntityReadQuery(read.withFetchSize(fetchSize), clazz);
        }
        
        @Override
        public ListRead<EntityList<E>> withAllowFiltering() {
            return newListEntityReadQuery(read.withAllowFiltering(), clazz);
        }
        
        @Override
        public ListRead<EntityList<E>> withLimit(int limit) {
            return newListEntityReadQuery(read.withLimit(limit), clazz);
        }

        
        @Override
        public EntityList<E> execute() {
            try {
                return executeAsync().get(Long.MAX_VALUE, TimeUnit.DAYS);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                throw Exceptions.unwrapIfNecessary(e);
            }
        }
        
        
        @Override
        public CompletableFuture<EntityList<E>> executeAsync() {
            return read.executeAsync().thenApply(recordIterator -> EntityList.newEntityList(getContext(), recordIterator, clazz));
        }
    }
    
    
    
    
    static final class CountReadQuery extends ReadQuery<CountReadQuery> implements ListRead<Count> {
        private final ImmutableSet<Clause> clauses;
        private final Optional<Integer> optionalLimit;
        private final Optional<Boolean> optionalAllowFiltering;
        private final Optional<Integer> optionalFetchSize;
        private final Optional<Boolean> optionalDistinct;


        public CountReadQuery(Context ctx, 
                              QueryFactory queryFactory,
                              ImmutableSet<Clause> clauses, 
                              Optional<Integer> optionalLimit, 
                              Optional<Boolean> optionalAllowFiltering,
                              Optional<Integer> optionalFetchSize,
                              Optional<Boolean> optionalDistinct) {
            super(ctx, queryFactory);
            this.clauses = clauses;
            this.optionalLimit = optionalLimit;
            this.optionalAllowFiltering = optionalAllowFiltering;
            this.optionalFetchSize = optionalFetchSize;
            this.optionalDistinct = optionalDistinct;
        }

        
        @Override
        protected CountReadQuery newQuery(Context newContext) {
            return newCountReadQuery(newContext, 
                                     clauses, 
                                     optionalLimit, 
                                     optionalAllowFiltering, 
                                     optionalFetchSize,
                                     optionalDistinct);
        }
        
        @Override
        public ListRead<Count> withLimit(int limit) {
            return newCountReadQuery(clauses, 
                                     Optional.of(limit), 
                                     optionalAllowFiltering, 
                                     optionalFetchSize,
                                     optionalDistinct);
        }
        
        
        @Override
        public ListRead<Count> withAllowFiltering() {
            return newCountReadQuery(clauses, 
                                     optionalLimit, 
                                     Optional.of(true), 
                                     optionalFetchSize,
                                     optionalDistinct);
        }

        @Override
        public ListRead<Count> withFetchSize(int fetchSize) {
            return newCountReadQuery(clauses, 
                                     optionalLimit, 
                                     optionalAllowFiltering, 
                                     Optional.of(fetchSize),
                                     optionalDistinct);
        }
        
        @Override
        public ListRead<Count> withDistinct() {
            return newCountReadQuery(clauses, 
                                     optionalLimit, 
                                     optionalAllowFiltering, 
                                     optionalFetchSize,
                                     Optional.of(true));
        }
        
        
        @Override
        public Count execute() {
            try {
                return executeAsync().get(Long.MAX_VALUE, TimeUnit.DAYS);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                throw Exceptions.unwrapIfNecessary(e);
            }
        }


        public CompletableFuture<Count> executeAsync() {
            Select.Selection selection = select();

            optionalDistinct.ifPresent(distinct -> { if (distinct) selection.distinct(); });

     
            selection.countAll();
            
            Select select = selection.from(getTable());
            Select.Where where = null;
            for (Clause clause : clauses) {
                if (where == null) {
                    where = select.where(clause);
                } else {
                    where = where.and(clause);
                }
            }

            optionalLimit.ifPresent(limit -> select.limit(limit));
            optionalAllowFiltering.ifPresent(allowFiltering -> { if (allowFiltering)  select.allowFiltering(); });
            optionalFetchSize.ifPresent(fetchSize -> select.setFetchSize(fetchSize));
            
            return performAsync(select)
                               .thenApply(resultSet -> Count.newCountResult(resultSet));
        }        
    }    


    
    

    static class ColumnToFetch implements Consumer<Select.Selection> {
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
        public void accept(Select.Selection selection) {
             selection.column(name);

             if (isFetchTtl) {
                 selection.ttl(name);
             }

             if (isFetchWritetime) {
                 selection.writeTime(name);
             }
        }
    }
}
 

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

import java.util.Iterator;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.reactivestreams.Publisher;

import com.datastax.driver.core.ResultSet;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;




/**
 * Read query implementation
 */
class SingleReadQuery extends AbstractQuery<SingleReadQuery> implements SingleReadWithUnit<Record, Record> {

    private final ReadQueryData data;
    
    /**
     * @param ctx   the context 
     * @param data  the data
     */
    SingleReadQuery(Context ctx, ReadQueryData data) {
        super(ctx);
        this.data = data;
    }
   
    
    ////////////////////
    // factory methods
    
    @Override
    protected SingleReadQuery newQuery(Context newContext) {
        return new SingleReadQuery(newContext, data);
    }

    private SingleReadQuery newQuery(ReadQueryData data) {
        return new SingleReadQuery(getContext(), data);
    }

    //
    ////////////////////


    
    @Override
    public SingleReadQuery all() {
        return newQuery(data.columnsToFetch(ImmutableMap.<String, Boolean>of()));
    }
    
   
    @Override
    public SingleReadQuery column(String name) {
        return newQuery(data.columnsToFetch(Immutables.join(data.getColumnsToFetch(), name, false)));
    }

    @Override
    public SingleReadQuery columnWithMetadata(String name) {
        return newQuery(data.columnsToFetch(Immutables.join(data.getColumnsToFetch(), name, true)));
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
    public SingleReadQuery column(ColumnName<?> name) {
        return column(name.getName());
    }

    @Override
    public SingleReadQuery columnWithMetadata(ColumnName<?> name) {
        return column(name.getName());
    }
    
    @Override
    public SingleReadQuery columns(ColumnName<?>... names) {
        final List<String> ns = Lists.newArrayList();
        for (ColumnName<?> name : names) {
            ns.add(name.getName());
        }
        return columns(ImmutableList.copyOf(ns));
    }
  

    @Override
    public <E> SingleEntityReadQuery<E> asEntity(Class<E> objectClass) {
        return new SingleEntityReadQuery<E>(getContext(), this, objectClass);
    }
  
  
    @Override
    public CompletableFuture<Optional<Record>> executeAsync() {
        return new ListReadQuery(getContext(), data).executeAsync()
                                                    .thenApply(resultList -> new SingleEntryResultList<>(resultList))
                                                    .thenApply(records -> {
                                                                            Iterator<Record> it = records.iterator();
                                                                            if (it.hasNext()) {
                                                                                Record record = it.next();
                                                                                 
                                                                                if (it.hasNext()) {
                                                                                    throw new TooManyResultsException(records, "more than one record exists");
                                                                                }
                                                                                                                               
                                                                                return Optional.of(record);
                                                                            } else {
                                                                                return Optional.empty();
                                                                            }                                                                                                       
                                                                           });
    }
    
    
    @Override
    public Publisher<Record> executeRx() {
        return new ResultListPublisher<Record>(new ListReadQuery(getContext(), data).executeAsync()
                                                                                    .thenApply(resultList -> new SingleEntryResultList<>(resultList)));
    }
    
    
    /**
     * Entity read query 
     * @param <E> the entity type
     */
    static class SingleEntityReadQuery<E> extends AbstractQuery<SingleEntityReadQuery<E>> implements SingleRead<E, E> {
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
        public CompletableFuture<Optional<E>> executeAsync() {
            return query.executeAsync()
                        .thenApply(optionalRecord -> optionalRecord.map(record -> getBeanMapper().fromValues(clazz, RecordImpl.toPropertiesSource(record), getCatalog().getColumnNames(query.data.getTablename())))); 
        }
        
        @Override
        public Publisher<E> executeRx() {
            return new ResultListPublisher<E>(new ListReadQuery(getContext(), query.data).asEntity(clazz)
                                                                                         .executeAsync()
                                                                                         .thenApply(resultList -> new SingleEntryResultList<>(resultList)));
        }
    }
    
    
    
    private static final class SingleEntryResultList<T> extends ResultListAdapter<T> {
        
        public SingleEntryResultList(ResultList<T> list) {
           super(list);
        }
        
        @Override
        public FetchingIterator<T> iterator() {
            return new SingleFetchingIterator<>(super.iterator());
        }

        
        private final class SingleFetchingIterator<E> implements FetchingIterator<E> {
            
            private final FetchingIterator<E> iterator;
            
            public SingleFetchingIterator(FetchingIterator<E> iterator) {
                this.iterator = iterator;
            }
            
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }
            
            @Override
            public E next() {
                E element = iterator.next();
                    
                if (iterator.hasNext()) {
                    throw new TooManyResultsException(SingleEntryResultList.this, "more than one record exists");
                }
                    
                return element;
            }
            
            @Override
            public int getAvailableWithoutFetching() {
                return iterator.getAvailableWithoutFetching();
            }
            
            @Override
            public CompletableFuture<ResultSet> fetchMoreResultsAsync() {
                return iterator.fetchMoreResultsAsync();
            }
            
            @Override
            public boolean isFullyFetched() {
                return iterator.isFullyFetched();
            }
        }
    }
}
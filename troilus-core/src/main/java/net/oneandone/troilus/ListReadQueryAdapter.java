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


import java.util.concurrent.CompletableFuture;

import org.reactivestreams.Publisher;

import net.oneandone.troilus.AbstractQuery;
import net.oneandone.troilus.Context;
import net.oneandone.troilus.Count;
import net.oneandone.troilus.ListReadQuery;
import net.oneandone.troilus.ColumnName;
import net.oneandone.troilus.ListReadQuery.CountReadQuery;
import net.oneandone.troilus.ListReadQuery.ListEntityReadQuery;


 

/**
 * Java8 adapter of a ListReadQuery
 */
class ListReadQueryAdapter extends AbstractQuery<ListReadQueryAdapter> implements ListReadWithUnit<ResultList<Record>, Record> {
    
    private final ListReadQuery query;

    
    /**
     * @param ctx    the context
     * @param query  the underyling query
     */
    ListReadQueryAdapter(Context ctx, ListReadQuery query) {
        super(ctx);
        this.query = query;
    }

    
    ////////////////////
    // factory methods
    
    @Override
    protected ListReadQueryAdapter newQuery(Context newContext) {
        return new ListReadQueryAdapter(newContext, query.newQuery(newContext));
    }

    private ListReadQueryAdapter newQuery(ListReadQuery query) {
        return new ListReadQueryAdapter(getContext(), query.newQuery(getContext()));
    }

    // 
    ////////////////////

    
    @Override
    public ListReadQueryAdapter all() {
        return new ListReadQueryAdapter(getContext(), query.all());
    }
    
    @Override
    public ListReadQueryAdapter column(String name) {
        return newQuery(query.column(name));
    }
    
    @Override
    public ListReadQueryAdapter columnWithMetadata(String name) {
        return newQuery(query.column(name));
    }
    
    @Override
    public ListReadWithUnit<ResultList<Record>, Record> columns(String... names) {
        return newQuery(query.columns(names));
    }
    
    @Override
    public ListReadWithUnit<ResultList<Record>, Record> column(ColumnName<?> name) {
        return newQuery(query.column(name));
    }
    
    @Override
    public ListReadWithUnit<ResultList<Record>, Record> columnWithMetadata(ColumnName<?> name) {
        return newQuery(query.columnWithMetadata(name));
    }
    
    @Override
    public ListReadWithUnit<ResultList<Record>, Record> columns(ColumnName<?>... names) {
        return newQuery(query.columns(names));
    }

    @Override
    public ListReadQueryAdapter withLimit(int limit) {
        return newQuery(query.withLimit(limit));
    }
    
    @Override
    public ListReadQueryAdapter withAllowFiltering() {
        return newQuery(query.withAllowFiltering());
    }

    @Override
    public ListReadQueryAdapter withFetchSize(int fetchSize) {
        return newQuery(query.withFetchSize(fetchSize));
    }
    
    @Override
    public ListReadQueryAdapter withDistinct() {
        return newQuery(query.withDistinct());
    }
    
    @Override
    public ListRead<Count, Integer> count() {
        return new CountReadQueryAdapter(getContext(), query.count());
    }
    
    @Override
    public <E> ListEntityReadQueryAdapter<E> asEntity(Class<E> objectClass) {
        return new ListEntityReadQueryAdapter<>(getContext(), query.asEntity(objectClass)) ;
    }

    @Override
    public ResultList<Record> execute() {
        return CompletableFutures.getUninterruptibly(executeAsync());
    }
    
    @Override
    public CompletableFuture<ResultList<Record>> executeAsync() {
        return CompletableFutures.toCompletableFuture(query.executeAsync())
                                 .thenApply(recordList -> DaoImpl.RecordListAdapter.convertFromJava7(recordList));
    }        
    
    @Override
    public Publisher<Record> executeRx() {
        Publisher<net.oneandone.troilus.java7.Record> publisher = query.executeRx();
        return new RecordMappingPublisher(publisher);
    }
    
    

    /**
     * Java8 adapter of a ListEntityReadQuery
     */
    private class ListEntityReadQueryAdapter<E> extends AbstractQuery<ListEntityReadQueryAdapter<E>> implements ListRead<ResultList<E>, E> {
         
        private final ListEntityReadQuery<E> query;
        
        
        /**
         * @param ctx    the context
         * @param query  the underlying query
         */
        ListEntityReadQueryAdapter(Context ctx, ListEntityReadQuery<E> query) {
            super(ctx);
            this.query = query;
        }

        @Override
        protected ListEntityReadQueryAdapter<E> newQuery(Context newContext) {
            return new ListEntityReadQueryAdapter<>(newContext, query.newQuery(newContext));
        }

        @Override
        public ListRead<ResultList<E>, E> withDistinct() {
            return new ListEntityReadQueryAdapter<>(getContext(), query.withDistinct());
        }
        
        @Override
        public ListRead<ResultList<E>, E> withFetchSize(int fetchSize) {
            return new ListEntityReadQueryAdapter<>(getContext(), query.withFetchSize(fetchSize));
        }
        
        @Override
        public ListRead<ResultList<E>, E> withAllowFiltering() {
            return new ListEntityReadQueryAdapter<>(getContext(), query.withAllowFiltering());
        }
        
        @Override
        public ListRead<ResultList<E>, E> withLimit(int limit) {
            return new ListEntityReadQueryAdapter<>(getContext(), query.withLimit(limit));
        }

        @Override
        public ResultList<E> execute() {
            return CompletableFutures.getUninterruptibly(executeAsync());
        }

        @Override
        public CompletableFuture<ResultList<E>> executeAsync() {
            return CompletableFutures.toCompletableFuture(query.executeAsync())
                                     .thenApply(entityList -> new DaoImpl.EntityListAdapter<>(entityList));
        }
        
        @Override
        public Publisher<E> executeRx() {
            return query.executeRx();
        }
    }
    
    
     

    /**
     * Java8 adapter of a CountReadQuery
     */
    private static class CountReadQueryAdapter extends AbstractQuery<CountReadQueryAdapter> implements ListRead<Count, Integer> {

        private final CountReadQuery query;
    
        /**
         * @param ctx     the context
         * @param query   the query
         */
        CountReadQueryAdapter(Context ctx, CountReadQuery query) {
            super(ctx);
            this.query = query;
        }
        
        @Override
        protected CountReadQueryAdapter newQuery(Context newContext) {
            return new CountReadQueryAdapter(newContext, query.newQuery(newContext));
        }
        
        @Override
        public ListRead<Count, Integer> withLimit(int limit) {
            return new CountReadQueryAdapter(getContext(), query.withLimit(limit)); 
        }
        
        @Override
        public ListRead<Count, Integer> withAllowFiltering() {
            return new CountReadQueryAdapter(getContext(), query.withAllowFiltering());
        }
    
        @Override
        public ListRead<Count, Integer> withFetchSize(int fetchSize) {
            return new CountReadQueryAdapter(getContext(), query.withFetchSize(fetchSize));

        }
    
        @Override
        public ListRead<Count, Integer> withDistinct() {
            return new CountReadQueryAdapter(getContext(), query.withDistinct());
        }
        
        @Override
        public Count execute() {
            return CompletableFutures.getUninterruptibly(executeAsync());
        }

        @Override
        public Publisher<Integer> executeRx() {
            // TODO Auto-generated method stub
            return null;
        }
        
        @Override
        public CompletableFuture<Count> executeAsync() {
            return CompletableFutures.toCompletableFuture(query.executeAsync());
        }        
    }  
}
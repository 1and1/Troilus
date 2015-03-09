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

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.reactivestreams.Publisher;

import net.oneandone.troilus.AbstractQuery;
import net.oneandone.troilus.Context;
import net.oneandone.troilus.ColumnName;
import net.oneandone.troilus.SingleReadQuery;
import net.oneandone.troilus.SingleReadQuery.SingleEntityReadQuery;


 
/**
 * Java8 adapter of a SingleReadQuery
 */
class SingleReadQueryAdapter extends AbstractQuery<SingleReadQueryAdapter> implements SingleReadWithUnit<Optional<Record>, Record> {
    
    private final SingleReadQuery query;
     
    
    /**
     * @param ctx     the context
     * @param query   the underlying query
     */
     SingleReadQueryAdapter(Context ctx, SingleReadQuery query) {
        super(ctx);
        this.query = query;
    }
   
    
    ////////////////////
    // factory methods
     
    @Override
    protected SingleReadQueryAdapter newQuery(Context newContext) {
        return new SingleReadQueryAdapter(newContext, query.newQuery(newContext));
    }
    
    private SingleReadQueryAdapter newQuery(SingleReadQuery query) {
        return new SingleReadQueryAdapter(getContext(), query.newQuery(getContext()));
    }
    
    // 
    ////////////////////
    

    
    @Override
    public SingleRead<Optional<Record>, Record> all() {
        return newQuery(query.all());
    }
    
    @Override
    public <E> SingleEntityReadQueryAdapter<E> asEntity(Class<E> objectClass) {
        return new SingleEntityReadQueryAdapter<>(getContext(), query.asEntity(objectClass));
    }
    
    @Override
    public SingleReadQueryAdapter column(String name) {
        return newQuery(query.column(name));
    }

    @Override
    public SingleReadQueryAdapter columnWithMetadata(String name) {
        return newQuery(query.columnWithMetadata(name));
    }
    
    @Override
    public SingleReadQueryAdapter columns(String... names) {
        return newQuery(query.columns(names));
    }

    @Override
    public SingleReadWithColumns<Optional<Record>, Record> column(ColumnName<?> name) {
        return newQuery(query.column(name));
    }

    @Override
    public SingleReadWithColumns<Optional<Record>, Record> columnWithMetadata(ColumnName<?> name) {
        return newQuery(query.columnWithMetadata(name));
    }
    
    @Override
    public SingleReadWithColumns<Optional<Record>, Record> columns(ColumnName<?>... names) {
        return newQuery(query.columns(names));
    }
    
    @Override
    public Optional<Record> execute() {
        return CompletableFutures.getUninterruptibly(executeAsync());
    }
    
    @Override
    public CompletableFuture<Optional<Record>> executeAsync() {
        return CompletableFutures.toCompletableFuture(query.executeAsync())
                            .thenApply(record -> (record == null) ? Optional.empty() : Optional.of(RecordAdapter.convertFromJava7(record))); 
    }
    
    @Override
    public Publisher<Record> executeRx() {
        Publisher<net.oneandone.troilus.java7.Record> publisher = query.executeRx();
        return new RecordMappingPublisher(publisher);
    }
    
    
    
    

    /**
     * Java8 adapter of a SingleEntityReadQuery
     */
    private static class SingleEntityReadQueryAdapter<E> extends AbstractQuery<SingleEntityReadQueryAdapter<E>> implements SingleRead<Optional<E>, E> {
        
        private final SingleEntityReadQuery<E> query;
        
        /**
         * @param ctx    the context 
         * @param query  the underlying query
         */
        SingleEntityReadQueryAdapter(Context ctx, SingleEntityReadQuery<E> query) {
            super(ctx);
            this.query = query;
        }
        
        @Override
        protected SingleEntityReadQueryAdapter<E> newQuery(Context newContext) {
            return new SingleEntityReadQueryAdapter<>(newContext, query.newQuery(newContext)); 
        }
        
        @Override
        public Optional<E> execute() {
            return CompletableFutures.getUninterruptibly(executeAsync());
        }

        @Override
        public CompletableFuture<Optional<E>> executeAsync() {
            return CompletableFutures.toCompletableFuture(query.executeAsync())
                            .thenApply(entity -> Optional.ofNullable(entity));
        }   
        
        @Override
        public Publisher<E> executeRx() {
            return query.executeRx();
        }
    }
}

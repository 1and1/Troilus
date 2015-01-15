/*
 * Copyright (c) 2015 1&1 Internet AG, Germany, http://www.1und1.de
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

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import com.unitedinternet.troilus.Dao.SingleRead;
import com.unitedinternet.troilus.Dao.SingleReadWithColumns;
import com.unitedinternet.troilus.Dao.SingleReadWithUnit;
import com.unitedinternet.troilus.SingleReadQuery.SingleEntityReadQuery;


 
/**
 * Java8 adapter of a SingleReadQuery
 */
class SingleReadQueryAdapter extends AbstractQuery<SingleReadQueryAdapter> implements SingleReadWithUnit<Optional<Record>> {
    
    private final SingleReadQuery query;
     
    
    /**
     * @param ctx     the context
     * @param query   the underlying query
     */
     SingleReadQueryAdapter(Context ctx, SingleReadQuery query) {
        super(ctx);
        this.query = query;
    }
   
    
    @Override
    protected SingleReadQueryAdapter newQuery(Context newContext) {
        return new SingleReadQueryAdapter(newContext, query.newQuery(newContext));
    }
    
    @Override
    public SingleRead<Optional<Record>> all() {
        return new SingleReadQueryAdapter(getContext(), query.all());
    }
    
    @Override
    public <E> SingleEntityReadQueryAdapter<E> asEntity(Class<E> objectClass) {
        return new SingleEntityReadQueryAdapter<>(getContext(), query.asEntity(objectClass));
    }
    
    @Override
    public SingleReadQueryAdapter column(String name) {
        return new SingleReadQueryAdapter(getContext(), query.column(name));
    }

    @Override
    public SingleReadQueryAdapter columnWithMetadata(String name) {
        return new SingleReadQueryAdapter(getContext(), query.columnWithMetadata(name));
    }
    
    @Override
    public SingleReadQueryAdapter columns(String... names) {
        return new SingleReadQueryAdapter(getContext(), query.columns(names));
    }

    @Override
    public SingleReadWithColumns<Optional<Record>> column(Name<?> name) {
        return new SingleReadQueryAdapter(getContext(), query.column(name));
    }

    @Override
    public SingleReadWithColumns<Optional<Record>> columnWithMetadata(Name<?> name) {
        return new SingleReadQueryAdapter(getContext(), query.columnWithMetadata(name));
    }
    
    @Override
    public SingleReadWithColumns<Optional<Record>> columns(Name<?>... names) {
        return new SingleReadQueryAdapter(getContext(), query.columns(names));
    }
    
    @Override
    public CompletableFuture<Optional<Record>> executeAsync() {
        return new ListenableToCompletableFutureAdapter<>(query.executeAsync())
                            .thenApply(record -> (record == null) ? Optional.empty() : Optional.of(new RecordAdapter(record))); 
    }
    
    
    
    
    

    /**
     * Java8 adapter of a SingleEntityReadQuery
     */
    private static class SingleEntityReadQueryAdapter<E> extends AbstractQuery<SingleEntityReadQueryAdapter<E>> implements SingleRead<Optional<E>> {
        
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
        public CompletableFuture<Optional<E>> executeAsync() {
            return new ListenableToCompletableFutureAdapter<>(query.executeAsync())
                            .thenApply(entity -> Optional.ofNullable(entity));
        }        
    }
}

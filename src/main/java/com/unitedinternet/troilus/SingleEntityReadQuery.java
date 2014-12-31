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


import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.unitedinternet.troilus.Dao.SingleRead;



class SingleEntityReadQuery<E> extends AbstractQuery<SingleRead<Optional<E>>> implements SingleRead<Optional<E>> {
    private final QueryFactory queryFactory;
    private final SingleRead<Optional<Record>> read;
    private final Class<?> clazz;
    
    public SingleEntityReadQuery(Context ctx, QueryFactory queryFactory, SingleRead<Optional<Record>> read, Class<?> clazz) {
        super(ctx);
        this.queryFactory = queryFactory;
        this.read = read;
        this.clazz = clazz;
    }
    

    @Override
    protected SingleRead<Optional<E>> newQuery(Context newContext) {
        return queryFactory.newSingleSelection(newContext, read, clazz);
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
        return read.executeAsync().thenApply(optionalRecord -> optionalRecord.map(record -> getContext().fromValues(clazz, record.getAccessor())));
    }        
}
 

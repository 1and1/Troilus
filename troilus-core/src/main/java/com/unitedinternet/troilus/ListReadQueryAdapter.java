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


import java.util.concurrent.CompletableFuture;


import com.unitedinternet.troilus.Dao.EntityList;
import com.unitedinternet.troilus.Dao.ListRead;
import com.unitedinternet.troilus.Dao.ListReadWithUnit;
import com.unitedinternet.troilus.Dao.RecordList;
import com.unitedinternet.troilus.ListReadQuery.CountReadQuery;
import com.unitedinternet.troilus.ListReadQuery.ListEntityReadQuery;


 

class ListReadQueryAdapter extends AbstractQuery<ListReadQueryAdapter> implements ListReadWithUnit<RecordList> {
    
    private final ListReadQuery query;

    
    public ListReadQueryAdapter(Context ctx, ListReadQuery query) {
        super(ctx);
        this.query = query;
    }
    
    @Override
    protected ListReadQueryAdapter newQuery(Context newContext) {
        return new ListReadQueryAdapter(newContext, query.newQuery(newContext));
    }
    
    @Override
    public ListReadQueryAdapter all() {
        return new ListReadQueryAdapter(getContext(), query.all());
    }
    
    @Override
    public ListReadQueryAdapter column(String name) {
        return new ListReadQueryAdapter(getContext(), query.column(name));
    }
    
    @Override
    public ListReadQueryAdapter columnWithMetadata(String name) {
        return new ListReadQueryAdapter(getContext(), query.column(name));
    }
    
    @Override
    public ListReadWithUnit<RecordList> columns(String... names) {
        return new ListReadQueryAdapter(getContext(), query.columns(names));
    }
    
    
    @Override
    public ListReadWithUnit<RecordList> column(Name<?> name) {
        return new ListReadQueryAdapter(getContext(), query.column(name));
    }
    
    @Override
    public ListReadWithUnit<RecordList> columnWithMetadata(Name<?> name) {
        return new ListReadQueryAdapter(getContext(), query.columnWithMetadata(name));
    }
    
    @Override
    public ListReadWithUnit<RecordList> columns(Name<?>... names) {
        return new ListReadQueryAdapter(getContext(), query.columns(names));
    }

    @Override
    public ListReadQueryAdapter withLimit(int limit) {
        return new ListReadQueryAdapter(getContext(), query.withLimit(limit));
    }
    
    @Override
    public ListReadQueryAdapter withAllowFiltering() {
        return new ListReadQueryAdapter(getContext(), query.withAllowFiltering());
    }

    @Override
    public ListReadQueryAdapter withFetchSize(int fetchSize) {
        return new ListReadQueryAdapter(getContext(), query.withFetchSize(fetchSize));
    }
    
    @Override
    public ListReadQueryAdapter withDistinct() {
        return new ListReadQueryAdapter(getContext(), query.withDistinct());
    }
    
    @Override
    public ListRead<Count> count() {
        return new CountReadQueryAdapter(getContext(), query.count());
    }
    
    @Override
    public <E> ListEntityReadQueryAdapter<E> asEntity(Class<E> objectClass) {
        return new ListEntityReadQueryAdapter<>(getContext(), query.asEntity(objectClass)) ;
    }


    @Override
    public CompletableFuture<RecordList> executeAsync() {
        return new ListenableToCompletableFutureAdapter<>(query.executeAsync())
                            .thenApply(recordList -> new DaoImpl.RecordListAdapter(recordList));
    }        
    
    
       
     private class ListEntityReadQueryAdapter<E> extends AbstractQuery<ListEntityReadQueryAdapter<E>> implements ListRead<EntityList<E>> {
         
        private final ListEntityReadQuery<E> query;
        
        public ListEntityReadQueryAdapter(Context ctx, ListEntityReadQuery<E> query) {
            super(ctx);
            this.query = query;
        }

        @Override
        protected ListEntityReadQueryAdapter<E> newQuery(Context newContext) {
            return new ListEntityReadQueryAdapter<>(newContext, query.newQuery(newContext));
        }

        @Override
        public ListRead<EntityList<E>> withDistinct() {
            return new ListEntityReadQueryAdapter<>(getContext(), query.withDistinct());
        }
        
        @Override
        public ListRead<EntityList<E>> withFetchSize(int fetchSize) {
            return new ListEntityReadQueryAdapter<>(getContext(), query.withFetchSize(fetchSize));
        }
        
        @Override
        public ListRead<EntityList<E>> withAllowFiltering() {
            return new ListEntityReadQueryAdapter<>(getContext(), query.withAllowFiltering());
        }
        
        @Override
        public ListRead<EntityList<E>> withLimit(int limit) {
            return new ListEntityReadQueryAdapter<>(getContext(), query.withLimit(limit));
        }

        @Override
        public CompletableFuture<EntityList<E>> executeAsync() {
            return new ListenableToCompletableFutureAdapter<>(query.executeAsync())
                            .thenApply(entityList -> new DaoImpl.EntityListAdapter<>(entityList));
        }
    }
    
    
     
   
    
    private static class CountReadQueryAdapter extends AbstractQuery<CountReadQueryAdapter> implements ListRead<Count> {
        
        private final CountReadQuery query;
    
    
        public CountReadQueryAdapter(Context ctx, CountReadQuery query) {
            super(ctx);
            this.query = query;
        }
    
        
        @Override
        protected CountReadQueryAdapter newQuery(Context newContext) {
            return new CountReadQueryAdapter(newContext, query.newQuery(newContext));
        }
        
        @Override
        public ListRead<Count> withLimit(int limit) {
            return new CountReadQueryAdapter(getContext(), query.withLimit(limit)); 
        }
        
        @Override
        public ListRead<Count> withAllowFiltering() {
            return new CountReadQueryAdapter(getContext(), query.withAllowFiltering());
        }
    
        @Override
        public ListRead<Count> withFetchSize(int fetchSize) {
            return new CountReadQueryAdapter(getContext(), query.withFetchSize(fetchSize));

        }
        
        @Override
        public ListRead<Count> withDistinct() {
            return new CountReadQueryAdapter(getContext(), query.withDistinct());
        }
    
        public CompletableFuture<Count> executeAsync() {
            return new ListenableToCompletableFutureAdapter<>(query.executeAsync());
        }        
    }  
}
    
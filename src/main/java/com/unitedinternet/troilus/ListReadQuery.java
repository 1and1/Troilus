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

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.Select;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.unitedinternet.troilus.Dao.ListRead;
import com.unitedinternet.troilus.Dao.ListReadWithUnit;




 

public class ListReadQuery extends AbstractQuery<ListReadQuery> implements ListReadWithUnit<RecordList> {
    final ImmutableSet<Clause> whereClauses;
    final Optional<ImmutableMap<String, Boolean>> columnsToFetch;
    final Optional<Integer> optionalLimit;
    final Optional<Boolean> optionalAllowFiltering;
    final Optional<Integer> optionalFetchSize;
    final Optional<Boolean> optionalDistinct;

    
    public ListReadQuery(Context ctx,
                         QueryFactory queryFactory,
                         ImmutableSet<Clause> whereClauses, 
                         Optional<ImmutableMap<String, Boolean>> columnsToFetch, 
                         Optional<Integer> optionalLimit, 
                         Optional<Boolean> optionalAllowFiltering,
                         Optional<Integer> optionalFetchSize,
                         Optional<Boolean> optionalDistinct) {
        super(ctx, queryFactory);
        this.whereClauses = whereClauses;
        this.columnsToFetch = columnsToFetch;
        this.optionalLimit = optionalLimit;
        this.optionalAllowFiltering = optionalAllowFiltering;
        this.optionalFetchSize = optionalFetchSize;
        this.optionalDistinct = optionalDistinct;
    }

    
    
    @Override
    protected ListReadQuery newQuery(Context newContext) {
        return newListReadQuery(whereClauses, 
                                columnsToFetch,
                                optionalLimit, 
                                optionalAllowFiltering, 
                                optionalFetchSize,
                                optionalDistinct);
    }
    
    
    @Override
    public ListReadQuery all() {
        return newListReadQuery(whereClauses, 
                                Optional.empty(),
                                optionalLimit, 
                                optionalAllowFiltering, 
                                optionalFetchSize,
                                optionalDistinct);
    }
    
 
    @Override 
    public ListReadQuery columns(ImmutableCollection<String> namesToRead) {
        ListReadQuery read = this;
        for (String columnName : namesToRead) {
            read = read.column(columnName);
        }
        return read;
    }
    
    
    
    @Override
    public ListReadQuery column(String name) {
        return newListReadQuery(whereClauses,  
                                Immutables.merge(columnsToFetch, name, false), 
                                optionalLimit, 
                                optionalAllowFiltering,
                                optionalFetchSize,
                                optionalDistinct);
    }

    
    @Override
    public ListReadQuery columnWithMetadata(String name) {
        return newListReadQuery(whereClauses,  
                                Immutables.merge(columnsToFetch, name, true), 
                                optionalLimit, 
                                optionalAllowFiltering,
                                optionalFetchSize,
                                optionalDistinct);
    }
    

    @Override
    public ListReadQuery withLimit(int limit) {
        return newListReadQuery(whereClauses, 
                                columnsToFetch, 
                                Optional.of(limit), 
                                optionalAllowFiltering, 
                                optionalFetchSize,
                                optionalDistinct);
    }
    
    @Override
    public ListReadQuery withAllowFiltering() {
        return newListReadQuery(whereClauses, 
                                columnsToFetch, 
                                optionalLimit, 
                                Optional.of(true), 
                                optionalFetchSize,
                                optionalDistinct);
    }

    @Override
    public ListReadQuery withFetchSize(int fetchSize) {
        return newListReadQuery(whereClauses, 
                                columnsToFetch, 
                                optionalLimit,  
                                optionalAllowFiltering, 
                                Optional.of(fetchSize),
                                optionalDistinct);
    }
    
    @Override
    public ListReadQuery withDistinct() {
        return newListReadQuery(whereClauses, 
                                columnsToFetch, 
                                optionalLimit, 
                                optionalAllowFiltering, 
                                optionalFetchSize,
                                Optional.of(true));
    }
    
   
    @Override
    public ListRead<Count> count() {
        return new CountReadQuery(getContext(),
                                  this,
                                  whereClauses, 
                                  optionalLimit, 
                                  optionalAllowFiltering, 
                                  optionalFetchSize, 
                                  optionalDistinct);
    }
    
  
    @Override
    public <E> ListEntityReadQuery<E> asEntity(Class<E> objectClass) {
        return new ListEntityReadQuery(getContext(), this, this, objectClass) ;
    }

    
    @Override
    public ListReadWithUnit<RecordList> columns(String... names) {
        return columns(ImmutableSet.copyOf(names));
    }
    
    
    

    @Override
    public CompletableFuture<RecordList> executeAsync() {

        Select.Selection selection = select();

        optionalDistinct.ifPresent(distinct -> { if (distinct) selection.distinct(); });

        
        if (columnsToFetch.isPresent()) {
            columnsToFetch.get().forEach((columnName, withMetaData) -> selection.column(columnName));
            columnsToFetch.get().entrySet()
                                .stream()
                                .filter(entry -> entry.getValue())
                                .forEach(entry -> { selection.ttl(entry.getKey()); selection.writeTime(entry.getKey()); });
        } else {
            selection.all();
        }
        
        Select select = selection.from(getTable());
        
        whereClauses.forEach(whereClause -> select.where(whereClause));

        optionalLimit.ifPresent(limit -> select.limit(limit));
        optionalAllowFiltering.ifPresent(allowFiltering -> { if (allowFiltering)  select.allowFiltering(); });
        optionalFetchSize.ifPresent(fetchSize -> select.setFetchSize(fetchSize));
        
        return performAsync(select).thenApply(resultSet -> newRecordList(resultSet));
    }        
    
     
    
    private static class ListEntityReadQuery<E> extends AbstractQuery<ListEntityReadQuery<E>> implements ListRead<EntityList<E>> {
        private final ListReadQuery read;
        private final Class<E> clazz;
        
        public ListEntityReadQuery(Context ctx, QueryFactory queryFactory, ListReadQuery read, Class<E> clazz) {
            super(ctx, queryFactory);
            this.read = read;
            this.clazz = clazz;
        }

        @Override
        protected ListEntityReadQuery<E> newQuery(Context newContext) {
            return newListReadQuery(newContext,
                                    read.whereClauses,
                                    read.columnsToFetch, 
                                    read.optionalLimit, 
                                    read.optionalAllowFiltering,
                                    read.optionalFetchSize,
                                    read.optionalDistinct).asEntity(clazz);
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
        public CompletableFuture<EntityList<E>> executeAsync() {
            return read.executeAsync().thenApply(recordList -> newEntityList(recordList, clazz));
        }
    }
}
    
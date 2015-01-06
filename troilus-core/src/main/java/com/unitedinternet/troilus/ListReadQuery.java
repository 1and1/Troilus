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

import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.Select;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableSet;
import com.unitedinternet.troilus.Dao.ListRead;
import com.unitedinternet.troilus.Dao.ListReadWithUnit;




 

class ListReadQuery extends AbstractQuery<ListReadQuery> implements ListReadWithUnit<RecordList> {
    
    final ListReadQueryData data;

    
    public ListReadQuery(Context ctx, ListReadQueryData data) {
        super(ctx);
        this.data = data;
    }

    
    
    @Override
    protected ListReadQuery newQuery(Context newContext) {
        return new ListReadQuery(newContext, data);
    }
    
    
    @Override
    public ListReadQuery all() {
        return new ListReadQuery(getContext(), data.withColumnsToFetch(Optional.empty()));
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
        return new ListReadQuery(getContext(), data.withColumnsToFetch(Immutables.merge(data.getColumnsToFetch(), name, false)));
    }

    
    @Override
    public ListReadQuery columnWithMetadata(String name) {
        return new ListReadQuery(getContext(), data.withColumnsToFetch(Immutables.merge(data.getColumnsToFetch(), name, true)));
    }
    

    @Override
    public ListReadQuery withLimit(int limit) {
        return new ListReadQuery(getContext(), data.withLimit(Optional.of(limit)));
    }
    
    @Override
    public ListReadQuery withAllowFiltering() {
        return new ListReadQuery(getContext(), data.withAllowFiltering(Optional.of(true)));
    }

    @Override
    public ListReadQuery withFetchSize(int fetchSize) {
        return new ListReadQuery(getContext(), data.withFetchSize(Optional.of(fetchSize)));
    }
    
    @Override
    public ListReadQuery withDistinct() {
        return new ListReadQuery(getContext(), data.withDistinct(Optional.of(true)));
    }
    
   
    @Override
    public ListRead<Count> count() {
        return new CountReadQuery(getContext(), new CountReadQueryData(data.getWhereClauses(), 
                                                                       data.getLimit(),
                                                                       data.getAllowFiltering(),
                                                                       data.getFetchSize(),
                                                                       data.getDistinct()));
    }
    
  
    @Override
    public <E> ListEntityReadQuery<E> asEntity(Class<E> objectClass) {
        return new ListEntityReadQuery<>(getContext(), this, objectClass) ;
    }

    
    @Override
    public ListReadWithUnit<RecordList> columns(String... names) {
        return columns(ImmutableSet.copyOf(names));
    }
    
    
    

    @Override
    public CompletableFuture<RecordList> executeAsync() {
        Statement statement = data.toStatement(getContext());
        return performAsync(statement).thenApply(resultSet -> newRecordList(resultSet));
    }        
    

    
    
    
    private static class ListEntityReadQuery<E> extends AbstractQuery<ListEntityReadQuery<E>> implements ListRead<EntityList<E>> {
        private final ListReadQuery read;
        private final Class<E> clazz;
        
        public ListEntityReadQuery(Context ctx, ListReadQuery read, Class<E> clazz) {
            super(ctx);
            this.read = read;
            this.clazz = clazz;
        }

        @Override
        protected ListEntityReadQuery<E> newQuery(Context newContext) {
            return new ListReadQuery(newContext, read.data).asEntity(clazz);
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
    
    
    
    
    private static final class CountReadQueryData extends QueryData {
        final ImmutableSet<Clause> whereClauses;
        final Optional<Integer> optionalLimit;
        final Optional<Boolean> optionalAllowFiltering;
        final Optional<Integer> optionalFetchSize;
        final Optional<Boolean> optionalDistinct;

        public CountReadQueryData(ImmutableSet<Clause> whereClauses, 
                                  Optional<Integer> optionalLimit, 
                                  Optional<Boolean> optionalAllowFiltering,
                                  Optional<Integer> optionalFetchSize,
                                  Optional<Boolean> optionalDistinct) {
            this.whereClauses = whereClauses;
            this.optionalLimit = optionalLimit;
            this.optionalAllowFiltering = optionalAllowFiltering;
            this.optionalFetchSize = optionalFetchSize;
            this.optionalDistinct = optionalDistinct;
        }
        

        
        public CountReadQueryData withWhereClauses(ImmutableSet<Clause> whereClauses) {
            return new CountReadQueryData(whereClauses,
                                          this.optionalLimit,
                                          this.optionalAllowFiltering,
                                          this.optionalFetchSize,
                                          this.optionalDistinct);  
        }


        
        public CountReadQueryData withLimit(Optional<Integer> optionalLimit) {
            return new CountReadQueryData(this.whereClauses,
                                          optionalLimit,
                                          this.optionalAllowFiltering,
                                          this.optionalFetchSize,
                                          this.optionalDistinct);  
        }

        
        public CountReadQueryData withAllowFiltering(Optional<Boolean> optionalAllowFiltering) {
            return new CountReadQueryData(this.whereClauses,
                                          this.optionalLimit,
                                          optionalAllowFiltering,
                                          this.optionalFetchSize,
                                          this.optionalDistinct);  
        }

        
        public CountReadQueryData withFetchSize(Optional<Integer> optionalFetchSize) {
            return new CountReadQueryData(this.whereClauses,
                                          this.optionalLimit,
                                          this.optionalAllowFiltering,
                                          optionalFetchSize,
                                          this.optionalDistinct);  
        }

        
        public CountReadQueryData withDistinct(Optional<Boolean> optionalDistinct) {
            return new CountReadQueryData(this.whereClauses,
                                          this.optionalLimit,
                                          this.optionalAllowFiltering,
                                          this.optionalFetchSize,
                                          optionalDistinct);  
        }
        
        
        public ImmutableSet<Clause> getWhereClauses() {
            return whereClauses;
        }

        public Optional<Integer> getOptionalLimit() {
            return optionalLimit;
        }

        public Optional<Boolean> getOptionalAllowFiltering() {
            return optionalAllowFiltering;
        }

        public Optional<Integer> getFetchSize() {
            return optionalFetchSize;
        }

        public Optional<Boolean> getDistinct() {
            return optionalDistinct;
        }

        
        Statement toStatement(Context ctx) {
            Select.Selection selection = select();
            
            optionalDistinct.ifPresent(distinct -> { if (distinct) selection.distinct(); });
    
     
            selection.countAll();
            
            Select select = selection.from(ctx.getTable());
            Select.Where where = null;
            for (Clause clause : whereClauses) {
                if (where == null) {
                    where = select.where(clause);
                } else {
                    where = where.and(clause);
                }
            }
    
            optionalLimit.ifPresent(limit -> select.limit(limit));
            optionalAllowFiltering.ifPresent(allowFiltering -> { if (allowFiltering)  select.allowFiltering(); });
            optionalFetchSize.ifPresent(fetchSize -> select.setFetchSize(fetchSize));
            
            return select;
        }
    }


    private static class CountReadQuery extends AbstractQuery<CountReadQuery> implements ListRead<Count> {
        
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
        public ListRead<Count> withLimit(int limit) {
            return new CountReadQuery(getContext(),
                                      data.withLimit(Optional.of(limit))); 
        }
        
        
        @Override
        public ListRead<Count> withAllowFiltering() {
            return new CountReadQuery(getContext(),
                                      data.withAllowFiltering(Optional.of(true))); 
        }
    
        @Override
        public ListRead<Count> withFetchSize(int fetchSize) {
            return new CountReadQuery(getContext(),
                                      data.withFetchSize(Optional.of(fetchSize)));
        }
        
        @Override
        public ListRead<Count> withDistinct() {
            return new CountReadQuery(getContext(),
                                      data.withDistinct(Optional.of(true)));
        }
    
    
        
        public CompletableFuture<Count> executeAsync() {
            Statement statement = data.toStatement(getContext());
            
            return performAsync(statement).thenApply(resultSet -> Count.newCountResult(resultSet));
        }        
    }  
}
    
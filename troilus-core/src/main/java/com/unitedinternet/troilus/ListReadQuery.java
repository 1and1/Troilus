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
import com.unitedinternet.troilus.interceptor.ListReadQueryData;
import com.unitedinternet.troilus.interceptor.ListReadQueryPostInterceptor;
import com.unitedinternet.troilus.interceptor.ListReadQueryPreInterceptor;




 

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
        return new CountReadQuery(getContext(), new CountReadQueryData().withWhereClauses(data.getWhereClauses())
                                                                        .withLimit(data.getLimit())
                                                                        .withFetchSize(data.getFetchSize())
                                                                        .withAllowFiltering(data.getAllowFiltering())
                                                                        .withDistinct(data.getDistinct()));
    }
    
    @Override
    public <E> ListEntityReadQuery<E> asEntity(Class<E> objectClass) {
        return new ListEntityReadQuery<>(getContext(), this, objectClass) ;
    }

    
    @Override
    public ListReadWithUnit<RecordList> columns(String... names) {
        return columns(ImmutableSet.copyOf(names));
    }
    
    
    private Statement toStatement(ListReadQueryData queryData) {
        Select.Selection selection = select();

        queryData.getDistinct().ifPresent(distinct -> { if (distinct) selection.distinct(); });

        
        if (queryData.getColumnsToFetch().isPresent()) {
            queryData.getColumnsToFetch().get().forEach((columnName, withMetaData) -> selection.column(columnName));
            queryData.getColumnsToFetch().get().entrySet()
                                               .stream()
                                               .filter(entry -> entry.getValue())
                                               .forEach(entry -> { selection.ttl(entry.getKey()); selection.writeTime(entry.getKey()); });
        } else {
            selection.all();
        }
        
        Select select = selection.from(getContext().getTable());
        
        queryData.getWhereClauses().forEach(whereClause -> select.where(whereClause));

        queryData.getLimit().ifPresent(limit -> select.limit(limit));
        queryData.getAllowFiltering().ifPresent(allowFiltering -> { if (allowFiltering)  select.allowFiltering(); });
        queryData.getFetchSize().ifPresent(fetchSize -> select.setFetchSize(fetchSize));
        
        return select;
    }
    

    private ListReadQueryData getPreprocessedData() {
        ListReadQueryData queryData = data;
        for (ListReadQueryPreInterceptor interceptor : getContext().getInterceptors(ListReadQueryPreInterceptor.class)) {
            queryData = interceptor.onPreListRead(queryData);
        }
        
        return queryData;
    }
    
    

    @Override
    public CompletableFuture<RecordList> executeAsync() {
        ListReadQueryData preprocessedData = getPreprocessedData(); 
        Statement statement = toStatement(preprocessedData);
        
        return getContext().performAsync(statement)
                           .thenApply(resultSet -> newRecordList(resultSet))
                           .thenApply(recordList -> {
                                                       for (ListReadQueryPostInterceptor interceptor : getContext().getInterceptors(ListReadQueryPostInterceptor.class)) {
                                                           recordList = interceptor.onPostListRead(preprocessedData, recordList);
                                                       }
                                                       return recordList;
                                                    });
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
    
    
    
    
    private static final class CountReadQueryData {
        final ImmutableSet<Clause> whereClauses;
        final Optional<Integer> optionalLimit;
        final Optional<Boolean> optionalAllowFiltering;
        final Optional<Integer> optionalFetchSize;
        final Optional<Boolean> optionalDistinct;

        
        
        public CountReadQueryData() {
            this(ImmutableSet.of(),
                 Optional.empty(),
                 Optional.empty(),
                 Optional.empty(),
                 Optional.empty());
        }
        
        private CountReadQueryData(ImmutableSet<Clause> whereClauses, 
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

        public Optional<Integer> getLimit() {
            return optionalLimit;
        }

        public Optional<Boolean> getAllowFiltering() {
            return optionalAllowFiltering;
        }

        public Optional<Integer> getFetchSize() {
            return optionalFetchSize;
        }

        public Optional<Boolean> getDistinct() {
            return optionalDistinct;
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
    
    
        
        private Statement toStatement(CountReadQueryData queryData) {
            Select.Selection selection = select();
            
            queryData.getDistinct().ifPresent(distinct -> { if (distinct) selection.distinct(); });
    
     
            selection.countAll();
            
            Select select = selection.from(getContext().getTable());
            
            queryData.getWhereClauses().forEach(whereClause -> select.where(whereClause));
            
            queryData.getLimit().ifPresent(limit -> select.limit(limit));
            queryData.getAllowFiltering().ifPresent(allowFiltering -> { if (allowFiltering)  select.allowFiltering(); });
            queryData.getFetchSize().ifPresent(fetchSize -> select.setFetchSize(fetchSize));
            
            return select;
        }

        
        public CompletableFuture<Count> executeAsync() {
            Statement statement = toStatement(data);
            return getContext().performAsync(statement).thenApply(resultSet -> Count.newCountResult(resultSet));
        }        
    }  
}
    
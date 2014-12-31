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

import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.Select;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableSet;
import com.unitedinternet.troilus.Dao.ListRead;
import com.unitedinternet.troilus.Dao.ListReadWithColumns;
import com.unitedinternet.troilus.Dao.ListReadWithUnit;
import com.unitedinternet.troilus.QueryFactory.ColumnToFetch;


class ListReadQuery extends AbstractQuery<ListRead<RecordList>> implements ListReadWithUnit<RecordList> {
    private final QueryFactory queryFactory;
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
        super(ctx);
        this.queryFactory = queryFactory;
        this.clauses = clauses;
        this.columnsToFetch = columnsToFetch;
        this.optionalLimit = optionalLimit;
        this.optionalAllowFiltering = optionalAllowFiltering;
        this.optionalFetchSize = optionalFetchSize;
        this.optionalDistinct = optionalDistinct;
    }

    @Override
    protected ListRead<RecordList> newQuery(Context newContext) {
        return queryFactory.newListSelection(newContext, 
                                             clauses, 
                                             columnsToFetch,
                                             optionalLimit, 
                                             optionalAllowFiltering, 
                                             optionalFetchSize,
                                             optionalDistinct);
    }
    
    
    @Override
    public ListRead<RecordList> all() {
        return queryFactory.newListSelection(getContext(), 
                                             clauses, 
                                             Optional.empty(),
                                             optionalLimit, 
                                             optionalAllowFiltering, 
                                             optionalFetchSize,
                                             optionalDistinct);
    }
    
 
    @Override 
    public ListReadWithUnit<RecordList> columns(ImmutableCollection<String> namesToRead) {
        return queryFactory.newListSelection(getContext(), 
                                clauses, 
                                Immutables.merge(columnsToFetch, ColumnToFetch.create(namesToRead)), 
                                optionalLimit, 
                                optionalAllowFiltering, 
                                optionalFetchSize,
                                optionalDistinct);
    }
    
    
    
    @Override
    public ListReadWithUnit<RecordList> column(String name) {
        return queryFactory.newListSelection(getContext(), 
                                clauses,  
                                Immutables.merge(columnsToFetch, ColumnToFetch.create(name, false, false)), 
                                optionalLimit, 
                                optionalAllowFiltering,
                                optionalFetchSize,
                                optionalDistinct);
    }

    
    @Override
    public ListReadWithColumns<RecordList> columnWithMetadata(String name) {
        return queryFactory.newListSelection(getContext(), 
                                clauses,  
                                Immutables.merge(columnsToFetch, ColumnToFetch.create(name, true, true)), 
                                optionalLimit, 
                                optionalAllowFiltering,
                                optionalFetchSize,
                                optionalDistinct);
    }
    

    @Override
    public ListRead<RecordList> withLimit(int limit) {
        return queryFactory.newListSelection(getContext(),
                                clauses, 
                                columnsToFetch, 
                                Optional.of(limit), 
                                optionalAllowFiltering, 
                                optionalFetchSize,
                                optionalDistinct);
    }
    
    @Override
    public ListRead<RecordList> withAllowFiltering() {
        return queryFactory.newListSelection(getContext(), 
                                clauses, 
                                columnsToFetch, 
                                optionalLimit, 
                                Optional.of(true), 
                                optionalFetchSize,
                                optionalDistinct);
    }

    @Override
    public ListRead<RecordList> withFetchSize(int fetchSize) {
        return queryFactory.newListSelection(getContext(), 
                                clauses, 
                                columnsToFetch, 
                                optionalLimit, 
                                optionalAllowFiltering, 
                                Optional.of(fetchSize),
                                optionalDistinct);
    }
    
    @Override
    public ListRead<RecordList> withDistinct() {
        return queryFactory.newListSelection(getContext(), 
                                clauses, 
                                columnsToFetch, 
                                optionalLimit, 
                                optionalAllowFiltering, 
                                optionalFetchSize,
                                Optional.of(true));
    }
    
   
    @Override
    public ListRead<Count> count() {
        return queryFactory.newCountRead(getContext(),
                                         clauses, 
                                         optionalLimit, 
                                         optionalAllowFiltering, 
                                         optionalFetchSize, 
                                         optionalDistinct);
    }
    
  
    @Override
    public <E> ListRead<EntityList<E>> asEntity(Class<E> objectClass) {
        return queryFactory.newListSelection(getContext(), this, objectClass) ;
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
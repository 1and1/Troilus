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
import com.google.common.collect.ImmutableSet;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

import com.unitedinternet.troilus.Dao.ListRead;


 
class CountReadQuery extends AbstractQuery<ListRead<Count>> implements ListRead<Count> {
    private final QueryFactory queryFactory;
    private final ImmutableSet<Clause> clauses;
    private final Optional<Integer> optionalLimit;
    private final Optional<Boolean> optionalAllowFiltering;
    private final Optional<Integer> optionalFetchSize;
    private final Optional<Boolean> optionalDistinct;


    public CountReadQuery(Context ctx, 
                          QueryFactory queryFactory,
                          ImmutableSet<Clause> clauses, 
                          Optional<Integer> optionalLimit, 
                          Optional<Boolean> optionalAllowFiltering,
                          Optional<Integer> optionalFetchSize,
                          Optional<Boolean> optionalDistinct) {
        super(ctx);
        this.queryFactory = queryFactory;
        this.clauses = clauses;
        this.optionalLimit = optionalLimit;
        this.optionalAllowFiltering = optionalAllowFiltering;
        this.optionalFetchSize = optionalFetchSize;
        this.optionalDistinct = optionalDistinct;
    }

    
    @Override
    protected ListRead<Count> newQuery(Context newContext) {
        return queryFactory.newCountRead(newContext, 
                                         clauses, 
                                         optionalLimit, 
                                         optionalAllowFiltering, 
                                         optionalFetchSize,
                                         optionalDistinct);
    }
    
    @Override
    public ListRead<Count> withLimit(int limit) {
        return queryFactory.newCountRead(getContext(),
                                         clauses, 
                                         Optional.of(limit), 
                                         optionalAllowFiltering, 
                                         optionalFetchSize,
                                         optionalDistinct);
    }
    
    
    @Override
    public ListRead<Count> withAllowFiltering() {
        return queryFactory.newCountRead(getContext(), 
                                         clauses, 
                                         optionalLimit, 
                                         Optional.of(true), 
                                         optionalFetchSize,
                                         optionalDistinct);
    }

    @Override
    public ListRead<Count> withFetchSize(int fetchSize) {
        return queryFactory.newCountRead(getContext(), 
                                         clauses, 
                                         optionalLimit, 
                                         optionalAllowFiltering, 
                                         Optional.of(fetchSize),
                                         optionalDistinct);
    }
    
    @Override
    public ListRead<Count> withDistinct() {
        return queryFactory.newCountRead(getContext(), 
                                         clauses, 
                                         optionalLimit, 
                                         optionalAllowFiltering, 
                                         optionalFetchSize,
                                         Optional.of(true));
    }
    
    
    @Override
    public Count execute() {
        try {
            return executeAsync().get(Long.MAX_VALUE, TimeUnit.DAYS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw Exceptions.unwrapIfNecessary(e);
        }
    }


    public CompletableFuture<Count> executeAsync() {
        Select.Selection selection = select();

        optionalDistinct.ifPresent(distinct -> { if (distinct) selection.distinct(); });

 
        selection.countAll();
        
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
                           .thenApply(resultSet -> Count.newCountResult(resultSet));
    }        
}    


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

import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Select.Selection;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.unitedinternet.troilus.Dao.ListRead;
import com.unitedinternet.troilus.Dao.ListReadWithColumns;
import com.unitedinternet.troilus.Dao.ListReadWithUnit;
import com.unitedinternet.troilus.Dao.SingleRead;
import com.unitedinternet.troilus.Dao.SingleReadWithColumns;
import com.unitedinternet.troilus.Dao.SingleReadWithUnit;




 

public class CountReadQuery extends AbstractQuery<CountReadQuery> implements ListRead<Count> {
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
        super(ctx, queryFactory);
        this.clauses = clauses;
        this.optionalLimit = optionalLimit;
        this.optionalAllowFiltering = optionalAllowFiltering;
        this.optionalFetchSize = optionalFetchSize;
        this.optionalDistinct = optionalDistinct;
    }

    
    @Override
    protected CountReadQuery newQuery(Context newContext) {
        return newCountReadQuery(newContext, 
                                 clauses, 
                                 optionalLimit, 
                                 optionalAllowFiltering, 
                                 optionalFetchSize,
                                 optionalDistinct);
    }
    
    @Override
    public ListRead<Count> withLimit(int limit) {
        return newCountReadQuery(clauses, 
                                 Optional.of(limit), 
                                 optionalAllowFiltering, 
                                 optionalFetchSize,
                                 optionalDistinct);
    }
    
    
    @Override
    public ListRead<Count> withAllowFiltering() {
        return newCountReadQuery(clauses, 
                                 optionalLimit, 
                                 Optional.of(true), 
                                 optionalFetchSize,
                                 optionalDistinct);
    }

    @Override
    public ListRead<Count> withFetchSize(int fetchSize) {
        return newCountReadQuery(clauses, 
                                 optionalLimit, 
                                 optionalAllowFiltering, 
                                 Optional.of(fetchSize),
                                 optionalDistinct);
    }
    
    @Override
    public ListRead<Count> withDistinct() {
        return newCountReadQuery(clauses, 
                                 optionalLimit, 
                                 optionalAllowFiltering, 
                                 optionalFetchSize,
                                 Optional.of(true));
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
    
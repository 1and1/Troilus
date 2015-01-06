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

import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.Select;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;



 
public class ListReadQueryData extends QueryData {

    final ImmutableSet<Clause> whereClauses;
    final Optional<ImmutableMap<String, Boolean>> columnsToFetch;
    final Optional<Integer> optionalLimit;
    final Optional<Boolean> optionalAllowFiltering;
    final Optional<Integer> optionalFetchSize;
    final Optional<Boolean> optionalDistinct;

    public ListReadQueryData(ImmutableSet<Clause> whereClauses, 
                             Optional<ImmutableMap<String, Boolean>> columnsToFetch, 
                             Optional<Integer> optionalLimit, 
                             Optional<Boolean> optionalAllowFiltering,
                             Optional<Integer> optionalFetchSize,
                             Optional<Boolean> optionalDistinct) {
        this.whereClauses = whereClauses;
        this.columnsToFetch = columnsToFetch;
        this.optionalLimit = optionalLimit;
        this.optionalAllowFiltering = optionalAllowFiltering;
        this.optionalFetchSize = optionalFetchSize;
        this.optionalDistinct = optionalDistinct;
    }
    

    
    public ListReadQueryData withWhereClauses(ImmutableSet<Clause> whereClauses) {
        return new ListReadQueryData(whereClauses,
                                     this.columnsToFetch,
                                     this.optionalLimit,
                                     this.optionalAllowFiltering,
                                     this.optionalFetchSize,
                                     this.optionalDistinct);  
    }

    
    public ListReadQueryData withColumnsToFetch(Optional<ImmutableMap<String, Boolean>> columnsToFetch) {
        return new ListReadQueryData(this.whereClauses,
                                     columnsToFetch,
                                     this.optionalLimit,
                                     this.optionalAllowFiltering,
                                     this.optionalFetchSize,
                                     this.optionalDistinct);  
    }

    
    public ListReadQueryData withLimit(Optional<Integer> optionalLimit) {
        return new ListReadQueryData(this.whereClauses,
                                     this.columnsToFetch,
                                     optionalLimit,
                                     this.optionalAllowFiltering,
                                     this.optionalFetchSize,
                                     this.optionalDistinct);  
    }

    
    public ListReadQueryData withAllowFiltering(Optional<Boolean> optionalAllowFiltering) {
        return new ListReadQueryData(this.whereClauses,
                                     this.columnsToFetch,
                                     this.optionalLimit,
                                     optionalAllowFiltering,
                                     this.optionalFetchSize,
                                     this.optionalDistinct);  
    }

    
    public ListReadQueryData withFetchSize(Optional<Integer> optionalFetchSize) {
        return new ListReadQueryData(this.whereClauses,
                                     this.columnsToFetch,
                                     this.optionalLimit,
                                     this.optionalAllowFiltering,
                                     optionalFetchSize,
                                     this.optionalDistinct);  
    }

    
    public ListReadQueryData withDistinct(Optional<Boolean> optionalDistinct) {
        return new ListReadQueryData(this.whereClauses,
                                     this.columnsToFetch,
                                     this.optionalLimit,
                                     this.optionalAllowFiltering,
                                     this.optionalFetchSize,
                                     optionalDistinct);  
    }
    
    
    public ImmutableSet<Clause> getWhereClauses() {
        return whereClauses;
    }

    public Optional<ImmutableMap<String, Boolean>> getColumnsToFetch() {
        return columnsToFetch;
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

    
    @Override
    protected Statement toStatement(Context ctx) {
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
        
        Select select = selection.from(ctx.getTable());
        
        whereClauses.forEach(whereClause -> select.where(whereClause));

        optionalLimit.ifPresent(limit -> select.limit(limit));
        optionalAllowFiltering.ifPresent(allowFiltering -> { if (allowFiltering)  select.allowFiltering(); });
        optionalFetchSize.ifPresent(fetchSize -> select.setFetchSize(fetchSize));
        
        return select;
    }
}
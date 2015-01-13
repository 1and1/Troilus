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

import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Clause;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;



 
public class ListReadQueryData {

    private final MinimalListReadQueryData data;

    
    ListReadQueryData() {
        this(new MinimalListReadQueryData());
    }

    private ListReadQueryData(MinimalListReadQueryData data) {
        this.data = data;
    }
    

    public ListReadQueryData keys(ImmutableMap<String, ImmutableList<Object>> keys) {
        return new ListReadQueryData(data.keys(keys));  
    }
    
    public ListReadQueryData whereClauses(ImmutableSet<Clause> whereClauses) {
        return new ListReadQueryData(data.whereClauses(whereClauses));  
    }

    public ListReadQueryData columnsToFetch(ImmutableMap<String, Boolean> columnsToFetch) {
        return new ListReadQueryData(data.columnsToFetch(columnsToFetch));  
    }

    public ListReadQueryData limit(Optional<Integer> optionalLimit) {
        return new ListReadQueryData(data.limit(optionalLimit.orElse(null)));  
    }

    public ListReadQueryData allowFiltering(Optional<Boolean> optionalAllowFiltering) {
        return new ListReadQueryData(data.allowFiltering(optionalAllowFiltering.orElse(null)));  
    }

    public ListReadQueryData fetchSize(Optional<Integer> optionalFetchSize) {
        return new ListReadQueryData(data.fetchSize(optionalFetchSize.orElse(null)));  
    }

    public ListReadQueryData distinct(Optional<Boolean> optionalDistinct) {
        return new ListReadQueryData(data.distinct(optionalDistinct.orElse(null)));  
    }
    
    public ImmutableSet<Clause> getWhereClauses() {
        return data.getWhereClauses();
    }

    public ImmutableMap<String, Boolean> getColumnsToFetch() {
        return data.getColumnsToFetch();
    }

    public Optional<Integer> getLimit() {
        return Optional.ofNullable(data.getLimit());
    }

    public Optional<Boolean> getAllowFiltering() {
        return Optional.ofNullable(data.getAllowFiltering());
    }

    public Optional<Integer> getFetchSize() {
        return Optional.ofNullable(data.getFetchSize());
    }

    public Optional<Boolean> getDistinct() {
        return Optional.ofNullable(data.getDistinct());
    }
    
    
    Statement toStatement(Context ctx) {
        return data.toStatement(ctx);
    }
}
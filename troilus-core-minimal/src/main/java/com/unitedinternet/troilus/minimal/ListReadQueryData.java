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
package com.unitedinternet.troilus.minimal;


import com.datastax.driver.core.querybuilder.Clause;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;



 
public interface ListReadQueryData {

    ListReadQueryData keys(ImmutableMap<String, ImmutableList<Object>> keys);
    
    ListReadQueryData whereConditions(ImmutableSet<Clause> whereClauses);

    ListReadQueryData columnsToFetch(ImmutableMap<String, Boolean> columnsToFetch);

    ListReadQueryData limit(Integer limit);
    
    ListReadQueryData allowFiltering(Boolean allowFiltering);

    ListReadQueryData fetchSize(Integer fetchSize);
    
    ListReadQueryData distinct(Boolean distinct);
    
    ImmutableMap<String, ImmutableList<Object>> getKeys();
    
    ImmutableSet<Clause> getWhereClauses();

    ImmutableMap<String, Boolean> getColumnsToFetch();

    Integer getLimit();

    Boolean getAllowFiltering();

    Integer getFetchSize();
    
    Boolean getDistinct();
}
/*
 * Copyright 1&1 Internet AG, https://github.com/1and1/
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.oneandone.troilus.java7.interceptor;


import net.oneandone.troilus.Tablename;

import com.datastax.driver.core.PagingState;
import com.datastax.driver.core.querybuilder.Clause;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;



 
/**
 * list read query data 
 */
public interface ReadQueryData {
    
    
    /**
     * @return the tablename
     */
    Tablename getTablename();

    /**
     * @param keys  the keys
     * @return the new read query data
     */
    ReadQueryData keys(ImmutableMap<String, ImmutableList<Object>> keys);

    
    /**
     * @param whereConditions the where conditions
     * @return the new read query data
     */
    ReadQueryData whereConditions(ImmutableSet<Clause> whereConditions);

    /**
     * @param columnsToFetch  the columns to fetch 
     * @return the new read query data
     */
    ReadQueryData columnsToFetch(ImmutableMap<String, Boolean> columnsToFetch);

    /**
     * @param limit   the lmit
     * @return the new read query data
     */
    ReadQueryData limit(Integer limit);

    /**
     * @param allowFiltering  the allow filtering flag
     * @return the new read query data
     */
    ReadQueryData allowFiltering(Boolean allowFiltering);

    /**
     * @param fetchSize  the fetch size
     * @return the new read query data
     */
    ReadQueryData fetchSize(Integer fetchSize);

    /**
     * @param distinct    the distinct flag
     * @return the new read query data
     */
    ReadQueryData distinct(Boolean distinct);

    /**
     * @return  the keys
     */
    ImmutableMap<String, ImmutableList<Object>> getKeys();

    /**
     * @return the where conditions
     */
    ImmutableSet<Clause> getWhereConditions();

    /**
     * @return  the columns to fetch
     */
    ImmutableMap<String, Boolean> getColumnsToFetch();

    /**
     * @return the limit
     */
    Integer getLimit();

    /**
     * @return the allow filtering flag
     */
    Boolean getAllowFiltering();

    /**
     * @return  the fetch size
     */
    Integer getFetchSize();

    /**
     * @return the distinct flag
     */
    Boolean getDistinct();
    
    /**
     * @return paging state or null, if none
     * @return
     */
    PagingState getPagingState();
    
    /** Sets paging state
     * 
     * @param pagingState
     * @return
     */
    ReadQueryData pagingState(PagingState pagingState);
}
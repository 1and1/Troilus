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
package net.oneandone.troilus.interceptor;


import java.util.Optional;


import com.datastax.driver.core.querybuilder.Clause;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;



/**
 * List query data
 *
 */
public interface ListReadQueryData {

    /**
     * @param keys  the keys 
     * @return the new query data
     */
    ListReadQueryData keys(ImmutableMap<String, ImmutableList<Object>> keys);
    
    /**
     * @param whereConditions the where conditions
     * @return the new query data
     */
    ListReadQueryData whereConditions(ImmutableSet<Clause> whereConditions);

    /**
     * @param columnsToFetch  the columns to fetch
     * @return the new query data
     */
    ListReadQueryData columnsToFetch(ImmutableMap<String, Boolean> columnsToFetch);

    /**
     * @param limit the limit
     * @return the new query data
     */
    ListReadQueryData limit(Optional<Integer> limit);

    /**
     * @param oallowFiltering  the allow filtering flag
     * @return the new query data
     */
    ListReadQueryData allowFiltering(Optional<Boolean> allowFiltering);

    /**
     * @param fetchSize  the fetch size
     * @return the new query data
     */
    ListReadQueryData fetchSize(Optional<Integer> fetchSize);

    /**
     * @param distinct  the distinct flag
     * @return the new query data
     */
    ListReadQueryData distinct(Optional<Boolean> distinct);
    
    /**
     * @return  the keys
     */
    ImmutableMap<String, ImmutableList<Object>> getKeys();
    
    /**
     * @return the where conditions
     */
    ImmutableSet<Clause> getWhereConditions();

    /**
     * @return the columns to fetch
     */
    ImmutableMap<String, Boolean> getColumnsToFetch();

    /**
     * @return  the limit
     */
    Optional<Integer> getLimit();

    /**
     * @return the allow filtering flag
     */
    Optional<Boolean> getAllowFiltering();

    /**
     * @return  the fetch size
     */
    Optional<Integer> getFetchSize();

    /**
     * @return the distinct flag
     */
    Optional<Boolean> getDistinct();
}
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


import java.util.List;
import java.util.Map;
import java.util.Optional;



import java.util.Set;

import net.oneandone.troilus.ColumnName;

import com.datastax.driver.core.querybuilder.Clause;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;



/**
 * The write query data
 */
public interface WriteQueryData {
 
    /**
     * @return the tablename
     */
    String getTablename();
    
    
    /**
     * @param keys  the keys 
     * @return the new write data
     */
    WriteQueryData keys(ImmutableMap<String, Object> keys);
    
    /**
     * @param whereConditions the conditions 
     * @return the new write data
     */
    WriteQueryData whereConditions(ImmutableList<Clause> whereConditions);
    
    /**
     * @param valuesToMutate the mutation values
     * @return the new write data
     */
    WriteQueryData valuesToMutate(ImmutableMap<String, Optional<Object>> valuesToMutate);
 
    /**
     * @param setValuesToAdd the values to set 
     * @return the new write data
     */
    WriteQueryData setValuesToAdd(ImmutableMap<String, ImmutableSet<Object>> setValuesToAdd);
    
    /**
     * @param setValuesToRemove   the value to remove
     * @return the new write data
     */
    WriteQueryData setValuesToRemove(ImmutableMap<String, ImmutableSet<Object>> setValuesToRemove);
    
    /**
     * @param listValuesToAppend    the lsit values to append
     * @return the new write data
     */
    WriteQueryData listValuesToAppend(ImmutableMap<String, ImmutableList<Object>> listValuesToAppend);
    
    /**
     * @param listValuesToPrepend   the list values to prepend
     * @return the new write data
     */
    WriteQueryData listValuesToPrepend(ImmutableMap<String, ImmutableList<Object>> listValuesToPrepend);
    
    /**
     * @param listValuesToRemove    the list values to remove
     * @return the new write data
     */
    WriteQueryData listValuesToRemove(ImmutableMap<String, ImmutableList<Object>> listValuesToRemove);
    
    /**
     * @param mapValuesToMutate     the map values to mutate
     * @return the new write data
     */
    WriteQueryData mapValuesToMutate(ImmutableMap<String, ImmutableMap<Object, Optional<Object>>> mapValuesToMutate);

    /**
     * @param onlyIfConditions     the onlyIf conditions
     * @return the new write data
     */
    WriteQueryData onlyIfConditions(ImmutableList<Clause> onlyIfConditions);

    /**
     * @param ifNotExists   the ifNotExists flag
     * @return the new write data
     */
    WriteQueryData ifNotExists(Optional<Boolean> ifNotExists);

    /**
     * @return the keys
     */
    ImmutableMap<String, Object> getKeys();

    /**
     * @param name the key name
     * @param <T> the type
     * @return true, if contained
     */
    <T> boolean hasKey(ColumnName<T> name);

    /**
     * @param name the key name
     * @return true, if contained
     */
    boolean hasKey(String name);

    /**
     * @param name  the key name
     * @return the key value or NULL
     */
    Object getKey(String name);

    /**
     * @param name  the key name
     * @param <T>   the type
     * @return the key value or NULL
     */
    <T> T getKey(ColumnName<T> name);

    
    /**
     * @return  the whre conditions
     */
    ImmutableList<Clause> getWhereConditions();
    
    /**
     * @return the values to mutate
     */
    ImmutableMap<String, Optional<Object>> getValuesToMutate();

    /**
     * @param name the column name
     * @return true, if contained
     */
    boolean hasValueToMutate(String name);

    /**
     * @param name the column name
     * @param <T> the type
     * @return true, if contained
     */
    <T> boolean hasValueToMutate(ColumnName<T> name);

    /**
     * @param name the column name
     * @return the value or null
     */
    Object getValueToMutate(String name);

    /**
     * @param name the column name
     * @param <T> the type
     * @return the value or null
     */
    <T> T getValueToMutate(ColumnName<T> name);
    
    /**
     * @return the set values to add
     */
    ImmutableMap<String, ImmutableSet<Object>> getSetValuesToAdd();

    /**
     * @param name the column name
     * @return the value or null
     */
    boolean hasSetValuesToAdd(String name);

    /**
     * @param name the column name
     * @param <T>  the type
     * @return true, if contained
     */
    <T> boolean hasSetValuesToAdd(ColumnName<Set<T>> name);

    /**
     * @param name the column name
     * @return the values or empty set
     */
    ImmutableSet<Object> getSetValuesToAdd(String name);

    /**
     * @param name the column name
     * @param <T>  the type
     * @return the values or empty set
     */
    <T> ImmutableSet<T> getSetValuesToAdd(ColumnName<Set<T>> name);

    /**
     * @param name the column name
     * @return the value or null
     */
    boolean hasSetValuesToAddOrSet(String name);

    /**
     * @param name the column name
     * @param <T>  the type
     * @return true, if contained
     */
    <T> boolean hasSetValuesToAddOrSet(ColumnName<Set<T>> name);

    /**
     * @param name the column name
     * @return the values or empty set
     */
    ImmutableSet<Object> getSetValuesToAddOrSet(String name);

    /**
     * @param name the column name
     * @param <T>  the type
     * @return the values or empty set
     */
    <T> ImmutableSet<T> getSetValuesToAddOrSet(ColumnName<Set<T>> name);
    
    /**
     * @return the set values to remove
     */
    ImmutableMap<String, ImmutableSet<Object>> getSetValuesToRemove();
    
    /**
     * @param name the column name
     * @return true, if contained
     */
    boolean hasSetValuesToRemove(String name);

    /**
     * @param name the column name
     * @param <T>  the type
     * @return true, if contained
     */
    <T> boolean hasSetValuesToRemove(ColumnName<Set<T>> name);

    /**
     * @param name the column name
     * @return the values or empty set
     */
    ImmutableSet<Object> getSetValuesToRemove(String name);

    /**
     * @param name the column name
     * @param <T>  the type
     * @return the values or empty set
     */
    <T> ImmutableSet<T> getSetValuesToRemove(ColumnName<Set<T>> name);

    /**
     * @return the list values to append
     */
    ImmutableMap<String, ImmutableList<Object>> getListValuesToAppend();

    /**
     * @param name the column name
     * @return true, if contained
     */
    boolean hasListValuesToAppend(String name);

    /**
     * @param name the column name
     * @param <T>  the type
     * @return true, if contained
     */
    <T> boolean hasListValuesToAppend(ColumnName<List<T>> name);

    /**
     * @param name the column name
     * @return the values or empty list
     */
    ImmutableList<Object> getListValuesToAppend(String name);

    /**
     * @param name the column name
     * @param <T>  the type
     * @return the values or empty list
     */
    <T> ImmutableList<T> getListValuesToAppend(ColumnName<List<T>> name);
    
    /**
     * @return the list values to prepend
     */
    ImmutableMap<String, ImmutableList<Object>> getListValuesToPrepend();

    /**
     * @param name the column name
     * @return true, if contained
     */
    boolean hasListValuesToPrepend(String name);

    /**
     * @param name the column name
     * @param <T>  the type
     * @return true, if contained
     */
    <T> boolean hasListValuesToPrepend(ColumnName<List<T>> name);

    /**
     * @param name the column name
     * @return the values or empty list
     */
    ImmutableList<Object> getListValuesToPrepend(String name);

    /**
     * @param name the column name
     * @param <T>  the type
     * @return the values or empty list
     */
    <T> ImmutableList<T> getListValuesToPrepend(ColumnName<List<T>> name);

    /**
     * @param name the column name
     * @return true, if contained
     */
    boolean hasListValuesToAddOrSet(String name);

    /**
     * @param name the column name
     * @param <T>  the type
     * @return true, if contained
     */
    <T> boolean hasListValuesToAddOrSet(ColumnName<List<T>> name);

    /**
     * @param name the column name
     * @return the values or empty list
     */
    ImmutableList<Object> getListValuesToAddOrSet(String name);

    /**
     * @param name the column name
     * @param <T>  the type
     * @return the values or empty list
     */
    <T> ImmutableList<T> getListValuesToAddOrSet(ColumnName<List<T>> name);

    /**
     * @return the list values to remove
     */
    ImmutableMap<String, ImmutableList<Object>> getListValuesToRemove();

    /**
     * @param name the column name
     * @return true, if contained
     */
    boolean hasListValuesToRemove(String name);

    /**
     * @param name the column name
     * @param <T>  the type
     * @return true, if contained
     */
    <T> boolean hasListValuesToRemove(ColumnName<List<T>> name);

    /**
     * @param name the column name
     * @return the values or empty list
     */
    ImmutableList<Object> getListValuesToRemove(String name);

    /**
     * @param name the column name
     * @param <T>  the type
     * @return the values or empty list
     */
    <T> ImmutableList<T> getListValuesToRemove(ColumnName<List<T>> name);

    /**
     * @return the map values to mutate
     */
    ImmutableMap<String, ImmutableMap<Object, Optional<Object>>> getMapValuesToMutate();
    
    /**
     * @param name the column name
     * @return true, if contained
     */
    boolean hasMapValuesToMutate(String name);

    /**
     * @param name the column name
     * @param <T>  the key type
     * @param <V>  the value type
     * @return true, if contained
     */
    <T, V> boolean hasMapValuesToMutate(ColumnName<Map<T, V>> name);

    /**
     * @param name the column name
     * @return the values or empty map
     */
    ImmutableMap<Object, Optional<Object>> getMapValuesToMutate(String name);

    /**
     * @param name the column name
     * @param <T>  the key type
     * @param <V>  the value type
     * @return the values or empty map
     */
    <T, V> ImmutableMap<T, Optional<V>> getMapValuesToMutate(ColumnName<Map<T, V>> name);
    
    /**
     * @return the onlyIf conditions
     */
    ImmutableList<Clause> getOnlyIfConditions();
    
    /**
     * @return the ifNotExists flag
     */
    Optional<Boolean> getIfNotExits();
}
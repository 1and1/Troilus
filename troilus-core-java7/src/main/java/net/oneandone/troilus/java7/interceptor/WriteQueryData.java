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


import java.util.List;
import java.util.Map;
import java.util.Set;

import net.oneandone.troilus.ColumnName;










import com.datastax.driver.core.querybuilder.Clause;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;


 
/**
 * Write query data
 *
 */
public interface WriteQueryData {

    /**
     * @param keys  the keys
     * @return the new write query data
     */
    WriteQueryData keys(ImmutableMap<String, Object> keys);

    /**
     * @param whereConditions   the where conditions
     * @return the new write query data
     */
    WriteQueryData whereConditions(ImmutableList<Clause> whereConditions);

    /**
     * @param valuesToMutate  the values to mutate
     * @return the new write query data
     */
    WriteQueryData valuesToMutate(ImmutableMap<String, Optional<Object>> valuesToMutate);

    /**
     * @param setValuesToAdd   the set values to add
     * @return the new write query data
     */
    WriteQueryData setValuesToAdd(ImmutableMap<String, ImmutableSet<Object>> setValuesToAdd);
    
    /**
     * @param setValuesToRemove   the set values to remove
     * @return the new write query data
     */
    WriteQueryData setValuesToRemove(ImmutableMap<String, ImmutableSet<Object>> setValuesToRemove);

    /**
     * @param listValuesToAppend  the list values to append
     * @return the new write query data
     */
    WriteQueryData listValuesToAppend(ImmutableMap<String, ImmutableList<Object>> listValuesToAppend);

    /**
     * @param listValuesToPrepend  the list values to prepend
     * @return the new write query data
     */
    WriteQueryData listValuesToPrepend(ImmutableMap<String, ImmutableList<Object>> listValuesToPrepend);

    /**
     * @param listValuesToRemove the list values to remove
     * @return the new write query data
     */
    WriteQueryData listValuesToRemove(ImmutableMap<String, ImmutableList<Object>> listValuesToRemove);

    /**
     * @param mapValuesToMutate  the list values to mutate
     * @return the new write query data
     */
    WriteQueryData mapValuesToMutate(ImmutableMap<String, ImmutableMap<Object, Optional<Object>>> mapValuesToMutate);

    /**
     * @param onlyIfConditions  the onlyIf conditions
     * @return the new write query data
     */
    WriteQueryData onlyIfConditions(ImmutableList<Clause> onlyIfConditions);

    /**
     * @param ifNotExists  the ifNotExists flag
     * @return the new write query data
     */
    WriteQueryData ifNotExists(Boolean ifNotExists);

    /**
     * @return  the keys
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
     * @return the key value or NULL
     */
    <T> T getKey(ColumnName<T> name);

    /**
     * @return  the where conditions
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
     * @return true, if contained
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
     * @return the value or null
     */
    ImmutableSet<Object> getSetValuesToAdd(String name);

    /**
     * @param name the column name
     * @param <T>  the type
     * @return the value or null
     */
    <T> ImmutableSet<T> getSetValuesToAdd(ColumnName<Set<T>> name);
    
    /**
     * @return the set values to remove
     */
    ImmutableMap<String, ImmutableSet<Object>> getSetValuesToRemove();

    /**
     * @param name the column name
     * @return true, if contained
     */
    boolean hasSetValueToRemove(String name);

    /**
     * @param name the column name
     * @param <T>  the type
     * @return true, if contained
     */
    <T> boolean hasSetValueToRemove(ColumnName<Set<T>> name);

    /**
     * @param name the column name
     * @return the value or null
     */
    ImmutableSet<Object> getSetValueToRemove(String name);

    /**
     * @param name the column name
     * @param <T>  the type
     * @return the value or null
     */
    <T> ImmutableSet<T> getSetValueToRemove(ColumnName<Set<T>> name);

    /**
     * @return the list values to append
     */
    ImmutableMap<String, ImmutableList<Object>> getListValuesToAppend();

    /**
     * @param name the column name
     * @return true, if contained
     */
    boolean hasListValueToAppend(String name);

    /**
     * @param name the column name
     * @param <T>  the type 
     * @return true, if contained
     */
    <T> boolean hasListValueToAppend(ColumnName<List<T>> name);

    /**
     * @param name the column name
     * @return the value or null
     */
    ImmutableList<Object> getListValueToAppend(String name);

    /**
     * @param name the column name
     * @return true, if contained
     */
    boolean hasListValueToPrepend(String name);

    /**
     * @param name the column name
     * @param <T>  the type
     * @return true, if contained
     */
    <T> boolean hasListValueToPrepend(ColumnName<List<T>> name);

    /**
     * @param name the column name
     * @return the value or null
     */
    ImmutableList<Object> getListValueToPrepend(String name);

    /**
     * @param name the column name
     * @param <T>  the type
     * @return the value or null
     */
    <T> ImmutableList<T> getListValueToPrepend(ColumnName<List<T>> name);

    /**
     * @param name the column name
     * @param <T>  the type
     * @return the value or null
     */
    <T> ImmutableList<T> getListValueToAppend(ColumnName<List<T>> name);

    /**
     * @return  the list values to prepend
     */
    ImmutableMap<String, ImmutableList<Object>> getListValuesToPrepend();

    /**
     * @return the list values to remove
     */
    ImmutableMap<String, ImmutableList<Object>> getListValuesToRemove();

    /**
     * @param name the column name
     * @return true, if contained
     */
    boolean hasListValueToRemove(String name);

    /**
     * @param name the column name
     * @param <T>  the type
     * @return true, if contained
     */
    <T> boolean hasListValueToRemove(ColumnName<List<T>> name);

    /**
     * @param name the column name
     * @return the value or null
     */
    ImmutableList<Object> getListValueToRemove(String name);

    /**
     * @param name the column name
     * @param <T>  the type
     * @return the value or null
     */
    <T> ImmutableList<T> getListValueToRemove(ColumnName<List<T>> name);

    /**
     * @return the map values to mutate
     */
    ImmutableMap<String, ImmutableMap<Object, Optional<Object>>> getMapValuesToMutate();
    
    /**
     * @param name the column name
     * @return true, if contained
     */
    boolean hasMapValueToMutate(String name);

    /**
     * @param name the column name
     * @param <T>  the type
     * @return true, if contained
     */
    <T, V> boolean hasMapValueToMutate(ColumnName<Map<T, V>> name);

    /**
     * @param name the column name
     * @return the value or null
     */
    ImmutableMap<Object, Optional<Object>> getMapValueToMutate(String name);

    /**
     * @param name the column name
     * @param <T>  the type
     * @return the value or null
     */
    <T, V> ImmutableMap<T, Optional<V>> getMapValueToMutate(ColumnName<Map<T, V>> name);
    
    /**
     * @return the onyIf conditions
     */
    ImmutableList<Clause> getOnlyIfConditions();

    /**
     * @return the ifNotExists flag
     */
    Boolean getIfNotExits();
}
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
package net.oneandone.troilus;



import java.util.List;


import com.datastax.driver.core.querybuilder.Clause;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;


   

 
/**
 * Delete query data
 */
public interface DeleteQueryData {
    
    /**
     * @return the tablename
     */
    Tablename getTablename();  

    /**
     * @param key  the key
     * @return the new delete query data
     */
    DeleteQueryData key(ImmutableMap<String, Object> key);

    /**
     * returns map of values to remove from column family
     * @return
     */
    ImmutableMap<String, List<Object>> getMapValuesToRemove();
    
    /**
     * this method's purpose is to populate the list of map 
     * values to remove
     * 
     * @param removedMapValues
     * @return
     */
    DeleteQueryData mapValuesToRemove(ImmutableMap<String, List<Object>> removedMapValues);
    /**
     * @param whereConditions the where conditions
     * @return the new delete query data
     */
    DeleteQueryData whereConditions(ImmutableList<Clause> whereConditions);

    /**
     * @param onlyIfConditions  the onlyIf conditions
     * @return the new delete query data
     */
    DeleteQueryData onlyIfConditions(ImmutableList<Clause> onlyIfConditions);

    /**
     * @param IfExists  the ifNotExits conditions
     * @return the new delete query data
     */
    DeleteQueryData ifExists(Boolean IfExists);
    
    /**
     * @return the key
     */
    ImmutableMap<String, Object> getKey();

    /**
     * @return the where conditions
     */
    ImmutableList<Clause> getWhereConditions();

    /**
     * @return the onlyIf conditions
     */
    ImmutableList<Clause> getOnlyIfConditions();

    /**
     * @return the ifExists flag
     */
    Boolean getIfExists();
}
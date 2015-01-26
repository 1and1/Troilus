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
package net.oneandone.troilus.java7;

import net.oneandone.troilus.ColumnName;

import com.google.common.collect.ImmutableMap;


/**
 * value-aware update query
 * @param <U> the query type
 */    
public interface UpdateWithValues<U extends Update<U>> extends Update<U> {

    /**
     * @param name  the column name 
     * @param value the value to add
     * @return a cloned query instance with the modified behavior
     */
    U value(String name, Object value);

    /**
     * @param nameValuePairsToAdd  the column name value pairs to add
     * @return a cloned query instance with the modified behavior
     */
    U values(ImmutableMap<String, Object> nameValuePairsToAdd);

    /**
     * @param name  the column name
     * @param value the value to add
     * @param <T> the name type
     * @return a cloned query instance with the modified behavior
     */
    <T> U value(ColumnName<T> name, T value);
    
    /**
     * @param name   the set column name
     * @param value  the set value to remove
     * @return a cloned query instance with the modified behavior
     */
    U removeSetValue(String name, Object value);

    /**
     * @param name    the set column name
     * @param value   the set value to set
     * @return a cloned query instance with the modified behavior
     */
    U addSetValue(String name, Object value);

    /**
     * @param name   the list column name
     * @param value  the list value to append
     * @return a cloned query instance with the modified behavior
     */
    U appendListValue(String name, Object value);

    /**
     * @param name   the list column name
     * @param value  the list value to preprend
     * @return a cloned query instance with the modified behavior
     */
    U prependListValue(String name, Object value);

    /**
     * @param name   the list column name
     * @param value  the list value to remove
     * @return a cloned query instance with the modified behavior
     */
    U removeListValue(String name, Object value);

    /**
     * @param name   the map column name 
     * @param key    the map key name
     * @param value  the map value
     * @return a cloned query instance with the modified behavior
     */
    U putMapValue(String name, Object key, Object value);
}


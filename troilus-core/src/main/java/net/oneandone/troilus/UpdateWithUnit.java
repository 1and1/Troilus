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
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;



/**
 * value-aware update query
 * @param <U> the query type
 */    
public interface UpdateWithUnit<U extends Update<U>> extends Update<U> {

    /**
     * @param entity  the entity to write
     * @return a cloned query instance with the modified behavior 
     */
    U entity(Object entity);
    
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
    default <T> U value(final ColumnName<T> name, final T value) {
        return value(name.getName(), value);
    }
    
    /**
     * @param name   the set column name
     * @param value  the set value to remove 
     * @return a cloned query instance with the modified behavior
     */
    U removeSetValue(String name, Object value);

    /**
     * @param name   the set column name
     * @param value  the set value to remove 
     * @param <T>    the type
     * @return a cloned query instance with the modified behavior
     */
    default <T> U removeSetValue(final ColumnName<Set<T>> name, final T value) {
        return removeSetValue(name.getName(), value);
    }
    
    /**
     * @param name    the set column name
     * @param value   the set value to set
     * @return a cloned query instance with the modified behavior
     */
    U addSetValue(String name, Object value);

    /**
     * @param name    the set column name
     * @param value   the set value to set
     * @param <T>     the type
     * @return a cloned query instance with the modified behavior
     */
    default <T> U addSetValue(final ColumnName<Set<T>> name, final T value) {
        return addSetValue(name.getName(), value);
    }
   

    /**
     * @param name   the list column name
     * @param value  the list value to append
     * @return a cloned query instance with the modified behavior
     */
    U appendListValue(String name, Object value);

    /**
     * @param name   the list column name
     * @param value  the list value to append
     * @param <T>    the type
     * @return a cloned query instance with the modified behavior
     */
    default <T> U appendListValue(final ColumnName<List<T>> name, final T value) {
        return appendListValue(name.getName(), value);
    }

    /**
     * @param name   the list column name
     * @param value  the list value to prepend
     * @return a cloned query instance with the modified behavior
     */
    U prependListValue(String name, Object value);

    /**
     * @param name   the list column name
     * @param value  the list value to prepend
     * @param <T>    the type
     * @return a cloned query instance with the modified behavior
     */
    default <T> U prependListValue(final ColumnName<List<T>> name, final T value) {
        return prependListValue(name.getName(), value);
    }
    
    /**
     * @param name   the list column name
     * @param value  the list value to remove
     * @return a cloned query instance with the modified behavior
     */
    U removeListValue(String name, Object value);

    /**
     * @param name   the list column name
     * @param value  the list value to remove
     * @param <T>    the type
     * @return a cloned query instance with the modified behavior
     */
    default <T> U removeListValue(final ColumnName<List<T>> name, final T value) {
        return removeListValue(name.getName(), value);
    }
       
    /**
     * @param name   the map column name 
     * @param key    the map key name
     * @param value  the map value
     * @return a cloned query instance with the modified behavior
     */
    U putMapValue(String name, Object key, Object value);
    
    /**
     * @param name   the map column name 
     * @param key    the map key name
     * @param value  the map value
     * @param <T>    the key type
     * @param <V>    the value type
     * @return a cloned query instance with the modified behavior
     */
    default <T, V> U putMapValue(final ColumnName<Map<T, V>> name, final T key, final V value) {
       return putMapValue(name.getName(), key, value);
    }
}

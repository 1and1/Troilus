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

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

/**
 * column-ware list read
 *
 * @param <T>  the result type
 */
public interface ListReadWithColumns<T, R> extends ListRead<T, R> {
    
    /**
     * @param namesToRead  the names to read
     * @return  a cloned query instance with the modified behavior
     */
    ListReadWithColumns<T, R> columns(ImmutableCollection<String> namesToRead);

    
    /**
     * @param name  the column name to read 
     * @return  a cloned query instance with the modified behavior
     */
    ListReadWithColumns<T, R> column(String name);

    
    /**
     * @param name  the column name incl. meta data to read 
     * @return  a cloned query instance with the modified behavior
     */
    ListReadWithColumns<T, R> columnWithMetadata(String name);

    /**
     * @param names  the column names to read 
     * @return  a cloned query instance with the modified behavior
     */
    default ListReadWithColumns<T, R> columns(final String... names) {
        return columns(ImmutableSet.copyOf(names));
    }
    
    /**
     * @param name  the column name to read 
     * @return  a cloned query instance with the modified behavior
     */
    default ListReadWithColumns<T, R> column(final ColumnName<?> name) {
        return column(name.getName());
    }
    
    /**
     * @param name  the column name incl. meta data to read 
     * @return  a cloned query instance with the modified behavior
     */
    default ListReadWithColumns<T, R> columnWithMetadata(final ColumnName<?> name) {
        return columnWithMetadata(name.getName());
    }
    
    /**
     * @param names  the column names to read 
     * @return  a cloned query instance with the modified behavior
     */
    default ListReadWithColumns<T, R> columns(final ColumnName<?>... names) {
        final List<String> ns = Lists.newArrayList();
        for (ColumnName<?> name : names) {
            ns.add(name.getName());
        }
        return columns(ImmutableList.copyOf(ns));
    }
}
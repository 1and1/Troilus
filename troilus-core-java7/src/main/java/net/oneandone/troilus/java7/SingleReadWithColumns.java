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



/**
 * Single column-aware read query 
 *
 * @param <T>  the result type
 */
public interface SingleReadWithColumns<T, R> extends SingleRead<T, R> {

    /**
     * @param name  the column name to read 
     * @return  a cloned query instance with the modified behavior
     */
    SingleReadWithColumns<T, R> column(String name);

    /**
     * @param name  the column name incl. meta data to read 
     * @return  a cloned query instance with the modified behavior
     */
    SingleReadWithColumns<T, R> columnWithMetadata(String name);

    /**
     * @param names  the column names to read 
     * @return  a cloned query instance with the modified behavior
     */
    SingleReadWithColumns<T, R> columns(String... names);

    /**
     * @param name  the column name to read 
     * @return  a cloned query instance with the modified behavior
     */
    SingleReadWithColumns<T, R> column(ColumnName<?> name);

    /**
     * @param name  the column name incl. meta data to read 
     * @return  a cloned query instance with the modified behavior
     */
    SingleReadWithColumns<T, R> columnWithMetadata(ColumnName<?> name);

    /**
     * @param names  the column names to read 
     * @return  a cloned query instance with the modified behavior
     */
    SingleReadWithColumns<T, R> columns(ColumnName<?>... names);
}

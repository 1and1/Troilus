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

import net.oneandone.troilus.ColumnName;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.policies.RetryPolicy;
import com.datastax.driver.core.querybuilder.Clause;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;



/**
 * The data access object
 */
public interface Dao {
    
    /**
     * @param consistencyLevel  the consistency level to use
     * @return a cloned Dao instance with the modified behavior
     */
    Dao withConsistency(ConsistencyLevel consistencyLevel);

    /**
     * @param serialConsistencyLevel   the serial consistency level to use
     * @return a cloned Dao instance with the modified behavior
     */
    Dao withSerialConsistency(ConsistencyLevel serialConsistencyLevel);

    /**
     * @return a cloned Dao instance with activated tracking
     */
    Dao withTracking();

    /**
     * @return a cloned Dao instance with deactivated tracking 
     */
    Dao withoutTracking();

    /**
     * @param policy  the retry policy
     * @return a cloned Dao instance with the modified behavior
     */
    Dao withRetryPolicy(RetryPolicy policy);

    
    
    ////////////////////////////////
    // MUTATIONS

    
    /**
     * @param clauses the clauses 
     * @return a write query 
     */
    UpdateWithUnitAndCounter writeWhere(Clause... clauses);

    /**
     * @param entity the entity to write
     * @return a write 
     */
    Insertion writeEntity(Object entity);

    /**
     * @param composedKeyParts the composed key 
     * @return the write query 
     */
    WriteWithCounter writeWithKey(ImmutableMap<String, Object> composedKeyParts);
    
    /**
     * @param keyName   the key name
     * @param keyValue  the key value
     * @return the write query 
     */
    default WriteWithCounter writeWithKey(final String keyName, final Object keyValue) {
        return writeWithKey(ImmutableMap.of(keyName, keyValue));
    }

    /**
     * @param composedKeyNamePart1   the composed key name 1
     * @param composedKeyValuePart1  the composed key value 1
     * @param composedKeyNamePart2   the composed key name 2
     * @param composedKeyValuePart2  the composed key value 2
     * @return the write query
     */
    default WriteWithCounter writeWithKey(final String composedKeyNamePart1, final Object composedKeyValuePart1,
                                          final String composedKeyNamePart2, final Object composedKeyValuePart2) {
        return writeWithKey(ImmutableMap.of(composedKeyNamePart1, composedKeyValuePart1,
                                            composedKeyNamePart2, composedKeyValuePart2));
    }
    
    /**
     * @param composedKeyNamePart1   the composed key name 1
     * @param composedKeyValuePart1  the composed key value 1
     * @param composedKeyNamePart2   the composed key name 2
     * @param composedKeyValuePart2  the composed key value 2
     * @param composedKeyNamePart3   the composed key name 3
     * @param composedKeyValuePart3  the composed key value 3
     * @return the write query
     */
    default WriteWithCounter writeWithKey(final String composedKeyNamePart1, final Object composedKeyValuePart1,
                                          final String composedKeyNamePart2, final Object composedKeyValuePart2, 
                                          final String composedKeyNamePart3, final Object composedKeyValuePart3) {
        return writeWithKey(ImmutableMap.of(composedKeyNamePart1, composedKeyValuePart1, 
                                            composedKeyNamePart2, composedKeyValuePart2, 
                                            composedKeyNamePart3, composedKeyValuePart3));
    }
        
    /**
     * @param keyName   the key name 
     * @param keyValue  the key value
     * @param <T> the name type
     * @return the write query 
     */
    default <T> WriteWithCounter writeWithKey(final ColumnName<T> keyName, final T keyValue) {
        return writeWithKey(keyName.getName(), (Object) keyValue); 
    }
        
    /**
     * @param composedKeyNamePart1   the composed key name 1
     * @param composedKeyValuePart1  the composed key value 1
     * @param composedKeyNamePart2   the composed key name 2
     * @param composedKeyValuePart2  the composed key value 2
     * @param <T> the name 1 type 
     * @param <E> the name 2 type 
     * @return the write query
     */
    default <T, E> WriteWithCounter writeWithKey(final ColumnName<T> composedKeyNamePart1, final T composedKeyValuePart1,
                                                 final ColumnName<E> composedKeyNamePart2, final E composedKeyValuePart2) {
        return writeWithKey(composedKeyNamePart1.getName(), (Object) composedKeyValuePart1,
                            composedKeyNamePart2.getName(), (Object) composedKeyValuePart2); 
    }


    /**
     * @param composedKeyNamePart1   the composed key name 1
     * @param composedKeyValuePart1  the composed key value 1
     * @param composedKeyNamePart2   the composed key name 2
     * @param composedKeyValuePart2  the composed key value 2
     * @param composedKeyNamePart3   the composed key name 3
     * @param composedKeyValuePart3  the composed key value 3
     * @param <T> the name 1 type 
     * @param <E> the name 2 type 
     * @param <F> the name 3 type
     * @return the write query
     */
    default <T, E, F> WriteWithCounter writeWithKey(final ColumnName<T> composedKeyNamePart1, final T composedKeyValuePart1, 
                                                    final ColumnName<E> composedKeyNamePart2, final E composedKeyValuePart2, 
                                                    final ColumnName<F> composedKeyNamePart3, final F composedKeyValuePart3) {
        return writeWithKey(composedKeyNamePart1.getName(), (Object) composedKeyValuePart1,
                            composedKeyNamePart2.getName(), (Object) composedKeyValuePart2,
                            composedKeyNamePart3.getName(), (Object) composedKeyValuePart3); 
    }
    
    /**
     * @param composedKeyParts  the composed key
     * @return the delete query 
     */
    Deletion deleteWithKey(ImmutableMap<String, Object> composedKeyParts);
    
    /**
     * @param keyname   the key name 
     * @param keyValue  the key value
     * @return the delete query 
     */
    default Deletion deleteWithKey(final String keyname, final Object keyValue) {
        return deleteWithKey(ImmutableMap.of(keyname, keyValue));
    }

    /**
     * @param composedKeyNamePart1   the key name 1
     * @param composedKeyValuePart1  the key value 1
     * @param composedKeyNamePart2   the key name 2
     * @param composedKeyValuePart2  the key value 2
     * @return the delete query
     */
    default Deletion deleteWithKey(final String composedKeyNamePart1, final Object composedKeyValuePart1, 
                                   final String composedKeyNamePart2, final Object composedKeyValuePart2) {
        return deleteWithKey(ImmutableMap.of(composedKeyNamePart1, composedKeyValuePart1, 
                                             composedKeyNamePart2, composedKeyValuePart2));
    }
        
    /**
     * @param composedKeyNamePart1    the key name 1
     * @param composedKeyValuePart1   the key value 1
     * @param composedKeyNamePart2    the key name 2
     * @param composedKeyValuePart2   the key value 2
     * @param composedKeyNamePart3    the key name 3
     * @param composedKeyValuePart3   the key value 3
     * @return the delete query 
     */
    default Deletion deleteWithKey(final String composedKeyNamePart1, final Object composedKeyValuePart1, 
                                   final String composedKeyNamePart2, final Object composedKeyValuePart2, 
                                   final String composedKeyNamePart3, final Object composedKeyValuePart3) {
        return deleteWithKey(ImmutableMap.of(composedKeyNamePart1, composedKeyValuePart1,
                                             composedKeyNamePart2, composedKeyValuePart2, 
                                             composedKeyNamePart3, composedKeyValuePart3));
    }
    
    /**
     * @param keyName  the key name 
     * @param keyValue the key value 
     * @param <T> the name type
     * @return the delete query
     */
    default <T> Deletion deleteWithKey(final ColumnName<T> keyName, final T keyValue) {
        return deleteWithKey(keyName.getName(), (Object) keyValue);
    }
        
    /**
     * @param composedKeyNamePart1   the key name 1
     * @param composedKeyValuePart1  the key value 1
     * @param composedKeyNamePart2   the key name 2
     * @param composedKeyValuePart2  the key value 2
     * @param <T> the name 1 type 
     * @param <E> the name 2 type 
     * @return the delete query 
     */
    default <T, E> Deletion deleteWithKey(final ColumnName<T> composedKeyNamePart1, final T composedKeyValuePart1, 
                                          final ColumnName<E> composedKeyNamePart2, final E composedKeyValuePart2) {
        return deleteWithKey(composedKeyNamePart1.getName(), (Object) composedKeyValuePart1,
                             composedKeyNamePart2.getName(), (Object) composedKeyValuePart2);
    }
    
    /**
     * @param composedKeyNamePart1   the key name 1
     * @param composedKeyValuePart1  the key value 1
     * @param composedKeyNamePart2   the key name 2
     * @param composedKeyValuePart2  the key value 2
     * @param composedKeyNamePart3   the key name 3
     * @param composedKeyValuePart3  the key value 3
     * @param <T> the name 1 type 
     * @param <E> the name 2 type 
     * @param <F> the name 3 type  
     * @return the delete query
     */
    default <T, E, F> Deletion deleteWithKey(final ColumnName<T> composedKeyNamePart1, final T composedKeyValuePart1, 
                                             final ColumnName<E> composedKeyNamePart2, final E composedKeyValuePart2, 
                                             final ColumnName<F> composedKeyNamePart3, final F composedKeyValuePart3) {
        return deleteWithKey(composedKeyNamePart1.getName(), (Object) composedKeyValuePart1,
                             composedKeyNamePart2.getName(), (Object) composedKeyValuePart2,
                             composedKeyNamePart3.getName(), (Object) composedKeyValuePart3);
    }
    
    /**
     * @param whereConditions  the where conditions
     * @return the delete query
     */
    Deletion deleteWhere(Clause... whereConditions);
    
    
 
 
    
    
    
    ////////////////////////////////
    // READ

    /**
     * @param composedKeyParts the key parts
     * @return the read query
     */
    SingleReadWithUnit<Record, Record> readWithKey(ImmutableMap<String, Object> composedKeyParts);
        
    /**
     * @param keyName   the key
     * @param keyValue  the key value
     * @return the read query
     */
    default SingleReadWithUnit<Record, Record> readWithKey(final String keyName, final Object keyValue) {
        return readWithKey(ImmutableMap.of(keyName, keyValue));
    }
    
    /**
     * @param composedKeyNamePart1   the key name 1
     * @param composedKeyValuePart1  the value 1
     * @param composedKeyNamePart2   the key name 2
     * @param composedKeyValuePart2  the value 2
     * @return the read query
     */
    default SingleReadWithUnit<Record, Record> readWithKey(final String composedKeyNamePart1, final Object composedKeyValuePart1, 
                                                           final String composedKeyNamePart2, final Object composedKeyValuePart2) {
        return readWithKey(ImmutableMap.of(composedKeyNamePart1, composedKeyValuePart1, 
                                           composedKeyNamePart2, composedKeyValuePart2));
    }
    
    /**
     * @param composedKeyNamePart1   the key name 1
     * @param composedKeyValuePart1  the key value 1
     * @param composedKeyNamePart2   the key name 2
     * @param composedKeyValuePart2  the key value 2
     * @param composedKeyNamePart3   the key name 3
     * @param composedKeyValuePart3  the key value 3
     * @return the read query
     */
    default SingleReadWithUnit<Record, Record> readWithKey(final String composedKeyNamePart1, final Object composedKeyValuePart1, 
                                                           final String composedKeyNamePart2, final Object composedKeyValuePart2,
                                                           final String composedKeyNamePart3, final Object composedKeyValuePart3) {
        return readWithKey(ImmutableMap.of(composedKeyNamePart1, composedKeyValuePart1, 
                                           composedKeyNamePart2, composedKeyValuePart2, 
                                           composedKeyNamePart3, composedKeyValuePart3));
    }

    /**
     * @param keyName   the key name
     * @param keyValue  the key value
     * @return the read query
     */
    default <T> SingleReadWithUnit<Record, Record> readWithKey(final ColumnName<T> keyName, final T keyValue) {
        return readWithKey(keyName.getName(), (Object) keyValue);
    }
    
    /**
     * @param composedKeyNamePart1   the key name 1
     * @param composedKeyValuePart1  the value 1
     * @param composedKeyNamePart2   the key name 2
     * @param composedKeyValuePart2  the value 2
     * @return the read query
     */
    default <T, E> SingleReadWithUnit<Record, Record> readWithKey(final ColumnName<T> composedKeyNamePart1, final T composedKeyValuePart1, 
                                                                  final ColumnName<E> composedKeyNamePart2, final E composedKeyValuePart2) {
        return readWithKey(composedKeyNamePart1.getName(), (Object) composedKeyValuePart1,
                           composedKeyNamePart2.getName(), (Object) composedKeyValuePart2);
    }
    
    /**
     * @param composedKeyNamePart1   the key name 1
     * @param composedKeyValuePart1  the key value 1
     * @param composedKeyNamePart2   the key name 2
     * @param composedKeyValuePart2  the key value 2
     * @param composedKeyNamePart3   the key name 3
     * @param composedKeyValuePart3  the key value 3
     * @return the read query
     */
    default <T, E, F> SingleReadWithUnit<Record, Record> readWithKey(final ColumnName<T> composedKeyNamePart1, final T composedKeyValuePart1, 
                                                                     final ColumnName<E> composedKeyNamePart2, final E composedKeyValuePart2, 
                                                                     final ColumnName<F> composedKeyNamePart3, final F composedKeyValuePart3) {
        return readWithKey(composedKeyNamePart1.getName(), (Object) composedKeyValuePart1,
                           composedKeyNamePart2.getName(), (Object) composedKeyValuePart2,                         
                           composedKeyNamePart3.getName(), (Object) composedKeyValuePart3);
    }
    
    /**
     * @param keys the keys
     * @return the read query
     */
    ListReadWithUnit<ResultList<Record>, Record> readSequenceWithKeys(final ImmutableMap<String, ImmutableList<Object>> keys);    

    /**
     * @param name    the key name 
     * @param values  the key values
     * @return the read query
     */
    default ListReadWithUnit<ResultList<Record>, Record> readSequenceWithKeys(final String name, final ImmutableList<Object> values) {
        return readSequenceWithKeys(ImmutableMap.of(name, values));
    }
   
    /**
     * @param composedKeyNamePart1   the key name 1
     * @param composedKeyValuePart1  the key value 1 
     * @param composedKeyNamePart2   the key name 2
     * @param composedKeyValuesPart2 the key values 2
     * @return the read query
     */
    default ListReadWithUnit<ResultList<Record>, Record> readSequenceWithKeys(final String composedKeyNamePart1, final Object composedKeyValuePart1, 
                                                                              final String composedKeyNamePart2, final ImmutableList<Object> composedKeyValuesPart2) {
        return readSequenceWithKeys(ImmutableMap.of(composedKeyNamePart1, ImmutableList.of(composedKeyValuePart1),
                                                    composedKeyNamePart2, composedKeyValuesPart2));
    }
   
    /**
     * @param composedKeyNamePart1    the key name 1
     * @param composedKeyValuePart1   the key value 1
     * @param composedKeyNamePart2    the key name 2
     * @param composedKeyValuePart2   the key value 2
     * @param composedKeyNamePart3    the key name 3
     * @param composedKeyValuesPart3  the key values 3
     * @return
     */
    default ListReadWithUnit<ResultList<Record>, Record> readSequenceWithKeys(final String composedKeyNamePart1, final Object composedKeyValuePart1, 
                                                                              final String composedKeyNamePart2, final Object composedKeyValuePart2,
                                                                              final String composedKeyNamePart3, final ImmutableList<Object> composedKeyValuesPart3) {
        return readSequenceWithKeys(ImmutableMap.of(composedKeyNamePart1, ImmutableList.of(composedKeyValuePart1),
                                                    composedKeyNamePart2, ImmutableList.of(composedKeyValuePart2),
                                                    composedKeyNamePart3, composedKeyValuesPart3));        
    }
    
    /**
     * @param name    the key name 
     * @param values  the values
     * @return the read query
     */
    @SuppressWarnings("unchecked")
    default <T> ListReadWithUnit<ResultList<Record>, Record> readSequenceWithKeys(final ColumnName<T> name, final ImmutableList<T> values) {
        return readSequenceWithKeys(name.getName(), (ImmutableList<Object>) values);
    }

    /**
     * @param composedKeyNamePart1      key name 1 
     * @param composedKeyValuePart1     key value 1
     * @param composedKeyNamePart2      key name 2 
     * @param composedKeyValuesPart2    key value 2
     * @return the read query
     */
    @SuppressWarnings("unchecked")
    default <T, E> ListReadWithUnit<ResultList<Record>, Record> readSequenceWithKeys(final ColumnName<T> composedKeyNamePart1, final T composedKeyValuePart1, 
                                                                                     final ColumnName<E> composedKeyNamePart2, final ImmutableList<E> composedKeyValuesPart2) {
        return readSequenceWithKeys(composedKeyNamePart1.getName(), (Object) composedKeyValuePart1,
                                    composedKeyNamePart2.getName(), (ImmutableList<Object>) composedKeyValuesPart2);
    }

    /**
     * @param composedKeyNamePart1      key name 1 
     * @param composedKeyValuePart1     key value 1
     * @param composedKeyNamePart2      key name 2 
     * @param composedKeyValuesPart2    key value 2
     * @param composedKeyNamePart2      key name 3 
     * @param composedKeyValuesPart2    key value 3
     * @return the read query
     */
    @SuppressWarnings("unchecked")
    default <T, E, F> ListReadWithUnit<ResultList<Record>, Record> readSequenceWithKeys(final ColumnName<T> composedKeyNamePart1, final T composedKeyValuePart1, 
                                                                                        final ColumnName<E> composedKeyNamePart2, final E composedKeyValuePart2,
                                                                                        final ColumnName<F> composedKeyNamePart3, final ImmutableList<F> composedKeyValuesPart3) {
        return readSequenceWithKeys(composedKeyNamePart1.getName(), (Object) composedKeyValuePart1,
                                    composedKeyNamePart2.getName(), (Object) composedKeyValuePart2,
                                    composedKeyNamePart3.getName(), (ImmutableList<Object>) composedKeyValuesPart3);
    }

    /**
     * @param key   the key
     * @return the read query
     */
    ListReadWithUnit<ResultList<Record>, Record> readSequenceWithKey(ImmutableMap<String, ImmutableList<Object>> key);
    
    /**
     * @param composedKeyNamePart1  the key name 
     * @param composedKeyValuePart1 the key values
     * @return the read query
     */
    default ListReadWithUnit<ResultList<Record>, Record> readSequenceWithKey(final String composedKeyNamePart1, final Object composedKeyValuePart1) {
        return readSequenceWithKey(ImmutableMap.of(composedKeyNamePart1, ImmutableList.of(composedKeyValuePart1)));
    }
    
    /**
     * @param composedKeyNamePart1  the key name part 1 
     * @param composedKeyValuePart1 the key part value 1
     * @param composedKeyNamePart2  the key name part 2 
     * @param composedKeyValuePart2 the key part value 2
     * @return the read query
     */
    default ListReadWithUnit<ResultList<Record>, Record> readSequenceWithKey(final String composedKeyNamePart1, final Object composedKeyValuePart1, 
                                                                             final String composedKeyNamePart2, final Object composedKeyValuePart2) {
        return readSequenceWithKey(ImmutableMap.of(composedKeyNamePart1, ImmutableList.of(composedKeyValuePart1),
                                                   composedKeyNamePart2, ImmutableList.of(composedKeyValuePart2)));
    }

    /**
     * @param name   the key name
     * @param value  the key value
     * @return
     */
    default <T> ListReadWithUnit<ResultList<Record>, Record> readSequenceWithKey(final ColumnName<T> name, final T value) {
        return readSequenceWithKey(name.getName(), (Object) value);
    }
    
    /**
     * @param composedKeyNamePart1    the key name 1
     * @param composedKeyValuePart1   the key value 1
     * @param composedKeyNamePart2    the key name 2
     * @param composedKeyValuePart2   the key value 2
     * @return the read query
     */
    default <T, E> ListReadWithUnit<ResultList<Record>, Record> readSequenceWithKey(final ColumnName<T> composedKeyNamePart1, final T composedKeyValuePart1, 
                                                                                    final ColumnName<E> composedKeyNamePart2, final E composedKeyValuePart2) {
        return readSequenceWithKey(composedKeyNamePart1.getName(), (Object) composedKeyValuePart1,
                                   composedKeyNamePart2.getName(), (Object) composedKeyValuePart2);
    }
    
    /**
     * @return the read query
     */
    ListReadWithUnit<ResultList<Record>, Record> readSequence();

    /**
     * @param clauses  the clauses
     * @return the read query
     */
    ListReadWithUnit<ResultList<Record>, Record> readSequenceWhere(Clause... clauses);
}

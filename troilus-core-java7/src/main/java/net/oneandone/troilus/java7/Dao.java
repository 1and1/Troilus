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
import net.oneandone.troilus.interceptor.QueryInterceptor;




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

    /**
     * @param queryInterceptor   the interceptor
     * @return a cloned Dao instance with the modified behavior
     */
    Dao withInterceptor(QueryInterceptor queryInterceptor);
    

    


    
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
    WriteWithCounter writeWithKey(String keyName, Object keyValue);

    /**
     * @param composedKeyNamePart1   the composed key name 1
     * @param composedKeyValuePart1  the composed key value 1
     * @param composedKeyNamePart2   the composed key name 2
     * @param composedKeyValuePart2  the composed key value 2
     * @return the write query
     */
    WriteWithCounter writeWithKey(String composedKeyNamePart1, Object composedKeyValuePart1,
                                  String composedKeyNamePart2, Object composedKeyValuePart2);

    /**
     * @param composedKeyNamePart1   the composed key name 1
     * @param composedKeyValuePart1  the composed key value 1
     * @param composedKeyNamePart2   the composed key name 2
     * @param composedKeyValuePart2  the composed key value 2
     * @param composedKeyNamePart3   the composed key name 3
     * @param composedKeyValuePart3  the composed key value 3
     * @return the write query
     */
    WriteWithCounter writeWithKey(String composedKeyNamePart1, Object composedKeyValuePart1,
                                  String composedKeyNamePart2, Object composedKeyValuePart2, 
                                  String composedKeyNamePart3, Object composedKeyValuePart3);

    /**
     * @param keyName   the key name 
     * @param keyValue  the key value
     * @param <T> the name type
     * @return the write query 
     */
    <T> WriteWithCounter writeWithKey(ColumnName<T> keyName, T keyValue);

    /**
     * @param composedKeyNamePart1   the composed key name 1
     * @param composedKeyValuePart1  the composed key value 1
     * @param composedKeyNamePart2   the composed key name 2
     * @param composedKeyValuePart2  the composed key value 2
     * @param <T> the name 1 type 
     * @param <E> the name 2 type 
     * @return the write query
     */
    <T, E> WriteWithCounter writeWithKey(ColumnName<T> composedKeyNamePart1, T composedKeyValuePart1,
                                         ColumnName<E> composedKeyNamePart2, E composedKeyValuePart2);

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
    <T, E, F> WriteWithCounter writeWithKey(ColumnName<T> composedKeyNamePart1, T composedKeyValuePart1, 
                                            ColumnName<E> composedKeyNamePart2, E composedKeyValuePart2, 
                                            ColumnName<F> composedKeyNamePart3, F composedKeyValuePart3);


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
    Deletion deleteWithKey(String keyname, Object keyValue);

    /**
     * @param composedKeyNamePart1   the key name 1
     * @param composedKeyValuePart1  the key value 1
     * @param composedKeyNamePart2   the key name 2
     * @param composedKeyValuePart2  the key value 2
     * @return the delete query
     */
    Deletion deleteWithKey(String composedKeyNamePart1, Object composedKeyValuePart1, 
                           String composedKeyNamePart2, Object composedKeyValuePart2);

    /**
     * @param composedKeyNamePart1    the key name 1
     * @param composedKeyValuePart1   the key value 1
     * @param composedKeyNamePart2    the key name 2
     * @param composedKeyValuePart2   the key value 2
     * @param composedKeyNamePart3    the key name 3
     * @param composedKeyValuePart3   the key value 3
     * @return the delete query 
     */
    Deletion deleteWithKey(String composedKeyNamePart1, Object composedKeyValuePart1, 
                           String composedKeyNamePart2, Object composedKeyValuePart2, 
                           String composedKeyNamePart3, Object composedKeyValuePart3);

    /**
     * @param keyName  the key name 
     * @param keyValue the key value 
     * @param <T> the name type
     * @return the delete query
     */
    <T> Deletion deleteWithKey(ColumnName<T> keyName, T keyValue);

    /**
     * @param composedKeyNamePart1   the key name 1
     * @param composedKeyValuePart1  the key value 1
     * @param composedKeyNamePart2   the key name 2
     * @param composedKeyValuePart2  the key value 2
     * @param <T> the name 1 type 
     * @param <E> the name 2 type 
     * @return the delete query 
     */
    <T, E> Deletion deleteWithKey(ColumnName<T> composedKeyNamePart1, T composedKeyValuePart1, 
                                  ColumnName<E> composedKeyNamePart2, E composedKeyValuePart2);

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
    <T, E, F> Deletion deleteWithKey(ColumnName<T> composedKeyNamePart1, T composedKeyValuePart1, 
                                     ColumnName<E> composedKeyNamePart2, E composedKeyValuePart2, 
                                     ColumnName<F> composedKeyNamePart3, F composedKeyValuePart3);

    /**
     * @param whereConditions  the where conditions
     * @return the delete query
     */
    Deletion deleteWhere(Clause... whereConditions);

    
    
    

  
    

  

    
 

    
    
 
    

    
 
    


  
    
 

    
     
   
    
  

   
 

    
 


    
    
    ////////////////////////////////
    // READ

    SingleReadWithUnit<Record, Record> readWithKey(ImmutableMap<String, Object> composedKeyParts);
    
    SingleReadWithUnit<Record, Record> readWithKey(String keyName, Object keyValue);

    SingleReadWithUnit<Record, Record> readWithKey(String composedKeyNamePart1, Object composedKeyValuePart1, 
                                           String composedKeyNamePart2, Object composedKeyValuePart2);

    SingleReadWithUnit<Record, Record> readWithKey(String composedKeyNamePart1, Object composedKeyValuePart1, 
                                           String composedKeyNamePart2, Object composedKeyValuePart2,
                                           String composedKeyNamePart3, Object composedKeyValuePart3);


    <T> SingleReadWithUnit<Record, Record> readWithKey(ColumnName<T> keyName, T keyValue);

    <T, E> SingleReadWithUnit<Record, Record> readWithKey(ColumnName<T> composedKeyNamePart1, T composedKeyValuePart1, 
                                                  ColumnName<E> composedKeyNamePart2, E composedKeyValuePart2);

    <T, E, F> SingleReadWithUnit<Record, Record> readWithKey(ColumnName<T> composedKeyNamePart1, T composedKeyValuePart1, 
                                                     ColumnName<E> composedKeyNamePart2, E composedKeyValuePart2, 
                                                     ColumnName<F> composedKeyNamePart3, F composedKeyValuePart3);

    ListReadWithUnit<ResultList<Record>, Record> readSequenceWithKeys(String name, ImmutableList<Object> values);

    ListReadWithUnit<ResultList<Record>, Record> readSequenceWithKeys(String composedKeyNamePart1, Object composedKeyValuePart1, 
                                              String composedKeyNamePart2, ImmutableList<Object> composedKeyValuesPart2);

    ListReadWithUnit<ResultList<Record>, Record> readSequenceWithKeys(String composedKeyNamePart1, Object composedKeyValuePart1, 
                                              String composedKeyNamePart2, Object composedKeyValuePart2,
                                              String composedKeyNamePart3, ImmutableList<Object> composedKeyValuesPart3);

    
    <T> ListReadWithUnit<ResultList<Record>, Record> readSequenceWithKeys(ColumnName<T> name, ImmutableList<T> values);

    <T, E> ListReadWithUnit<ResultList<Record>, Record> readSequenceWithKeys(ColumnName<T> composedKeyNamePart1, T composedKeyValuePart1, 
                                                     ColumnName<E> composedKeyNamePart2, ImmutableList<E> composedKeyValuesPart2);

    <T, E, F> ListReadWithUnit<ResultList<Record>, Record> readSequenceWithKeys(ColumnName<T> composedKeyNamePart1, T composedKeyValuePart1, 
                                                        ColumnName<E> composedKeyNamePart2, E composedKeyValuePart2,
                                                        ColumnName<F> composedKeyNamePart3, ImmutableList<F> composedKeyValuesPart3);
    
    ListReadWithUnit<ResultList<Record>, Record> readSequenceWithKey(String composedKeyNamePart1, Object composedKeyValuePart1);

    ListReadWithUnit<ResultList<Record>, Record> readSequenceWithKey(String composedKeyNamePart1, Object composedKeyValuePart1, 
                                                     String composedKeyNamePart2, Object composedKeyValuePart2);

    <T> ListReadWithUnit<ResultList<Record>, Record> readSequenceWithKey(ColumnName<T> name, T value);

    <T, E> ListReadWithUnit<ResultList<Record>, Record> readSequenceWithKey(ColumnName<T> composedKeyNamePart1, T composedKeyValuePart1, 
                                                            ColumnName<E> composedKeyNamePart2, E composedKeyValuePart2);
    
    ListReadWithUnit<ResultList<Record>, Record> readSequence();

    ListReadWithUnit<ResultList<Record>, Record> readSequenceWhere(Clause... clauses);
   }

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

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import net.oneandone.troilus.Count;
import net.oneandone.troilus.ColumnName;
import net.oneandone.troilus.Result;
import net.oneandone.troilus.interceptor.QueryInterceptor;

import org.reactivestreams.Publisher;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Statement;
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
    
    
    
    /**
     * The Query 
     * @param <T>  the result type
     */
    public interface Query<T> {

        /**
         * performs the query in an async way 
         * @return the result future 
         */
        CompletableFuture<T> executeAsync();

        /**
         * performs the query in a sync way 
         * @return the result
         */
        T execute();
    }

    
    
    /**
     * @param <Q> the response type
     * @param <R> the query type
     */
    public static interface ConfiguredQuery<Q, R> extends Query<R> {

        /**
         * @param consistencyLevel  the consistency level to use
         * @return a cloned query instance with the modified behavior
         */
        Q withConsistency(ConsistencyLevel consistencyLevel);

        /**
         * @return a cloned query instance with activated tracking
         */
        Q withEnableTracking();

        /**
         * @return a cloned query instance with deactivated tracking
         */
        Q withDisableTracking();
        
        /**
         * @param policy  the retry policy
         * @return a cloned query instance with the modified behavior
         */
        Q withRetryPolicy(RetryPolicy policy);
    }

    
    
    
    ////////////////////////////////
    // MUTATIONS

    
    /**
     * @param clauses the clauses 
     * @return a write query 
     */
    UpdateWithValuesAndCounter writeWhere(Clause... clauses);

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
    
    
    /**
     * @param <Q> the query type
     */
    public static interface Mutation<Q extends Mutation<Q>> extends ConfiguredQuery<Q, Result> {

        /**
          * @param consistencyLevel  the consistency level to use
          * @return a cloned query instance with the modified behavior
         */
        Mutation<Q> withSerialConsistency(ConsistencyLevel consistencyLevel);

        /**
         * @param ttl  the time-to-live set
         * @return a cloned query instance with the modified behavior
         */
        Mutation<Q> withTtl(Duration ttl);

        /**
         * @param microsSinceEpoch  the writetime in since epoch to set
         * @return a cloned query instance with the modified behavior
         */
        Mutation<Q> withWritetime(long microsSinceEpoch);
    }

    
    /**
     * Batchable mutation query
     */
    public static interface Batchable {

        /**
         * @return the statement future
         */
        CompletableFuture<Statement> getStatementAsync();
    }


    /**
     * Batchable mutation
     * @param <Q>  the query
     */
    public static interface BatchableMutation<Q extends BatchableMutation<Q>> extends Mutation<BatchableMutation<Q>>, Batchable {

        /**
         * @param other  the other query to combine with
         * @return a cloned query instance with the modified behavior
         */
        BatchMutation combinedWith(Batchable other);
    }

    /**
     * BatchMutation
     */
    public interface BatchMutation extends ConfiguredQuery<BatchMutation, Result> {

        /**
         * @param other  the other query to combine with
         * @return a cloned query instance with the modified behavior
         */
        BatchMutation combinedWith(Batchable other);

        /**
         * @return a cloned query instance with write ahead log
         */
        Query<Result> withWriteAheadLog();

        /**
         * @return a cloned query instance without write ahead log
         */
        Query<Result> withoutWriteAheadLog();
    }

    
    /**
     * insertion query
     */
    public static interface Insertion extends BatchableMutation<Insertion> {

        /**
         * @return a cloned query instance with lwt (if-not-exits)
         */
        Mutation<?> ifNotExists();
    }
  

    /**
     * update query
     * @param <U> the query type 
     */
    public static interface Update<U extends BatchableMutation<U>> extends BatchableMutation<U> {

        /**
         * @param conditions  the condtions
         * @return a cloned query instance with lwt (only-if)
         */
        Mutation<?> onlyIf(Clause... conditions);
    }

    
    /**
     * write query
     */
    public static interface Write extends UpdateWithValues<Write> {
     
        /**
         * @return a cloned query instance with lwt (if-not-exits)
         */
        Mutation<?> ifNotExists();
    }

    
    /**
     * Counter-aware write query
     */
    public static interface WithCounter {

        /**
         * @param name the name of the value to decrement
         * @return a cloned query instance with the modified behavior
         */
        CounterMutation decr(String name); 
        
        /**
         * @param name  the name of the value to decrement
         * @param value the value
         * @return a cloned query instance with the modified behavior
         */
        CounterMutation decr(String name, long value); 

        /**
         * @param name the name of the value to increment
         * @return a cloned query instance with the modified behavior 
         */
        CounterMutation incr(String name);  

        /**
         * @param name   the name  of the value to increment
         * @param value  the value
         * @return a cloned query instance with the modified behavior
         */
        CounterMutation incr(String name, long value);   
    }

    

    /**
     * write with counter
     */
    public static interface WriteWithCounter extends Write, WithCounter {

    }

    
    /**
     * value-aware update query
     * @param <U> the query type
     */    
    public static interface UpdateWithValues<U extends Update<U>> extends Update<U> {

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


    /**
     * values and counter aware update query
     */
    public static interface UpdateWithValuesAndCounter extends UpdateWithValues<Write>, WithCounter {
 
    }

    /**
     * counter batchable query
     */
    public static interface CounterBatchable {

        /**
         * @return a cloned query instance with the modified behavior
         */
        CompletableFuture<Statement> getStatementAsync();
    }

    
    /**
     * counter mutation
     */
    public static interface CounterMutation extends ConfiguredQuery<CounterMutation, Result>, CounterBatchable  {

        /**
         * @param consistencyLevel the consistency level to use
         * @return a cloned query instance with the modified behavior
         */
        CounterMutation withSerialConsistency(ConsistencyLevel consistencyLevel);

        /**
         * @param ttl  the time-to-live in sec to set
         * @return a cloned query instance with the modified behavior
         */
        CounterMutation withTtl(Duration ttl);

        /**
         * @param microsSinceEpoch the writetime in since epoch to set
         * @return a cloned query instance with the modified behavior
         */
        CounterMutation withWritetime(long microsSinceEpoch);

        /**
         * @param other  the other query to combine with
         * @return a cloned query instance with the modified behavior
         */
        CounterBatchMutation combinedWith(CounterBatchable other);
    }

        /**
     * counter-aware batch mutation query  
     */
    public interface CounterBatchMutation extends ConfiguredQuery<CounterBatchMutation, Result> {

        /**
         * @param other  the other query to combine with
         * @return a cloned query instance with the modified behavior
         */
        CounterBatchMutation combinedWith(CounterBatchable other);
    }
    

    /**
     * delete query
     */
    public static interface Deletion extends BatchableMutation<Deletion> {

        /**
         * @param conditions  the conditions
         * @return a cloned query instance with lwt (only-if)
         */
        Mutation<?> onlyIf(Clause... conditions);
        
        /**
         * @return a cloned query instance with lwt (if-exits)
         */
        Mutation<?> ifExists();
    }


    
    
    
    
    ////////////////////////////////
    // READ

    SingleReadWithUnit<Optional<Record>> readWithKey(ImmutableMap<String, Object> composedKeyParts);
    
    SingleReadWithUnit<Optional<Record>> readWithKey(String keyName, Object keyValue);

    SingleReadWithUnit<Optional<Record>> readWithKey(String composedKeyNamePart1, Object composedKeyValuePart1, 
                                                     String composedKeyNamePart2, Object composedKeyValuePart2);

    SingleReadWithUnit<Optional<Record>> readWithKey(String composedKeyNamePart1, Object composedKeyValuePart1, 
                                                     String composedKeyNamePart2, Object composedKeyValuePart2,
                                                     String composedKeyNamePart3, Object composedKeyValuePart3);


    <T> SingleReadWithUnit<Optional<Record>> readWithKey(ColumnName<T> keyName, T keyValue);

    <T, E> SingleReadWithUnit<Optional<Record>> readWithKey(ColumnName<T> composedKeyNamePart1, T composedKeyValuePart1, 
                                                            ColumnName<E> composedKeyNamePart2, E composedKeyValuePart2);

    <T, E, F> SingleReadWithUnit<Optional<Record>> readWithKey(ColumnName<T> composedKeyNamePart1, T composedKeyValuePart1, 
                                                               ColumnName<E> composedKeyNamePart2, E composedKeyValuePart2, 
                                                               ColumnName<F> composedKeyNamePart3, F composedKeyValuePart3);
    
    ListReadWithUnit<RecordList> readListWithKeys(String name, ImmutableList<Object> values);

    ListReadWithUnit<RecordList> readListWithKeys(String composedKeyNamePart1, Object composedKeyValuePart1, 
                                                  String composedKeyNamePart2, ImmutableList<Object> composedKeyValuesPart2);

    ListReadWithUnit<RecordList> readListWithKeys(String composedKeyNamePart1, Object composedKeyValuePart1, 
                                                  String composedKeyNamePart2, Object composedKeyValuePart2,
                                                  String composedKeyNamePart3, ImmutableList<Object> composedKeyValuesPart3);

    
    <T> ListReadWithUnit<RecordList> readListWithKeys(ColumnName<T> name, ImmutableList<T> values);

    <T, E> ListReadWithUnit<RecordList> readListWithKeys(ColumnName<T> composedKeyNamePart1, T composedKeyValuePart1, 
                                                         ColumnName<E> composedKeyNamePart2, ImmutableList<E> composedKeyValuesPart2);

    <T, E, F> ListReadWithUnit<RecordList> readListWithKeys(ColumnName<T> composedKeyNamePart1, T composedKeyValuePart1, 
                                                            ColumnName<E> composedKeyNamePart2, E composedKeyValuePart2,
                                                            ColumnName<F> composedKeyNamePart3, ImmutableList<F> composedKeyValuesPart3);
    
    ListReadWithUnit<RecordList> readListWithKey(String composedKeyNamePart1, Object composedKeyValuePart1);

    ListReadWithUnit<RecordList> readListWithKey(String composedKeyNamePart1, Object composedKeyValuePart1, 
                                                 String composedKeyNamePart2, Object composedKeyValuePart2);

    <T> ListReadWithUnit<RecordList> readListWithKey(ColumnName<T> name, T value);

    <T, E> ListReadWithUnit<RecordList> readListWithKey(ColumnName<T> composedKeyNamePart1, T composedKeyValuePart1, 
                                                        ColumnName<E> composedKeyNamePart2, E composedKeyValuePart2);

    
    ListReadWithUnit<RecordList> readAll();

    ListReadWithUnit<RecordList> readWhere(Clause... clauses);

    
    
        
    public static interface RecordList extends Result, Iterable<Record>, Publisher<Record> {   
        
    }

    
    
    
    public static interface SingleRead<T> extends Query<T> {

        SingleRead<T> withEnableTracking();

        SingleRead<T> withDisableTracking();

        SingleRead<T> withConsistency(ConsistencyLevel consistencyLevel);
    }

    public static interface SingleReadWithColumns<T> extends SingleRead<T> {

        SingleReadWithColumns<T> column(String name);

        SingleReadWithColumns<T> columnWithMetadata(String name);

        SingleReadWithColumns<T> columns(String... names);
        
        SingleReadWithColumns<T> column(ColumnName<?> name);

        SingleReadWithColumns<T> columnWithMetadata(ColumnName<?> name);

        SingleReadWithColumns<T> columns(ColumnName<?>... names);

    }

    public static interface SingleReadWithUnit<T> extends SingleReadWithColumns<T> {

        SingleRead<T> all();

        <E> SingleRead<Optional<E>> asEntity(Class<E> objectClass);
    }

    
       
    public static interface ListRead<T> extends SingleRead<T> {

        ListRead<T> withLimit(int limit);

        ListRead<T> withFetchSize(int fetchSize);

        ListRead<T> withDistinct();

        ListRead<T> withAllowFiltering();
    }

    public static interface ListReadWithColumns<T> extends ListRead<T> {

        ListReadWithColumns<T> column(String name);

        ListReadWithColumns<T> columnWithMetadata(String name);

        ListReadWithColumns<T> columns(String... names);

        ListReadWithColumns<T> column(ColumnName<?> name);

        ListReadWithColumns<T> columnWithMetadata(ColumnName<?> name);

        ListReadWithColumns<T> columns(ColumnName<?>... names);
    }
    
    
    /**
     * ListReadWithUnit
     * @param <T>  the result type
     */
    public static interface ListReadWithUnit<T> extends ListReadWithColumns<T> {

        ListRead<T> all();

        ListRead<Count> count();

        <E> ListRead<EntityList<E>> asEntity(Class<E> objectClass);
    }


    /**
     * EntityList
     * @param <E>  the entity type
     */
    public static interface EntityList<E> extends Result, Iterable<E>, Publisher<E> {

    }
}

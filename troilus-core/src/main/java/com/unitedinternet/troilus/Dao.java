/*
 * Copyright (c) 2014 1&1 Internet AG, Germany, http://www.1und1.de
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *  http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.unitedinternet.troilus;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.policies.RetryPolicy;
import com.datastax.driver.core.querybuilder.Clause;
import com.google.common.collect.ImmutableMap;
import com.unitedinternet.troilus.interceptor.QueryInterceptor;
import com.unitedinternet.troilus.utils.Exceptions;

public interface Dao {
    
    Dao withConsistency(ConsistencyLevel consistencyLevel);

    Dao withSerialConsistency(ConsistencyLevel consistencyLevel);

    Dao withEnableTracking();

    Dao withDisableTracking();

    Dao withRetryPolicy(RetryPolicy policy);
        
    Dao withInterceptor(QueryInterceptor queryInterceptor);
    
    
    public interface Query<T> {

        CompletableFuture<T> executeAsync();

        default T execute() {
            try {
                return executeAsync().get(Long.MAX_VALUE, TimeUnit.DAYS);
            } catch (InterruptedException | ExecutionException
                    | TimeoutException e) {
                throw Exceptions.unwrapIfNecessary(e);
            }
        }
    }

    
    
    public static interface ConfiguredQuery<Q, R> extends Query<R> {

        Q withConsistency(ConsistencyLevel consistencyLevel);

        Q withEnableTracking();

        Q withDisableTracking();
        
        Q withRetryPolicy(RetryPolicy policy);
    }

    
    
    
    ////////////////////////////////
    // MUTATIONS

    
    UpdateWithValuesAndCounter writeWhere(Clause... clauses);

    Insertion writeEntity(Object entity);

    WriteWithCounter writeWithKey(String keyName, Object keyValue);

    WriteWithCounter writeWithKey(String keyName1, Object keyValue1, String keyName2, Object keyValue2);

    WriteWithCounter writeWithKey(String keyName1, Object keyValue1, String keyName2, Object keyValue2, String keyName3, Object keyValue3);

    WriteWithCounter writeWithKey(String keyName1, Object keyValue1, String keyName2, Object keyValue2, String keyName3, Object keyValue3, String keyName4, Object keyValue4);

    <T> WriteWithCounter writeWithKey(Name<T> keyName, T keyValue);

    <T, E> WriteWithCounter writeWithKey(Name<T> keyName1, T keyValue1, Name<E> keyName2, E keyValue2);

    <T, E, F> WriteWithCounter writeWithKey(Name<T> keyName1, T keyValue1, Name<E> keyName2, E keyValue2, Name<F> keyName3, F keyValue3);

    <T, E, F, G> WriteWithCounter writeWithKey(Name<T> keyName1, T keyValue1, Name<E> keyName2, E keyValue2, Name<F> keyName3, F keyValue3, Name<G> keyName4, G keyValue4);

    
    
    Deletion deleteWithKey(String keyName, Object keyValue);

    Deletion deleteWithKey(String keyName1, Object keyValue1, String keyName2, Object keyValue2);

    Deletion deleteWithKey(String keyName1, Object keyValue1, String keyName2, Object keyValue2, String keyName3, Object keyValue3);

    Deletion deleteWithKey(String keyName1, Object keyValue1, String keyName2, Object keyValue2, String keyName3, Object keyValue3, String keyName4, Object keyValue4);

    <T> Deletion deleteWithKey(Name<T> keyName, T keyValue);

    <T, E> Deletion deleteWithKey(Name<T> keyName1, T keyValue1, Name<E> keyName2, E keyValue2);

    <T, E, F> Deletion deleteWithKey(Name<T> keyName1, T keyValue1, Name<E> keyName2, E keyValue2, Name<F> keyName3, F keyValue3);

    <T, E, F, G> Deletion deleteWithKey(Name<T> keyName1, T keyValue1, Name<E> keyName2, E keyValue2, Name<F> keyName3, F keyValue3, Name<G> keyName4, G keyValue4);

    Deletion deleteWhere(Clause... whereConditions);

    
    
    
    
    public static interface Mutation<Q extends Mutation<Q>> extends ConfiguredQuery<Q, Result> {

        Mutation<Q> withSerialConsistency(ConsistencyLevel consistencyLevel);

        Mutation<Q> withTtl(Duration ttl);

        Mutation<Q> withWritetime(long microsSinceEpoch);
    }
    
    
    public static interface Batchable {

        void addTo(BatchStatement batchStatement);
    }


    

    public static interface BatchableMutation<Q extends BatchableMutation<Q>> extends Mutation<BatchableMutation<Q>>, Batchable {

        BatchMutation combinedWith(Batchable other);
    }

    
    
    public interface BatchMutation extends ConfiguredQuery<BatchMutation, Result> {

        BatchMutation combinedWith(Batchable other);

        Query<Result> withLockedBatchType();

        Query<Result> withUnlockedBatchType();
    }

    
    
    
    public static interface Insertion extends BatchableMutation<Insertion> {

        Mutation<?> ifNotExits();
    }
  
    public static interface Update<U extends BatchableMutation<U>> extends BatchableMutation<U> {

        Mutation<?> onlyIf(Clause... conditions);
    }

    
    
    public static interface Write extends UpdateWithValues<Write> {
        
        Mutation<?> ifNotExits();
    }

    
    
    public static interface WithCounter {

        CounterMutation decr(String name); 
        
        CounterMutation decr(String name, long value); 
        
        CounterMutation incr(String name);  

        CounterMutation incr(String name, long value);   
    }

    

    public static interface WriteWithCounter extends Write, WithCounter {

    }

    
    
    public static interface UpdateWithValues<U extends Update<U>> extends Update<U> {

        U value(String name, Object value);

        U values(ImmutableMap<String, Object> nameValuePairsToAdd);
        
        <T> U value(Name<T> name, T value);
        
        <T> U value(SetName<T> name, Set<T> value);
        
        <T> U value(ListName<T> name, List<T> value);
        
        <K, V> U value(MapName<K, V> name, Map<K, V> value);
        
        U removeSetValue(String name, Object value);

        U addSetValue(String name, Object value);

        U appendListValue(String name, Object value);

        U prependListValue(String name, Object value);

        U removeListValue(String name, Object value);

        U putMapValue(String name, Object key, Object value);
    }


    
    public static interface UpdateWithValuesAndCounter extends UpdateWithValues<Write>, WithCounter {
 
    }

    
    public static interface CounterBatchable {

        void addTo(BatchStatement batchStatement);
    }

    
     
    
    public static interface CounterMutation extends ConfiguredQuery<CounterMutation, Result>, CounterBatchable  {

        CounterMutation withSerialConsistency(ConsistencyLevel consistencyLevel);

        CounterMutation withTtl(Duration ttl);

        CounterMutation withWritetime(long microsSinceEpoch);
        
        CounterBatchMutation combinedWith(CounterBatchable other);
    }

    

    public interface CounterBatchMutation extends ConfiguredQuery<CounterBatchMutation, Result> {

        CounterBatchMutation combinedWith(CounterBatchable other);
    }


    
    

    public static interface Deletion extends BatchableMutation<Deletion> {

        Mutation<?> onlyIf(Clause... conditions);
    }


    
    
    
    
    ////////////////////////////////
    // READ

    SingleReadWithUnit<Optional<Record>> readWithKey(String keyName, Object keyValue);

    SingleReadWithUnit<Optional<Record>> readWithKey(String keyName1, Object keyValue1, String keyName2, Object keyValue2);

    SingleReadWithUnit<Optional<Record>> readWithKey(String keyName1, Object keyValue1, String keyName2, Object keyValue2, String keyName3, Object keyValue3);

    SingleReadWithUnit<Optional<Record>> readWithKey(String keyName1, Object keyValue1, String keyName2, Object keyValue2, String keyName3, Object keyValue3, String keyName4, Object keyValue4);

    <T> SingleReadWithUnit<Optional<Record>> readWithKey(Name<T> keyName, T keyValue);

    <T, E> SingleReadWithUnit<Optional<Record>> readWithKey(Name<T> keyName1, T keyValue1, Name<E> keyName2, E keyValue2);

    <T, E, F> SingleReadWithUnit<Optional<Record>> readWithKey(Name<T> keyName1, T keyValue1, Name<E> keyName2, E keyValue2, Name<F> keyName3, F keyValue3);

    <T, E, F, G> SingleReadWithUnit<Optional<Record>> readWithKey(Name<T> keyName1, T keyValue1, Name<E> keyName2, E keyValue2, Name<F> keyName3, F keyValue3, Name<G> keyName4, G keyValue4);
    
    ListReadWithUnit<RecordList> readAll();

    ListReadWithUnit<RecordList> readWhere(Clause... clauses);

    
    
    
    
    public static interface SingleRead<T> extends Query<T> {

        SingleRead<T> withEnableTracking();

        SingleRead<T> withDisableTracking();

        SingleRead<T> withConsistency(ConsistencyLevel consistencyLevel);
    }

    public static interface SingleReadWithColumns<T> extends SingleRead<T> {

        SingleReadWithColumns<T> column(String name);

        SingleReadWithColumns<T> columnWithMetadata(String name);

        SingleReadWithColumns<T> columns(String... names);
        
        SingleReadWithColumns<T> column(Name<?> name);

        SingleReadWithColumns<T> columnWithMetadata(Name<?> name);

        SingleReadWithColumns<T> columns(Name<?>... names);

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

        ListReadWithColumns<T> column(Name<?> name);

        ListReadWithColumns<T> columnWithMetadata(Name<?> name);

        ListReadWithColumns<T> columns(Name<?>... names);
    }
    
    public static interface ListReadWithUnit<T> extends ListReadWithColumns<T> {

        ListRead<T> all();

        ListRead<Count> count();

        <E> ListRead<EntityList<E>> asEntity(Class<E> objectClass);
    }

}

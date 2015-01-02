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
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.querybuilder.Clause;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableMap;




public interface Dao {

    Dao withConsistency(ConsistencyLevel consistencyLevel);
    
    Dao withSerialConsistency(ConsistencyLevel consistencyLevel);

    
    public interface Query<T> {    

        CompletableFuture<T> executeAsync();

        
        default T execute() {
            try {
                return executeAsync().get(Long.MAX_VALUE, TimeUnit.DAYS);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                throw Exceptions.unwrapIfNecessary(e);
            } 
        }
    }


    public static interface Conditions<Q, R> extends Query<R> {
        
        Q withConsistency(ConsistencyLevel consistencyLevel);
        
        Q withEnableTracking();
        
        Q withDisableTracking();
    }

    

    public static interface Mutation<Q extends Mutation<Q>> extends Conditions<Q, Result>, Batchable {
        
        Mutation<Q> withSerialConsistency(ConsistencyLevel consistencyLevel);
        
        Mutation<Q> withTtl(Duration ttl);
    
        Mutation<Q> withWritetime(long microsSinceEpoch);
        
        BatchMutation combinedWith(Batchable other);
    }

    
   
   public static interface Update<U extends Mutation<U>> extends Mutation<U> {
       
       Update<U> onlyIf(Clause... conditions);
   }

    

   
   public static interface UpdateWithValues<U extends Mutation<U>> extends Update<U> {

       U value(String name, Object value);
       
       U values(ImmutableMap<String, Object> nameValuePairsToAdd);
       
  //     U removeValue(String name, Object value);
           
       U addSetValue(String name, Object value);
   
  //     U appendListValue(String name, Object value);
   
  //     U prependListValue(String name, Object value);
   
  //     U discardListValue(String name, Object value);
   
  //     U putMapValue(String name, Object key, Object value);     
   }

   
   
   public static interface Write extends UpdateWithValues<Write> {

       Insertion ifNotExits();
   }
   
   
 
   public static interface Insertion extends Mutation<Insertion> {
       
       Mutation<?> ifNotExits();
    }
 


   public static interface InsertionWithValues extends Mutation<Insertion> {

       InsertionWithValues value(String name, Object value);
       
       InsertionWithValues values(ImmutableMap<String, ? extends Object> nameValuePairsToAdd);
       
       Insertion ifNotExits();
   }

   

   public interface BatchMutation extends Conditions<BatchMutation, Result> {
       
       BatchMutation combinedWith(Batchable other);
       
       Query<Result> withLockedBatchType();
       
       Query<Result> withUnlockedBatchType();
   }


    
    UpdateWithValues<?> writeWhere(Clause... clauses);

    
    Insertion writeEntity(Object entity);
   
    Write writeWithKey(String keyName, Object keyValue);

    Write writeWithKey(String keyName1, Object keyValue1, String keyName2, Object keyValue2);
    
    Write writeWithKey(String keyName1, Object keyValue1, String keyName2, Object keyValue2, String keyName3, Object keyValue3);
    
    Write writeWithKey(String keyName1, Object keyValue1, String keyName2, Object keyValue2, String keyName3, Object keyValue3, String keyName4, Object keyValue4);


    
    
    Deletion deleteWithKey(String keyName, Object keyValue);
    
    Deletion deleteWithKey(String keyName1, Object keyValue1, String keyName2, Object keyValue2);
    
    Deletion deleteWithKey(String keyName1, Object keyValue1, String keyName2, Object keyValue2, String keyName3, Object keyValue3);
    
    Deletion deleteWithKey(String keyName1, Object keyValue1, String keyName2, Object keyValue2, String keyName3, Object keyValue3, String keyName4, Object keyValue4);

    Deletion deleteWhere(Clause... whereConditions);

    
    
    public static interface Deletion extends Mutation<Deletion> {
        
        Deletion onlyIf(Clause... conditions);
    }
    
    
    
    
    
    SingleReadWithUnit<Optional<Record>> readWithKey(String keyName, Object keyValue);
    
    SingleReadWithUnit<Optional<Record>> readWithKey(String keyName1, Object keyValue1, String keyName2, Object keyValue2);
    
    SingleReadWithUnit<Optional<Record>> readWithKey(String keyName1, Object keyValue1, String keyName2, Object keyValue2, String keyName3, Object keyValue3);
    
    SingleReadWithUnit<Optional<Record>> readWithKey(String keyName1, Object keyValue1, String keyName2, Object keyValue2, String keyName3, Object keyValue3, String keyName4, Object keyValue4);
   
    
    public static interface SingleRead<T> extends Query<T> {
        
        SingleRead<T> withEnableTracking();
        
        SingleRead<T> withDisableTracking();
        
        SingleRead<T> withConsistency(ConsistencyLevel consistencyLevel); 
    }
    
    
    public static interface SingleReadWithColumns<T> extends SingleRead<T> {
        
        SingleReadWithColumns<T> column(String name);
        
        SingleReadWithColumns<T> columnWithMetadata(String name);
         
        SingleReadWithColumns<T> columns(String... names);
        
        SingleReadWithColumns<T> columns(ImmutableCollection<String> nameToRead);
    }


    public static interface SingleReadWithUnit<T> extends SingleReadWithColumns<T> {
        
        SingleRead<T> all();
        
        <E> SingleRead<Optional<E>> asEntity(Class<E> objectClass);
    }


    
    
    ListReadWithUnit<RecordList> readAll();
    
    ListReadWithUnit<RecordList> readWhere(Clause... clauses);


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
        
        ListReadWithColumns<T> columns(ImmutableCollection<String> nameToRead);
    }
    
    
    public static interface ListReadWithUnit<T> extends ListReadWithColumns<T> {
        
        ListRead<T> all();
        
        ListRead<Count> count();
        
        <E> ListRead<EntityList<E>> asEntity(Class<E> objectClass);
    }
    
  
}

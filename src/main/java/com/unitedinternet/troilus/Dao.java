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

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.querybuilder.Clause;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableMap;




public interface Dao {

    Dao withConsistency(ConsistencyLevel consistencyLevel);
    
    Dao withSerialConsistency(ConsistencyLevel consistencyLevel);

    
    
    
    UpdateWithValuesAndCondition writeWhere(Clause... clauses);

    Insertion writeEntity(Object entity);
   
    WriteWithValues writeWithKey(String keyName, Object keyValue);

    WriteWithValues writeWithKey(String keyName1, Object keyValue1, String keyName2, Object keyValue2);
    
    WriteWithValues writeWithKey(String keyName1, Object keyValue1, String keyName2, Object keyValue2, String keyName3, Object keyValue3);
    
    WriteWithValues writeWithKey(String keyName1, Object keyValue1, String keyName2, Object keyValue2, String keyName3, Object keyValue3, String keyName4, Object keyValue4);

        
    
    public static interface Write extends Mutation<Write> {
    
        Write withConsistency(ConsistencyLevel consistencyLevel);
        
        Write withEnableTracking();
        
        Write withDisableTracking();
    
        Write withTtl(Duration ttl);
    
        Write withWritetime(long microsSinceEpoch);
        
        Update onlyIf(Clause... conditions);
        
        Insertion ifNotExits();
    }

    
    public static interface WriteWithValues extends Write {
        
        WriteWithValues value(String name, Object value);
        
        WriteWithValues values(ImmutableMap<String, ? extends Object> nameValuePairsToAdd);
        
        /*
        UpdateWithValues removeValue(String name, Object value);
        
        UpdateWithValues addSetValue(String name, Object value);
        
        UpdateWithValues removeSetValue(String name, Object value);
    
        UpdateWithValues appendListValue(String name, Object value);
    
        UpdateWithValues prependListValue(String name, Object value);
    
        UpdateWithValues discardListValue(String name, Object value);
    
        UpdateWithValues putMapValue(String name, Object key, Object value);*/
    }
    
    
    
    
    public static interface Update extends Mutation<Update> {
    
        Update withConsistency(ConsistencyLevel consistencyLevel);
        
        Update withEnableTracking();
        
        Update withDisableTracking();
    
        Update withTtl(Duration ttl);
    
        Update withWritetime(long microsSinceEpoch);
        
        Update onlyIf(Clause... conditions);
    }
        
        
    public static interface UpdateWithValues extends Update {
        
        UpdateWithValues value(String name, Object value);
        
        UpdateWithValues values(ImmutableMap<String, ? extends Object> nameValuePairsToAdd);
    
        /*
        UpdateWithValues removeValue(String name, Object value);
        
        UpdateWithValues addSetValue(String name, Object value);
        
        UpdateWithValues removeSetValue(String name, Object value);
    
        UpdateWithValues appendListValue(String name, Object value);
    
        UpdateWithValues prependListValue(String name, Object value);
    
        UpdateWithValues discardListValue(String name, Object value);
    
        UpdateWithValues putMapValue(String name, Object key, Object value);*/
    }
    
    
    public static interface UpdateWithValuesAndCondition extends UpdateWithValues {
        
        UpdateWithValuesAndCondition and(Clause where);
    }
    

    public static interface Insertion extends Mutation<Insertion> {

        Insertion withConsistency(ConsistencyLevel consistencyLevel);
        
        Insertion withEnableTracking();
        
        Insertion withDisableTracking();

        Insertion withTtl(Duration ttl);

        Insertion withWritetime(long microsSinceEpoch);
        
        Insertion ifNotExits();
    }
    
    
    public static interface InsertWithValues extends Insertion {
        
        InsertWithValues value(String name, Object value);
        
        InsertWithValues values(ImmutableMap<String, ? extends Object> nameValuePairsToAdd);
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


    
    
    ListReadWithUnit<ListResult<Record>> readAll();
    
    ListReadWithUnit<ListResult<Record>> readWhere(Clause... clauses);


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
        
        <E> ListRead<ListResult<E>> asEntity(Class<E> objectClass);
    }
    
    
    Deletion deleteWithKey(String keyName, Object keyValue);
    
    Deletion deleteWithKey(String keyName1, Object keyValue1, String keyName2, Object keyValue2);
    
    Deletion deleteWithKey(String keyName1, Object keyValue1, String keyName2, Object keyValue2, String keyName3, Object keyValue3);
    
    Deletion deleteWithKey(String keyName1, Object keyValue1, String keyName2, Object keyValue2, String keyName3, Object keyValue3, String keyName4, Object keyValue4);

    DeletionWithCondition deleteWhere(Clause clause);

    
    public static interface Deletion extends Mutation<Deletion> {
        
        Deletion withConsistency(ConsistencyLevel consistencyLevel);

        Deletion withEnableTracking();
        
        Deletion withDisableTracking();
        
        Deletion onlyIf(Clause... conditions);
    }


    public static interface DeletionWithCondition extends Deletion {
        
        DeletionWithCondition and(Clause where);
    }

    
    
    
}

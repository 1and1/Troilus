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



import com.datastax.driver.core.Session;

import com.datastax.driver.core.policies.RetryPolicy;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.ConsistencyLevel;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.unitedinternet.troilus.interceptor.QueryInterceptor;
import com.unitedinternet.troilus.minimal.MinimalDao;
import com.unitedinternet.troilus.minimal.Record;

 

public class MinimalDaoImpl implements MinimalDao {
    
    private final Context ctx;
    
    public MinimalDaoImpl(Session session, String tablename) {
        this(new Context(session, tablename));
    }
     
    private MinimalDaoImpl(Context ctx) {
        this.ctx = ctx;
    }
    
   
   
    
    @Override
    public MinimalDao withConsistency(ConsistencyLevel consistencyLevel) {
        return new MinimalDaoImpl(ctx.withConsistency(consistencyLevel));
    }
    
    @Override
    public MinimalDao withSerialConsistency(ConsistencyLevel consistencyLevel) {
        return new MinimalDaoImpl(ctx.withSerialConsistency(consistencyLevel));
    }
 
    @Override
    public MinimalDao withTracking() {
        return new MinimalDaoImpl(ctx.withEnableTracking());
    }
    
    @Override
    public MinimalDao withoutTracking() {
        return new MinimalDaoImpl(ctx.withDisableTracking());
    }

    @Override
    public MinimalDao withRetryPolicy(RetryPolicy policy) {
        return new MinimalDaoImpl(ctx.withRetryPolicy(policy));
    }

    
    @Override
    public MinimalDao withInterceptor(QueryInterceptor queryInterceptor) {
        return new MinimalDaoImpl(ctx.withInterceptor(queryInterceptor));
    }
    
    @Override
    public Insertion writeEntity(Object entity) {
        return new MinimalUpdateQuery(ctx, new WriteQueryDataImpl()).entity(entity);
    }
    
    @Override
    public UpdateWithValuesAndCounter writeWhere(Clause... clauses) {
        return new MinimalUpdateQuery(ctx, new WriteQueryDataImpl().whereConditions((ImmutableList.copyOf(clauses))));
    }
  
    
    @Override
    public WriteWithCounter writeWithKey(ImmutableMap<String, Object> composedKeyParts) {
        return new MinimalUpdateQuery(ctx, new WriteQueryDataImpl().keys(composedKeyParts));
    }
  
    
    @Override
    public WriteWithCounter writeWithKey(String keyName, Object keyValue) {
        return writeWithKey(ImmutableMap.of(keyName, keyValue));
    }
    
    @Override
    public WriteWithCounter writeWithKey(String keyName1, Object keyValue1, 
                                         String keyName2, Object keyValue2) {
        return writeWithKey(ImmutableMap.of(keyName1, keyValue1,
                                            keyName2, keyValue2));
        
    }
    
    @Override
    public WriteWithCounter writeWithKey(String keyName1, Object keyValue1, 
                                         String keyName2, Object keyValue2, 
                                         String keyName3, Object keyValue3) {
        return writeWithKey(ImmutableMap.of(keyName1, keyValue1, 
                                            keyName2, keyValue2, 
                                            keyName3, keyValue3));
        
    }
    
    @Override
    public <T> WriteWithCounter writeWithKey(Name<T> keyName, T keyValue) {
        return writeWithKey(keyName.getName(), (Object) keyValue); 
    }
    
    @Override
    public <T, E> WriteWithCounter writeWithKey(Name<T> keyName1, T keyValue1,
                                                Name<E> keyName2, E keyValue2) {
        return writeWithKey(keyName1.getName(), (Object) keyValue1,
                            keyName2.getName(), (Object) keyValue2); 
    }
    
    @Override
    public <T, E, F> WriteWithCounter writeWithKey(Name<T> keyName1, T keyValue1, 
                                                   Name<E> keyName2, E keyValue2, 
                                                   Name<F> keyName3, F keyValue3) {
        return writeWithKey(keyName1.getName(), (Object) keyValue1,
                            keyName2.getName(), (Object) keyValue2,
                            keyName3.getName(), (Object) keyValue3); 
    }
    
    
    @Override
    public Deletion deleteWhere(Clause... whereConditions) {
        return new MinimalDeleteQuery(ctx, new DeleteQueryDataImpl().whereConditions(ImmutableList.copyOf(whereConditions)));
    };
   
    
    @Override
    public Deletion deleteWithKey(String keyName, Object keyValue) {
        
        return deleteWithKey(ImmutableMap.of(keyName, keyValue));
    }

    @Override
    public Deletion deleteWithKey(String keyName1, Object keyValue1, 
                                  String keyName2, Object keyValue2) {
        
        return deleteWithKey(ImmutableMap.of(keyName1, keyValue1, 
                                             keyName2, keyValue2));
    }
    
    @Override
    public Deletion deleteWithKey(String keyName1, Object keyValue1, 
                                  String keyName2, Object keyValue2, 
                                  String keyName3, Object keyValue3) {
        
        return deleteWithKey(ImmutableMap.of(keyName1, keyValue1,
                                             keyName2, keyValue2, 
                                             keyName3, keyValue3));
    }
    

    @Override
    public <T> Deletion deleteWithKey(Name<T> keyName, T keyValue) {
        return deleteWithKey(keyName.getName(), (Object) keyValue);
    }
    
    @Override
    public <T, E> Deletion deleteWithKey(Name<T> keyName1, T keyValue1,
                                         Name<E> keyName2, E keyValue2) {
        return deleteWithKey(keyName1.getName(), (Object) keyValue1,
                             keyName2.getName(), (Object) keyName2);

    }
    
    @Override
    public <T, E, F> Deletion deleteWithKey(Name<T> keyName1, T keyValue1,
                                            Name<E> keyName2, E keyValue2, 
                                            Name<F> keyName3, F keyValue3) {
        return deleteWithKey(keyName1.getName(), (Object) keyValue1,
                             keyName2.getName(), (Object) keyName2,
                             keyName3.getName(), (Object) keyName3);
    }
    
    public MinimalDeleteQuery deleteWithKey(ImmutableMap<String, Object> keyNameValuePairs) {
        return new MinimalDeleteQuery(ctx, new DeleteQueryDataImpl().keys(keyNameValuePairs));
    }
    
    
    
    @Override
    public SingleReadWithUnit<Record> readWithKey(ImmutableMap<String, Object> composedkey) {
        return new MinimalSingleReadQuery(ctx, new SingleReadQueryDataImpl().keyParts(composedkey));
    }
    
    
    @Override
    public SingleReadWithUnit<Record> readWithKey(String keyName, Object keyValue) {
        return readWithKey(ImmutableMap.of(keyName, keyValue));
    }
     
    @Override
    public SingleReadWithUnit<Record> readWithKey(String keyName1, Object keyValue1, 
                                                            String keyName2, Object keyValue2) {
        return readWithKey(ImmutableMap.of(keyName1, keyValue1, 
                           keyName2, keyValue2));
    }
    
    @Override
    public SingleReadWithUnit<Record> readWithKey(String keyName1, Object keyValue1, 
                                                            String keyName2, Object keyValue2,
                                                            String keyName3, Object keyValue3) {
        return readWithKey(ImmutableMap.of(keyName1, keyValue1, 
                                           keyName2, keyValue2, 
                                           keyName3, keyValue3));
    }
    
    
    @Override
    public <T> SingleReadWithUnit<Record> readWithKey(Name<T> keyName, T keyValue) {
        return readWithKey(keyName.getName(), (Object) keyValue);
    }
    
    @Override
    public <T, E> SingleReadWithUnit<Record> readWithKey(Name<T> keyName1, T keyValue1,
                                                                   Name<E> keyName2, E keyValue2) {
        return readWithKey(keyName1.getName(), (Object) keyValue1,
                           keyName2.getName(), (Object) keyValue2);
    }
    
    @Override
    public <T, E, F> SingleReadWithUnit<Record> readWithKey(Name<T> keyName1, T keyValue1, 
                                                                      Name<E> keyName2, E keyValue2,
                                                                      Name<F> keyName3, F keyValue3) {
        return readWithKey(keyName1.getName(), (Object) keyValue1,
                           keyName2.getName(), (Object) keyValue2,                         
                           keyName3.getName(), (Object) keyValue3);
    }
    
    
    
    @Override
    public ListReadWithUnit<RecordList> readWithKeys(String name, ImmutableList<Object> values) {
        return new MinimalListReadQuery(ctx, new ListReadQueryDataImpl().keys(ImmutableMap.of(name, values)));
    }
    
    @Override
    public ListReadWithUnit<RecordList> readWithKeys(String composedKeyNamePart1, Object composedKeyValuePart1,
                                                     String composedKeyNamePart2, ImmutableList<Object> composedKeyValuesPart2) {
        return new MinimalListReadQuery(ctx, new ListReadQueryDataImpl().keys(ImmutableMap.of(composedKeyNamePart1, ImmutableList.of(composedKeyValuePart1),
                composedKeyNamePart2, composedKeyValuesPart2)));        
    }
    
    @Override
    public ListReadWithUnit<RecordList> readWithKeys(String composedKeyNamePart1, Object composedKeyValuePart1,
                                                     String composedKeyNamePart2, Object composedKeyValuePart2,
                                                     String composedKeyNamePart3, ImmutableList<Object> composedKeyValuesPart3) {
        return new MinimalListReadQuery(ctx, new ListReadQueryDataImpl().keys(ImmutableMap.of(composedKeyNamePart1, ImmutableList.of(composedKeyValuePart1),
                                                                                       composedKeyNamePart2, ImmutableList.of(composedKeyValuePart2),
                                                                                       composedKeyNamePart3, composedKeyValuesPart3)));        
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> ListReadWithUnit<RecordList> readWithKeys(Name<T> name, ImmutableList<T> values) {
        return readWithKeys(name.getName(), (ImmutableList<Object>) values);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T, E> ListReadWithUnit<RecordList> readWithKeys(Name<T> composedKeyNamePart1, T composedKeyValuePart1,
                                                            Name<E> composedKeyNamePart2, ImmutableList<E> composedKeyValuesPart2) {
        return readWithKeys(composedKeyNamePart1.getName(), (Object) composedKeyValuePart1,
                            composedKeyNamePart2.getName(), (ImmutableList<Object>) composedKeyValuesPart2);
    }
    
    
    @SuppressWarnings("unchecked")
    @Override
    public <T, E, F> ListReadWithUnit<RecordList> readWithKeys( Name<T> composedKeyNamePart1, T composedKeyValuePart1,
                                                                Name<E> composedKeyNamePart2, E composedKeyValuePart2,
                                                                Name<F> composedKeyNamePart3, ImmutableList<F> composedKeyValuesPart3) {
        return readWithKeys(composedKeyNamePart1.getName(), (Object) composedKeyValuePart1,
                            composedKeyNamePart2.getName(), (Object) composedKeyValuePart2,
                            composedKeyNamePart3.getName(), (ImmutableList<Object>) composedKeyValuesPart3);
        
    }
    
    @Override
    public MinimalListReadQuery readWhere(Clause... clauses) {
        return new MinimalListReadQuery(ctx, new ListReadQueryDataImpl().whereConditions(ImmutableSet.copyOf(clauses)));
    }
     
    
    @Override
    public MinimalListReadQuery readAll() {
        return new MinimalListReadQuery(ctx, new ListReadQueryDataImpl().columnsToFetch(ImmutableMap.<String, Boolean>of()));
    }
}
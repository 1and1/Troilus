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


import java.util.Optional;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.RetryPolicy;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.ConsistencyLevel;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.unitedinternet.troilus.interceptor.DeleteQueryData;
import com.unitedinternet.troilus.interceptor.ListReadQueryData;
import com.unitedinternet.troilus.interceptor.QueryInterceptor;
import com.unitedinternet.troilus.interceptor.SingleReadQueryData;
import com.unitedinternet.troilus.interceptor.WriteQueryData;

 

public class DaoImpl implements Dao {
    
    private final Context ctx;
    
    public DaoImpl(Session session, String tablename) {
        this(new Context(session, tablename));
    }
     
    private DaoImpl(Context ctx) {
        this.ctx = ctx;
    }
    
   
   
    
    @Override
    public Dao withConsistency(ConsistencyLevel consistencyLevel) {
        return new DaoImpl(ctx.withConsistency(consistencyLevel));
    }
    
    @Override
    public Dao withSerialConsistency(ConsistencyLevel consistencyLevel) {
        return new DaoImpl(ctx.withSerialConsistency(consistencyLevel));
    }
 
    @Override
    public Dao withEnableTracking() {
        return new DaoImpl(ctx.withEnableTracking());
    }
    
    @Override
    public Dao withDisableTracking() {
        return new DaoImpl(ctx.withDisableTracking());
    }

    @Override
    public Dao withRetryPolicy(RetryPolicy policy) {
        return new DaoImpl(ctx.withRetryPolicy(policy));
    }

    
    @Override
    public Dao withInterceptor(QueryInterceptor queryInterceptor) {
        return new DaoImpl(ctx.withInterceptor(queryInterceptor));
    }
    
    
    @Override
    public Insertion writeEntity(Object entity) {
        return new UpdateQuery(ctx, new WriteQueryData()).entity(entity);
    }
    
    @Override
    public UpdateWithValuesAndCounter writeWhere(Clause... clauses) {
        return new UpdateQuery(ctx, new WriteQueryData().whereConditions((ImmutableList.copyOf(clauses))));
    }
    
  
    
    @Override
    public WriteWithCounter writeWithKey(String keyName, Object keyValue) {
        return new UpdateQuery(ctx, new WriteQueryData().keys(ImmutableMap.of(keyName, keyValue)));
    }
    
    @Override
    public WriteWithCounter writeWithKey(String keyName1, Object keyValue1, 
                                         String keyName2, Object keyValue2) {
        return new UpdateQuery(ctx, new WriteQueryData().keys(ImmutableMap.of(keyName1, keyValue1,
                                                                                   keyName2, keyValue2)));
        
    }
    
    @Override
    public WriteWithCounter writeWithKey(String keyName1, Object keyValue1, 
                                         String keyName2, Object keyValue2, 
                                         String keyName3, Object keyValue3) {
        return new UpdateQuery(ctx, new WriteQueryData().keys(ImmutableMap.of(keyName1, keyValue1, 
                                                                                   keyName2, keyValue2, 
                                                                                   keyName3, keyValue3)));
        
    }

    @Override
    public WriteWithCounter writeWithKey(String keyName1, Object keyValue1, 
                                         String keyName2, Object keyValue2, 
                                         String keyName3, Object keyValue3, 
                                         String keyName4, Object keyValue4) {
        return new UpdateQuery(ctx, new WriteQueryData().keys(ImmutableMap.of(keyName1, keyValue1, 
                                                                                   keyName2, keyValue2, 
                                                                                   keyName3, keyValue3, 
                                                                                   keyName4, keyValue4)));
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
    public <T, E, F, G> WriteWithCounter writeWithKey(Name<T> keyName1, T keyValue1, 
                                                      Name<E> keyName2, E keyValue2,
                                                      Name<F> keyName3, F keyValue3, 
                                                      Name<G> keyName4, G keyValue4) {
        return writeWithKey(keyName1.getName(), (Object) keyValue1,
                            keyName2.getName(), (Object) keyValue2,
                            keyName3.getName(), (Object) keyValue3,
                            keyName4.getName(), (Object) keyValue4); 
    }
    
    @Override
    public Deletion deleteWhere(Clause... whereConditions) {
        return new DeleteQuery(ctx, new DeleteQueryData().whereConditions(ImmutableList.copyOf(whereConditions)));
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
    public Deletion deleteWithKey(String keyName1, Object keyValue1, 
                                  String keyName2, Object keyValue2, 
                                  String keyName3, Object keyValue3,
                                  String keyName4, Object keyValue4) {
        
        return deleteWithKey(ImmutableMap.of(keyName1, keyValue1, 
                                             keyName2, keyValue2, 
                                             keyName3, keyValue3, 
                                             keyName4, keyValue4));
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
    @Override
    public <T, E, F, G> Deletion deleteWithKey(Name<T> keyName1, T keyValue1,
                                               Name<E> keyName2, E keyValue2, 
                                               Name<F> keyName3, F keyValue3,
                                               Name<G> keyName4, G keyValue4) {
        return deleteWithKey(keyName1.getName(), (Object) keyValue1,
                             keyName2.getName(), (Object) keyName2,
                             keyName3.getName(), (Object) keyName3,
                             keyName4.getName(), (Object) keyName4);
    }
    
    private DeleteQuery deleteWithKey(ImmutableMap<String, Object> keyNameValuePairs) {
        return new DeleteQuery(ctx, new DeleteQueryData().keys(keyNameValuePairs));
    }
    
    
    
    
    @Override
    public SingleReadWithUnit<Optional<Record>> readWithKey(String keyName, Object keyValue) {
        return new SingleReadQuery(ctx, new SingleReadQueryData().keys(ImmutableMap.of(keyName, keyValue)));
    }
     
    @Override
    public SingleReadWithUnit<Optional<Record>> readWithKey(String keyName1, Object keyValue1, 
                                                            String keyName2, Object keyValue2) {
        return new SingleReadQuery(ctx, new SingleReadQueryData().keys(ImmutableMap.of(keyName1, keyValue1, 
                                                                                           keyName2, keyValue2)));
    }
    
    @Override
    public SingleReadWithUnit<Optional<Record>> readWithKey(String keyName1, Object keyValue1, 
                                                            String keyName2, Object keyValue2,
                                                            String keyName3, Object keyValue3) {
        return new SingleReadQuery(ctx, new SingleReadQueryData().keys(ImmutableMap.of(keyName1, keyValue1, 
                                                                                           keyName2, keyValue2, 
                                                                                           keyName3, keyValue3)));
    }
    
    
    @Override
    public SingleReadWithUnit<Optional<Record>> readWithKey(String keyName1, Object keyValue1, 
                                                            String keyName2, Object keyValue2, 
                                                            String keyName3, Object keyValue3, 
                                                            String keyName4, Object keyValue4) {
        return new SingleReadQuery(ctx, new SingleReadQueryData().keys(ImmutableMap.of(keyName1, keyValue1, 
                                                                                           keyName2, keyValue2, 
                                                                                           keyName3, keyValue3, 
                                                                                           keyName4, keyValue4)));
    }

    @Override
    public <T> SingleReadWithUnit<Optional<Record>> readWithKey(Name<T> keyName, T keyValue) {
        return readWithKey(keyName.getName(), (Object) keyValue);
    }
    
    @Override
    public <T, E> SingleReadWithUnit<Optional<Record>> readWithKey(Name<T> keyName1, T keyValue1,
                                                                   Name<E> keyName2, E keyValue2) {
        return readWithKey(keyName1.getName(), (Object) keyValue1,
                           keyName2.getName(), (Object) keyValue2);
    }
    
    @Override
    public <T, E, F> SingleReadWithUnit<Optional<Record>> readWithKey(Name<T> keyName1, T keyValue1, 
                                                                      Name<E> keyName2, E keyValue2,
                                                                      Name<F> keyName3, F keyValue3) {
        return readWithKey(keyName1.getName(), (Object) keyValue1,
                           keyName2.getName(), (Object) keyValue2,                         
                           keyName3.getName(), (Object) keyValue3);
    }
    
    @Override
    public <T, E, F, G> SingleReadWithUnit<Optional<Record>> readWithKey(Name<T> keyName1, T keyValue1, 
                                                                         Name<E> keyName2, E keyValue2,
                                                                         Name<F> keyName3, F keyValue3, 
                                                                         Name<G> keyName4, G keyValue4) {
        return readWithKey(keyName1.getName(), (Object) keyValue1,
                           keyName2.getName(), (Object) keyValue2,                         
                           keyName3.getName(), (Object) keyValue3,
                           keyName4.getName(), (Object) keyValue4);
    }
        
    
    @Override
    public ListReadWithUnit<RecordList> readWhere(Clause... clauses) {
        return new ListReadQuery(ctx, new ListReadQueryData().whereClauses(ImmutableSet.copyOf(clauses)));
    }
     
    
    @Override
    public ListReadWithUnit<RecordList> readAll() {
        return new ListReadQuery(ctx, new ListReadQueryData().columnsToFetch(Optional.empty()));
    }
}
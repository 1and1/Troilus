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

import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.RetryPolicy;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.ConsistencyLevel;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

 

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
    public Dao withTtl(Duration ttl) {
        return new DaoImpl(ctx.withTtl(ttl));
    }
    
    @Override
    public Dao withWritetime(long microsSinceEpoch) {
        return new DaoImpl(ctx.withWritetime(microsSinceEpoch));
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
        return new DaoImpl(ctx.interceptor(queryInterceptor));
    }
    
    
    
    <T extends QueryInterceptor> ImmutableList<T> getInterceptors(Class<T> clazz) {
        return ctx.getInterceptors(clazz);
    }
    
    
    @Override
    public Insertion writeEntity(Object entity) {
        return new UpdateQuery(ctx, new UpdateQueryData(ImmutableMap.of(), 
                                                        ImmutableList.of(),
                                                        ImmutableMap.of(),
                                                        ImmutableMap.of(),
                                                        ImmutableMap.of(),
                                                        ImmutableMap.of(),
                                                        ImmutableMap.of(),
                                                        ImmutableMap.of(),
                                                        ImmutableMap.of(),
                                                        ImmutableList.of())).entity(entity);
    }
    
    @Override
    public UpdateWithValuesAndCounter writeWhere(Clause... clauses) {
        return new UpdateQuery(ctx, new UpdateQueryData(ImmutableMap.of(), 
                                                        ImmutableList.copyOf(clauses),
                                                        ImmutableMap.of(),
                                                        ImmutableMap.of(),
                                                        ImmutableMap.of(),
                                                        ImmutableMap.of(),
                                                        ImmutableMap.of(),
                                                        ImmutableMap.of(),
                                                        ImmutableMap.of(),
                                                        ImmutableList.of()));
    }
    
  
    
    @Override
    public WriteWithCounter writeWithKey(String keyName, Object keyValue) {
        return new UpdateQuery(ctx, new UpdateQueryData(ImmutableMap.of(keyName, keyValue), 
                                                        ImmutableList.of(),
                                                        ImmutableMap.of(),
                                                        ImmutableMap.of(),
                                                        ImmutableMap.of(),
                                                        ImmutableMap.of(),
                                                        ImmutableMap.of(),
                                                        ImmutableMap.of(),
                                                        ImmutableMap.of(),
                                                        ImmutableList.of()));
    }
    
    @Override
    public WriteWithCounter writeWithKey(String keyName1, Object keyValue1, String keyName2, Object keyValue2) {
        return new UpdateQuery(ctx, new UpdateQueryData(ImmutableMap.of(keyName1, keyValue1,
                                                                        keyName2, keyValue2), 
                                                        ImmutableList.of(),
                                                        ImmutableMap.of(),
                                                        ImmutableMap.of(),
                                                        ImmutableMap.of(),
                                                        ImmutableMap.of(),
                                                        ImmutableMap.of(),
                                                        ImmutableMap.of(),
                                                        ImmutableMap.of(),
                                                        ImmutableList.of()));
        
    }
    
    @Override
    public WriteWithCounter writeWithKey(String keyName1, Object keyValue1, String keyName2, Object keyValue2, String keyName3, Object keyValue3) {
        return new UpdateQuery(ctx, new UpdateQueryData(ImmutableMap.of(keyName1, keyValue1, 
                                                                        keyName2, keyValue2, 
                                                                        keyName3, keyValue3), 
                                                        ImmutableList.of(),
                                                        ImmutableMap.of(),
                                                        ImmutableMap.of(),
                                                        ImmutableMap.of(),
                                                        ImmutableMap.of(),
                                                        ImmutableMap.of(),
                                                        ImmutableMap.of(),
                                                        ImmutableMap.of(),
                                                        ImmutableList.of()));
        
    }

    @Override
    public WriteWithCounter writeWithKey(String keyName1, Object keyValue1, String keyName2, Object keyValue2, String keyName3, Object keyValue3, String keyName4, Object keyValue4) {
        return new UpdateQuery(ctx, new UpdateQueryData(ImmutableMap.of(keyName1, keyValue1, 
                                                                        keyName2, keyValue2, 
                                                                        keyName3, keyValue3, 
                                                                        keyName4, keyValue4), 
                                                        ImmutableList.of(),
                                                        ImmutableMap.of(),
                                                        ImmutableMap.of(),
                                                        ImmutableMap.of(),
                                                        ImmutableMap.of(),
                                                        ImmutableMap.of(),
                                                        ImmutableMap.of(),
                                                        ImmutableMap.of(),
                                                        ImmutableList.of()));
    }
    
    
    
    @Override
    public Deletion deleteWhere(Clause... whereConditions) {
        return new DeleteQuery(ctx, new DeleteQueryData(ImmutableMap.of(), ImmutableList.copyOf(whereConditions), ImmutableList.of()));
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
    

    private DeleteQuery deleteWithKey(ImmutableMap<String, Object> keyNameValuePairs) {
        return new DeleteQuery(ctx, new DeleteQueryData(keyNameValuePairs, ImmutableList.of(), ImmutableList.of()));
    }
    
    
    
    
    @Override
    public SingleReadWithUnit<Optional<Record>> readWithKey(String keyName, Object keyValue) {
        return new SingleReadQuery(ctx, new SingleReadQueryData(ImmutableMap.of(keyName, keyValue),
                                                                Optional.empty()));
    }
    
    @Override
    public SingleReadWithUnit<Optional<Record>> readWithKey(String keyName1, Object keyValue1, 
                                                            String keyName2, Object keyValue2) {
        return new SingleReadQuery(ctx, new SingleReadQueryData(ImmutableMap.of(keyName1, keyValue1, 
                                                                                keyName2, keyValue2), 
                                                                Optional.of(ImmutableMap.of())));
    }
    
    @Override
    public SingleReadWithUnit<Optional<Record>> readWithKey(String keyName1, Object keyValue1, 
                                                            String keyName2, Object keyValue2,
                                                            String keyName3, Object keyValue3) {
        return new SingleReadQuery(ctx, new SingleReadQueryData(ImmutableMap.of(keyName1, keyValue1, 
                                                                                keyName2, keyValue2, 
                                                                                keyName3, keyValue3), 
                                                                Optional.of(ImmutableMap.of())));
    }
    
    @Override
    public SingleReadWithUnit<Optional<Record>> readWithKey(String keyName1, Object keyValue1, 
                                                            String keyName2, Object keyValue2, 
                                                            String keyName3, Object keyValue3, 
                                                            String keyName4, Object keyValue4) {
        return new SingleReadQuery(ctx, new SingleReadQueryData(ImmutableMap.of(keyName1, keyValue1, 
                                                                                keyName2, keyValue2, 
                                                                                keyName3, keyValue3, 
                                                                                keyName4, keyValue4), 
                                                                Optional.of(ImmutableMap.of())));
    }

    
    @Override
    public ListReadWithUnit<RecordList> readWhere(Clause... clauses) {
        return new ListReadQuery(ctx, new ListReadQueryData(ImmutableSet.copyOf(clauses), 
                                                            Optional.of(ImmutableMap.of()), 
                                                            Optional.empty(), 
                                                            Optional.empty(), 
                                                            Optional.empty(),
                                                            Optional.empty()));
    }
     
    
    @Override
    public ListReadWithUnit<RecordList> readAll() {
        return new ListReadQuery(ctx, new ListReadQueryData(ImmutableSet.of(), 
                                                            Optional.of(ImmutableMap.of()), 
                                                            Optional.empty(), 
                                                            Optional.empty(), 
                                                            Optional.empty(),
                                                            Optional.empty()));
    }
}
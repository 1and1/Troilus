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

import com.datastax.driver.core.BatchStatement.Type;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.RetryPolicy;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.ConsistencyLevel;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;


 

public class DaoImpl implements Dao, QueryFactory {
    
    private final Context ctx;
    
    
    public DaoImpl(Session session, String tablename) {
        this(new Context(session, tablename));
    }
     
    private DaoImpl(Context ctx) {
        this.ctx = ctx;
    }

    
    @Override
    public UpdateQuery newUpdateQuery(Context ctx,
                                      QueryFactory queryFactory,
                                      ImmutableMap<String, Object> keys,
                                      ImmutableList<Clause> whereConditions,
                                      ImmutableMap<String, Optional<Object>> valuesToMutate,
                                      ImmutableMap<String, ImmutableSet<Object>> setValuesToAdd,
                                      ImmutableMap<String, ImmutableSet<Object>> setValuesToRemove,
                                      ImmutableMap<String, ImmutableList<Object>> listValuesToAppend,
                                      ImmutableMap<String, ImmutableList<Object>> listValuesToPrepend,
                                      ImmutableMap<String, ImmutableList<Object>> listValuesToRemove,
                                      ImmutableMap<String, ImmutableMap<Object, Optional<Object>>> mapValuesToMutate,
                                      ImmutableMap<String, Long> counterValuesToIncr,
                                      ImmutableMap<String, Long> counterValuesToDecr,
                                      ImmutableList<Clause> ifConditions) {
        return new UpdateQuery(ctx, 
                               queryFactory, 
                               keys, 
                               whereConditions, 
                               valuesToMutate, 
                               setValuesToAdd, 
                               setValuesToRemove, 
                               listValuesToAppend, 
                               listValuesToPrepend, 
                               listValuesToRemove,
                               mapValuesToMutate,
                               counterValuesToIncr,
                               counterValuesToDecr,
                               ifConditions);
    }
        
    @Override
    public InsertionQuery newInsertionQuery(Context ctx, 
                                            QueryFactory queryFactory, 
                                            ImmutableMap<String, Optional<Object>> valuesToMutate, 
                                            boolean ifNotExists) {
        return new InsertionQuery(ctx, 
                                  queryFactory, 
                                  valuesToMutate, 
                                  ifNotExists);
    }
    
   
    
    @Override
    public DeleteQuery newDeleteQuery(Context ctx, 
                                      QueryFactory queryFactory,
                                      ImmutableMap<String, Object> keyNameValuePairs,
                                      ImmutableList<Clause> whereConditions,
                                      ImmutableList<Clause> ifConditions) {
        return new DeleteQuery(ctx, 
                               queryFactory, 
                               keyNameValuePairs, 
                               whereConditions, 
                               ifConditions);
    }


    public SingleReadQuery newSingleReadQuery(Context ctx, 
                                              QueryFactory queryFactory,
                                              ImmutableMap<String, Object> keyNameValuePairs, 
                                              Optional<ImmutableMap<String, Boolean>> optionalColumnsToFetch) {
        return new SingleReadQuery(ctx, 
                                   queryFactory, 
                                   keyNameValuePairs, 
                                   optionalColumnsToFetch);
    }
    
    
    public ListReadQuery newListReadQuery(Context ctx,
                                          QueryFactory queryFactory, 
                                          ImmutableSet<Clause> clauses, 
                                          Optional<ImmutableMap<String, Boolean>> columnsToFetch, 
                                          Optional<Integer> optionalLimit, 
                                          Optional<Boolean> optionalAllowFiltering,
                                          Optional<Integer> optionalFetchSize,
                                          Optional<Boolean> optionalDistinct) {
        return new ListReadQuery(ctx, 
                                 queryFactory, 
                                 clauses, 
                                 columnsToFetch, 
                                 optionalLimit, 
                                 optionalAllowFiltering, 
                                 optionalFetchSize, 
                                 optionalDistinct);
    }
    
    

    
    public CountReadQuery newCountReadQuery(Context ctx, 
                                            QueryFactory queryFactory,
                                            ImmutableSet<Clause> clauses, 
                                            Optional<Integer> optionalLimit, 
                                            Optional<Boolean> optionalAllowFiltering,
                                            Optional<Integer> optionalFetchSize,
                                            Optional<Boolean> optionalDistinct) {
        return new CountReadQuery(ctx, 
                                  queryFactory, 
                                  clauses, 
                                  optionalLimit, 
                                  optionalAllowFiltering, 
                                  optionalFetchSize, 
                                  optionalDistinct);
    }
    
  
    
    
    @Override
    public BatchMutationQuery newBatchMutationQuery(Context ctx,
                                                    QueryFactory queryFactory,
                                                    Type type,
                                                    ImmutableList<Batchable> batchables) {
        return new BatchMutationQuery(ctx, 
                                      queryFactory, 
                                      type, 
                                      batchables);
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
    public Insertion writeEntity(Object entity) {
        return newUpdateQuery(ctx,
                              this,
                              ImmutableMap.of(), 
                              ImmutableList.of(),
                              ImmutableMap.of(),
                              ImmutableMap.of(),
                              ImmutableMap.of(),
                              ImmutableMap.of(),
                              ImmutableMap.of(),
                              ImmutableMap.of(),
                              ImmutableMap.of(),
                              ImmutableMap.of(),
                              ImmutableMap.of(),
                              ImmutableList.of()).entity(entity);
    }
    
    @Override
    public UpdateWithValues<?> writeWhere(Clause... whereConditions) {
        return newUpdateQuery(ctx, 
                              this, 
                              ImmutableMap.of(), 
                              ImmutableList.copyOf(whereConditions),
                              ImmutableMap.of(),
                              ImmutableMap.of(),
                              ImmutableMap.of(),
                              ImmutableMap.of(),
                              ImmutableMap.of(),
                              ImmutableMap.of(),
                              ImmutableMap.of(),
                              ImmutableMap.of(),
                              ImmutableMap.of(),
                              ImmutableList.of());
    }
    
    @Override
    public Write writeWithKey(String keyName, Object keyValue) {
        return newUpdateQuery(ctx, 
                              this, 
                              ImmutableMap.of(keyName, keyValue), 
                              ImmutableList.of(),
                              ImmutableMap.of(),
                              ImmutableMap.of(),
                              ImmutableMap.of(),
                              ImmutableMap.of(),
                              ImmutableMap.of(),
                              ImmutableMap.of(),
                              ImmutableMap.of(),
                              ImmutableMap.of(),
                              ImmutableMap.of(),
                              ImmutableList.of());
    }
    
    @Override
    public Write writeWithKey(String keyName1, Object keyValue1, String keyName2, Object keyValue2) {
        return newUpdateQuery(ctx, 
                              this, 
                              ImmutableMap.of(keyName1, keyValue1,
                                              keyName2, keyValue2), 
                              ImmutableList.of(),
                              ImmutableMap.of(),
                              ImmutableMap.of(),
                              ImmutableMap.of(),
                              ImmutableMap.of(),
                              ImmutableMap.of(),
                              ImmutableMap.of(),
                              ImmutableMap.of(),
                              ImmutableMap.of(),
                              ImmutableMap.of(),
                              ImmutableList.of());
        
    }
    
    @Override
    public Write writeWithKey(String keyName1, Object keyValue1, String keyName2, Object keyValue2, String keyName3, Object keyValue3) {
        return newUpdateQuery(ctx, 
                              this, 
                              ImmutableMap.of(keyName1, keyValue1, 
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
                              ImmutableMap.of(),
                              ImmutableMap.of(),
                              ImmutableList.of());
        
    }

    @Override
    public Write writeWithKey(String keyName1, Object keyValue1, String keyName2, Object keyValue2, String keyName3, Object keyValue3, String keyName4, Object keyValue4) {
        return newUpdateQuery(ctx, 
                              this, 
                              ImmutableMap.of(keyName1, keyValue1, 
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
                              ImmutableMap.of(),
                              ImmutableMap.of(),
                              ImmutableList.of());
        
    }
    
    
    
    @Override
    public Deletion deleteWhere(Clause... whereConditions) {
        
        return newDeleteQuery(ctx, 
                              this,
                              ImmutableMap.of(), 
                              ImmutableList.copyOf(whereConditions), 
                              ImmutableList.of());
    };
    
    @Override
    public Deletion deleteWithKey(String keyName, Object keyValue) {
        
        return newDeleteQuery(ctx, 
                              this,
                              ImmutableMap.of(keyName, keyValue), 
                              ImmutableList.of(), 
                              ImmutableList.of());
    }

    @Override
    public Deletion deleteWithKey(String keyName1, Object keyValue1, 
                                  String keyName2, Object keyValue2) {
        
        return newDeleteQuery(ctx, 
                              this, 
                              ImmutableMap.of(keyName1, keyValue1, 
                                              keyName2, keyValue2),
                              ImmutableList.of(), 
                              ImmutableList.of());
    }
    
    @Override
    public Deletion deleteWithKey(String keyName1, Object keyValue1, 
                                  String keyName2, Object keyValue2, 
                                  String keyName3, Object keyValue3) {
        
        return newDeleteQuery(ctx, 
                              this, 
                              ImmutableMap.of(keyName1, keyValue1,
                                              keyName2, keyValue2, 
                                              keyName3, keyValue3),
                              ImmutableList.of(), 
                              ImmutableList.of());
    }
    
    @Override
    public Deletion deleteWithKey(String keyName1, Object keyValue1, 
                                  String keyName2, Object keyValue2, 
                                  String keyName3, Object keyValue3,
                                  String keyName4, Object keyValue4) {
        
        return newDeleteQuery(ctx, 
                              this,
                              ImmutableMap.of(keyName1, keyValue1, 
                                              keyName2, keyValue2, 
                                              keyName3, keyValue3, 
                                              keyName4, keyValue4), 
                              ImmutableList.of(), 
                              ImmutableList.of());
    }
    
    
    
    
    @Override
    public SingleReadWithUnit<Optional<Record>> readWithKey(String keyName, Object keyValue) {
        return newSingleReadQuery(ctx, 
                                  this,
                                  ImmutableMap.of(keyName, keyValue),
                                  Optional.empty());
    }
    
    @Override
    public SingleReadWithUnit<Optional<Record>> readWithKey(String keyName1, Object keyValue1, 
                                                            String keyName2, Object keyValue2) {
        return newSingleReadQuery(ctx, 
                                  this,
                                  ImmutableMap.of(keyName1, keyValue1, 
                                                  keyName2, keyValue2), 
                                  Optional.of(ImmutableMap.of()));
    }
    
    @Override
    public SingleReadWithUnit<Optional<Record>> readWithKey(String keyName1, Object keyValue1, 
                                                            String keyName2, Object keyValue2,
                                                            String keyName3, Object keyValue3) {
        return newSingleReadQuery(ctx, 
                                  this,
                                  ImmutableMap.of(keyName1, keyValue1, 
                                                  keyName2, keyValue2, 
                                                  keyName3, keyValue3), 
                                  Optional.of(ImmutableMap.of()));
    }
    
    @Override
    public SingleReadWithUnit<Optional<Record>> readWithKey(String keyName1, Object keyValue1, 
                                                            String keyName2, Object keyValue2, 
                                                            String keyName3, Object keyValue3, 
                                                            String keyName4, Object keyValue4) {
        return newSingleReadQuery(ctx, 
                                  this,
                                  ImmutableMap.of(keyName1, keyValue1, 
                                                  keyName2, keyValue2, 
                                                  keyName3, keyValue3, 
                                                  keyName4, keyValue4), 
                                  Optional.of(ImmutableMap.of()));
    }

    
    @Override
    public ListReadWithUnit<RecordList> readWhere(Clause... clauses) {
        return newListReadQuery(ctx,
                                this,
                                ImmutableSet.copyOf(clauses), 
                                Optional.of(ImmutableMap.of()), 
                                Optional.empty(), 
                                Optional.empty(), 
                                Optional.empty(),
                                Optional.empty());
    }
     
    
    @Override
    public ListReadWithUnit<RecordList> readAll() {
        return newListReadQuery(ctx, 
                                this,
                                ImmutableSet.of(), 
                                Optional.of(ImmutableMap.of()), 
                                Optional.empty(), 
                                Optional.empty(), 
                                Optional.empty(),
                                Optional.empty());
    }
}
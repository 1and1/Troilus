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

import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.UserType;
import com.datastax.driver.core.BatchStatement.Type;
import com.datastax.driver.core.policies.RetryPolicy;
import com.datastax.driver.core.querybuilder.BuiltStatement;
import com.datastax.driver.core.querybuilder.Clause;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.unitedinternet.troilus.Dao.Deletion;

 
abstract class AbstractQuery<Q> {
    
    private final Context ctx;
    private final QueryFactory queryFactory;
    
    public AbstractQuery(Context ctx, QueryFactory queryFactory) {
        this.ctx = ctx;
        this.queryFactory = queryFactory;
    }

    
    
    abstract protected Q newQuery(Context newContext);
    

    
    ////////////////////////
    // default implementations
  
    public Q withConsistency(ConsistencyLevel consistencyLevel) {
        return newQuery(ctx.withConsistency(consistencyLevel));
    }
  
    public Q withEnableTracking() {
        return newQuery(ctx.withEnableTracking());
    }
    
    public Q withDisableTracking() {
        return newQuery(ctx.withDisableTracking());
    }
    
    public Q withRetryPolicy(RetryPolicy policy) {
        return newQuery(ctx.withRetryPolicy(policy));
    }
    

    protected Optional<ConsistencyLevel> getConsistencyLevel() {
        return ctx.getConsistencyLevel();
    }

    protected Optional<ConsistencyLevel> getSerialConsistencyLevel() {
        return ctx.getSerialConsistencyLevel();
    }

    protected Optional<Duration> getTtl() {
        return ctx.getTtl();
    }
    

    
    
    
    
    
    //////////////////////////////
    // utilities methods
    
    
    @Deprecated
    protected Context getContext() {
        return ctx; 
    }
    
    @Deprecated
    protected QueryFactory getQueryFactory() {
        return queryFactory; 
    }
      
   
    protected Object toUdtValue(DataType datatype, Object value) {
        return UDTValueMapper.toUdtValue(ctx, datatype, value);
    }
        
      
    protected String getTable() {
        return ctx.getTable();
    }
  
    protected ProtocolVersion getProtocolVersion() {
        return ctx.getProtocolVersion();
    }
    
    protected Record newRecord(Result result, Row row) {
        return new Record(ctx, result, row);
    }
    
    protected RecordList newRecordList(ResultSet resultSet) {
        return RecordList.newRecordList(ctx, resultSet);
    }
    
    protected <E> EntityList<E> newEntityList(RecordList recordList, Class<E> clazz) {
        return EntityList.newEntityList(ctx, recordList, clazz); 
    }
    
    protected ColumnMetadata getColumnMetadata(String columnName) {
        return ctx.getColumnMetadata(columnName);
    }

    
    protected UserType getUserType(String usertypeName) {
        return ctx.getUserType(usertypeName);
    }

    
    protected <T> Optional<T> toOptional(T obj) {
        return ctx.toOptional(obj);
    }


    protected boolean isBuildInType(DataType dataType) {        
        if (dataType.isCollection()) {
            for (DataType type : dataType.getTypeArguments()) {
                if (!isBuildinType(type)) {
                    return false;
                }
            }
            return true;

        } else {
            return isBuildinType(dataType);
        }
    }
  
    private boolean isBuildinType(DataType type) {
        return ctx.isBuildInType(type);
    }   
    
    
    protected ImmutableMap<String, Optional<Object>> toValues(Object entity) {
        return ctx.toValues(entity);
    }

    protected PreparedStatement prepare(BuiltStatement statement) {
        return ctx.prepare(statement);
    }
    
    protected CompletableFuture<ResultSet> performAsync(Statement statement) {
        return ctx.performAsync(statement);
    }

    protected <T> T fromValues(Class<?> clazz, TriFunction<String, Class<?>, Class<?>, Optional<?>> datasource) {
        return ctx.fromValues(clazz, datasource);
    }

    
    
    
    /////////////////
    // factory methods
    
    protected InsertionQuery newInsertionQuery(ImmutableMap<String, Optional<Object>> valuesToMutate,
                                               boolean ifNotExists) {
        return queryFactory.newInsertionQuery(ctx, 
                                              queryFactory, 
                                              valuesToMutate,
                                              ifNotExists);
    }
    
    
    protected InsertionQuery newInsertionQuery(Context ctx,
                                               ImmutableMap<String, Optional<Object>> valuesToMutate,
                                               boolean ifNotExists) {
        return queryFactory.newInsertionQuery(ctx, 
                                              queryFactory, 
                                              valuesToMutate,
                                              ifNotExists);
    }
    


    protected UpdateQuery newUpdateQuery(Context ctx,
                                         ImmutableMap<String, Object> keys,
                                         ImmutableList<Clause> whereConditions,
                                         ImmutableMap<String, Optional<Object>> valuesToMutate,
                                         ImmutableMap<String, ImmutableSet<Object>> setValuesToAdd,
                                         ImmutableMap<String, ImmutableSet<Object>> setValuesToRemove,
                                         ImmutableMap<String, ImmutableList<Object>> listValuesToAppend,
                                         ImmutableMap<String, ImmutableList<Object>> listValuesToPrepend,
                                         ImmutableMap<String, ImmutableList<Object>> listValuesToRemove,
                                         ImmutableMap<String, ImmutableMap<Object, Optional<Object>>> mapValuesToMutate,
                                         ImmutableList<Clause> ifConditions) {
        return queryFactory.newUpdateQuery(ctx,
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
                                           ifConditions);
    }
  

    protected UpdateQuery newUpdateQuery(ImmutableMap<String, Object> keys,
                                         ImmutableList<Clause> whereConditions,
                                         ImmutableMap<String, Optional<Object>> valuesToMutate,
                                         ImmutableMap<String, ImmutableSet<Object>> setValuesToAdd,
                                         ImmutableMap<String, ImmutableSet<Object>> setValuesToRemove,
                                         ImmutableMap<String, ImmutableList<Object>> listValuesToAppend,
                                         ImmutableMap<String, ImmutableList<Object>> listValuesToPrepend,
                                         ImmutableMap<String, ImmutableList<Object>> listValuesToRemove,
                                         ImmutableMap<String, ImmutableMap<Object, Optional<Object>>> mapValuesToMutate,
                                         ImmutableList<Clause> ifConditions) {
        return queryFactory.newUpdateQuery(ctx,
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
                                           ifConditions);
    }
 
    
    
    protected Deletion newDeleteQuery(Context ctx, 
                                      ImmutableMap<String, Object> keyNameValuePairs, 
                                      ImmutableList<Clause> whereConditions, 
                                      ImmutableList<Clause> ifConditions) {
        return queryFactory.newDeleteQuery(ctx, queryFactory, keyNameValuePairs, whereConditions, ifConditions);
    }

    
    protected Deletion newDeleteQuery(ImmutableMap<String, Object> keyNameValuePairs, 
                                      ImmutableList<Clause> whereConditions, 
                                      ImmutableList<Clause> ifConditions) {
        return queryFactory.newDeleteQuery(ctx, queryFactory, keyNameValuePairs, whereConditions, ifConditions);
    }


    protected  BatchMutationQuery newBatchMutationQuery(Context ctx,
                                                        Type type,
                                                        ImmutableList<Batchable> batchables) {
        return queryFactory.newBatchMutationQuery(ctx, queryFactory, type, batchables);
    } 
    
    protected  BatchMutationQuery newBatchMutationQuery(Type type,
                                                        ImmutableList<Batchable> batchables) {
        return queryFactory.newBatchMutationQuery(ctx, queryFactory, type, batchables);
    }

    
    protected SingleReadQuery newSingleReadQuery(Context ctx, 
                                                 ImmutableMap<String, Object> keyNameValuePairs, 
                                                 Optional<ImmutableSet<ColumnToFetch>> optionalColumnsToFetch) {
        return queryFactory.newSingleReadQuery(ctx, queryFactory, keyNameValuePairs, optionalColumnsToFetch);
    }
    

    protected SingleReadQuery newSingleReadQuery(ImmutableMap<String, Object> keyNameValuePairs, 
                                                 Optional<ImmutableSet<ColumnToFetch>> optionalColumnsToFetch) {
        return queryFactory.newSingleReadQuery(ctx, queryFactory, keyNameValuePairs, optionalColumnsToFetch);
    }
    
    
    protected ListReadQuery newListReadQuery(Context ctx,
                                             ImmutableSet<Clause> clauses, 
                                             Optional<ImmutableSet<ColumnToFetch>> columnsToFetch, 
                                             Optional<Integer> optionalLimit, 
                                             Optional<Boolean> optionalAllowFiltering,
                                             Optional<Integer> optionalFetchSize,
                                             Optional<Boolean> optionalDistinct) {
        return queryFactory.newListReadQuery(ctx, queryFactory, clauses, columnsToFetch, optionalLimit, optionalAllowFiltering, optionalFetchSize, optionalDistinct);
    }
    
    

    protected ListReadQuery newListReadQuery(ImmutableSet<Clause> clauses, 
                                             Optional<ImmutableSet<ColumnToFetch>> columnsToFetch, 
                                             Optional<Integer> optionalLimit, 
                                             Optional<Boolean> optionalAllowFiltering,
                                             Optional<Integer> optionalFetchSize,
                                             Optional<Boolean> optionalDistinct) {
        return queryFactory.newListReadQuery(ctx, queryFactory, clauses, columnsToFetch, optionalLimit, optionalAllowFiltering, optionalFetchSize, optionalDistinct);
    }
    
    protected CountReadQuery newCountReadQuery(Context ctx, 
                                               ImmutableSet<Clause> clauses, 
                                               Optional<Integer> optionalLimit, 
                                               Optional<Boolean> optionalAllowFiltering,
                                               Optional<Integer> optionalFetchSize,
                                               Optional<Boolean> optionalDistinct) {
        return queryFactory.newCountReadQuery(ctx, queryFactory, clauses, optionalLimit, optionalAllowFiltering, optionalFetchSize, optionalDistinct);  
    }


    protected CountReadQuery newCountReadQuery(ImmutableSet<Clause> clauses, 
                                               Optional<Integer> optionalLimit, 
                                               Optional<Boolean> optionalAllowFiltering,
                                               Optional<Integer> optionalFetchSize,
                                               Optional<Boolean> optionalDistinct) {
        return queryFactory.newCountReadQuery(ctx, queryFactory, clauses, optionalLimit, optionalAllowFiltering, optionalFetchSize, optionalDistinct);
    }
}


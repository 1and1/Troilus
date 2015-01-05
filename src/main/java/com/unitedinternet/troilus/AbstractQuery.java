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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;
import com.datastax.driver.core.BatchStatement.Type;
import com.datastax.driver.core.policies.RetryPolicy;
import com.datastax.driver.core.querybuilder.BuiltStatement;
import com.datastax.driver.core.querybuilder.Clause;
import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import com.unitedinternet.troilus.Dao.Deletion;

 
abstract class AbstractQuery<Q> implements QueryFactory {
    
    private final UDTValueMapper valueMapper = new UDTValueMapper();
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
      
    protected ProtocolVersion getProtocolVersion() {
        return ctx.getProtocolVersion();
    }
   
    protected Object toUdtValue(DataType datatype, Object value) {
        return valueMapper.toUdtValue(datatype, value);
    }
        
      
    protected String getTable() {
        return ctx.getTable();
    }
  
    protected Record newRecord(Result result, Row row) {
        return new RecordImpl(result, row);
    }
    
    protected RecordList newRecordList(ResultSet resultSet) {
        return new RecordListImpl(resultSet);
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

    
    public boolean isOptional(Object obj) {
        if (obj == null) {
            return false;
        } else {
            return (Optional.class.isAssignableFrom(obj.getClass()));
        }
    }
 

    @SuppressWarnings("unchecked")
    public <T> Optional<T> toOptional(T obj) {
        if (obj == null) {
            return Optional.empty();
        } else {
            if (isOptional(obj)) {
                return (Optional<T>) obj;
            } else {
                return Optional.of(obj);
            }
        }
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
        return DataType.allPrimitiveTypes().contains(type);
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
    
    @Override
    public InsertionQuery newInsertionQuery(Context ctx, 
                                            QueryFactory queryFactory, 
                                            ImmutableMap<String, Optional<Object>> valuesToMutate, 
                                            boolean ifNotExists) {
        return this.queryFactory.newInsertionQuery(ctx,
                                                   queryFactory, 
                                                   valuesToMutate, 
                                                   ifNotExists);
    }
    
    protected InsertionQuery newInsertionQuery(Context ctx,
                                               ImmutableMap<String, Optional<Object>> valuesToMutate,
                                               boolean ifNotExists) {
        return newInsertionQuery(ctx, 
                                 queryFactory, 
                                 valuesToMutate,
                                 ifNotExists);
    }
    
    protected InsertionQuery newInsertionQuery(ImmutableMap<String, Optional<Object>> valuesToMutate,
                                               boolean ifNotExists) {
        return newInsertionQuery(ctx, 
                                 valuesToMutate,
                                 ifNotExists);
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
                                      ImmutableList<Clause> ifConditions) {
        return this.queryFactory.newUpdateQuery(ctx, 
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
        return newUpdateQuery(ctx,
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
        return newUpdateQuery(ctx,
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
 
    
    @Override
    public DeleteQuery newDeleteQuery(Context ctx,
                                      QueryFactory queryFactory,
                                      ImmutableMap<String, Object> keyNameValuePairs,
                                      ImmutableList<Clause> whereConditions,
                                      ImmutableList<Clause> ifConditions) {
        return this.queryFactory.newDeleteQuery(ctx, 
                                                queryFactory, 
                                                keyNameValuePairs,
                                                whereConditions, 
                                                ifConditions);
    }
    
    protected Deletion newDeleteQuery(Context ctx, 
                                      ImmutableMap<String, Object> keyNameValuePairs, 
                                      ImmutableList<Clause> whereConditions, 
                                      ImmutableList<Clause> ifConditions) {
        return newDeleteQuery(ctx, 
                              queryFactory, 
                              keyNameValuePairs, 
                              whereConditions, 
                              ifConditions);
    }

    protected Deletion newDeleteQuery(ImmutableMap<String, Object> keyNameValuePairs, 
                                      ImmutableList<Clause> whereConditions, 
                                      ImmutableList<Clause> ifConditions) {
        return newDeleteQuery(ctx,
                              keyNameValuePairs, 
                              whereConditions, 
                              ifConditions);
    }


    @Override
    public BatchMutationQuery newBatchMutationQuery(Context ctx,
                                                    QueryFactory queryFactory,
                                                    Type type,
                                                    ImmutableList<Batchable> batchables) {
        return this.queryFactory.newBatchMutationQuery(ctx,
                                                       queryFactory, 
                                                       type, 
                                                       batchables);
    }
    
    protected  BatchMutationQuery newBatchMutationQuery(Context ctx,
                                                        Type type,
                                                        ImmutableList<Batchable> batchables) {
        return newBatchMutationQuery(ctx, 
                                     queryFactory, 
                                     type, 
                                     batchables);
    } 
    
    protected  BatchMutationQuery newBatchMutationQuery(Type type,
                                                        ImmutableList<Batchable> batchables) {
        return newBatchMutationQuery(ctx, 
                                     type, 
                                     batchables);
    }
    
   
    @Override
    public CounterBatchMutationQuery newCounterBatchMutationQuery(Context ctx,
                                                                  QueryFactory queryFactory,
                                                                  ImmutableList<CounterBatchable> batchables) {
        return this.queryFactory.newCounterBatchMutationQuery(ctx, 
                                                              queryFactory,
                                                              batchables);
    }
 
    

    protected CounterBatchMutationQuery newCounterBatchMutationQuery(Context ctx,
                                                                     ImmutableList<CounterBatchable> batchables) {    
        return queryFactory.newCounterBatchMutationQuery(ctx, 
                                                         queryFactory,
                                                         batchables);
    }

    
    protected CounterBatchMutationQuery newCounterBatchMutationQuery(ImmutableList<CounterBatchable> batchables) {    
        return queryFactory.newCounterBatchMutationQuery(ctx, 
                                                         queryFactory,
                                                         batchables);
    }

    
    @Override
    public SingleReadQuery newSingleReadQuery(Context ctx,
                                              QueryFactory queryFactory,
                                              ImmutableMap<String, Object> keyNameValuePairs,
                                              Optional<ImmutableMap<String, Boolean>> optionalColumnsToFetch) {
        return this.queryFactory.newSingleReadQuery(ctx, 
                                                    queryFactory, 
                                                    keyNameValuePairs, 
                                                    optionalColumnsToFetch); 
    }
    
    protected SingleReadQuery newSingleReadQuery(Context ctx, 
                                                 ImmutableMap<String, Object> keyNameValuePairs, 
                                                 Optional<ImmutableMap<String, Boolean>> optionalColumnsToFetch) {
        return newSingleReadQuery(ctx, 
                                  queryFactory,
                                  keyNameValuePairs,
                                  optionalColumnsToFetch);
    }
    
    protected SingleReadQuery newSingleReadQuery(ImmutableMap<String, Object> keyNameValuePairs, 
                                                 Optional<ImmutableMap<String, Boolean>> optionalColumnsToFetch) {
        return newSingleReadQuery(ctx, 
                                  keyNameValuePairs,
                                  optionalColumnsToFetch);
    }
    
    
    @Override
    public ListReadQuery newListReadQuery(Context ctx,
                                          QueryFactory queryFactory, ImmutableSet<Clause> clauses,
                                          Optional<ImmutableMap<String, Boolean>> columnsToFetch,
                                          Optional<Integer> optionalLimit,
                                          Optional<Boolean> optionalAllowFiltering,
                                          Optional<Integer> optionalFetchSize,
                                          Optional<Boolean> optionalDistinct) {
        return this.queryFactory.newListReadQuery(ctx, 
                                                  queryFactory, 
                                                  clauses, 
                                                  columnsToFetch,
                                                  optionalLimit,
                                                  optionalAllowFiltering,
                                                  optionalFetchSize,
                                                  optionalDistinct);
    }
    
    protected ListReadQuery newListReadQuery(Context ctx,
                                             ImmutableSet<Clause> clauses, 
                                             Optional<ImmutableMap<String, Boolean>> columnsToFetch, 
                                             Optional<Integer> optionalLimit, 
                                             Optional<Boolean> optionalAllowFiltering,
                                             Optional<Integer> optionalFetchSize,
                                             Optional<Boolean> optionalDistinct) {
        return newListReadQuery(ctx, 
                                queryFactory, 
                                clauses, 
                                columnsToFetch, 
                                optionalLimit,
                                optionalAllowFiltering, 
                                optionalFetchSize, 
                                optionalDistinct);
    }
    
    protected ListReadQuery newListReadQuery(ImmutableSet<Clause> clauses, 
                                             Optional<ImmutableMap<String, Boolean>> columnsToFetch, 
                                             Optional<Integer> optionalLimit, 
                                             Optional<Boolean> optionalAllowFiltering,
                                             Optional<Integer> optionalFetchSize,
                                             Optional<Boolean> optionalDistinct) {
        return newListReadQuery(ctx,
                                clauses, 
                                columnsToFetch,
                                optionalLimit, 
                                optionalAllowFiltering, 
                                optionalFetchSize, 
                                optionalDistinct);
    }
    
    
    @Override
    public CountReadQuery newCountReadQuery(Context ctx,
                                            QueryFactory queryFactory,
                                            ImmutableSet<Clause> clauses,
                                            Optional<Integer> optionalLimit,
                                            Optional<Boolean> optionalAllowFiltering,
                                            Optional<Integer> optionalFetchSize,
                                            Optional<Boolean> optionalDistinct) {
        return this.queryFactory.newCountReadQuery(ctx, 
                                                   queryFactory, 
                                                   clauses, 
                                                   optionalLimit, 
                                                   optionalAllowFiltering, 
                                                   optionalFetchSize, 
                                                   optionalDistinct);
    }
    
    protected CountReadQuery newCountReadQuery(Context ctx, 
                                               ImmutableSet<Clause> clauses, 
                                               Optional<Integer> optionalLimit, 
                                               Optional<Boolean> optionalAllowFiltering,
                                               Optional<Integer> optionalFetchSize,
                                               Optional<Boolean> optionalDistinct) {
        return newCountReadQuery(ctx,
                                 queryFactory, 
                                 clauses,
                                 optionalLimit, 
                                 optionalAllowFiltering,
                                 optionalFetchSize, 
                                 optionalDistinct);  
    }

    protected CountReadQuery newCountReadQuery(ImmutableSet<Clause> clauses, 
                                               Optional<Integer> optionalLimit, 
                                               Optional<Boolean> optionalAllowFiltering,
                                               Optional<Integer> optionalFetchSize,
                                               Optional<Boolean> optionalDistinct) {
        return newCountReadQuery(ctx, 
                                 clauses, 
                                 optionalLimit, 
                                 optionalAllowFiltering,
                                 optionalFetchSize,
                                 optionalDistinct);
    }
    
    
    
    

    /**
     * UDTValueMapper
     */
    private class UDTValueMapper {
        
        private UDTValueMapper() {  }
        
        
        public Optional<?> fromUdtValue(DataType datatype, 
                                        UDTValue udtValue, Class<?> fieldtype1, 
                                        Class<?> fieldtype2,
                                        String fieldname) {
            
            // build-in type 
            if (isBuildInType(datatype)) {
                return Optional.ofNullable(datatype.deserialize(udtValue.getBytesUnsafe(fieldname), ctx.getProtocolVersion()));
            
                
            // udt collection    
            } else if (datatype.isCollection()) {
                Class<?> type = datatype.getName().asJavaClass();
               
                // set
                if (Set.class.isAssignableFrom(type)) {
                    return Optional.ofNullable(fromUdtValues(datatype.getTypeArguments().get(0), 
                                                             ImmutableSet.copyOf(udtValue.getSet(fieldname, UDTValue.class)), 
                                                             fieldtype1)); 
                    
                // list
                } else if (List.class.isAssignableFrom(type)) {
                    return Optional.ofNullable(fromUdtValues(datatype.getTypeArguments().get(0), 
                                                             ImmutableList.copyOf(udtValue.getList(fieldname, UDTValue.class)),
                                                             fieldtype1)); 

                // map
                } else {
                    if (isBuildInType(datatype.getTypeArguments().get(0))) {
                        return Optional.ofNullable(fromUdtValues(datatype.getTypeArguments().get(0), 
                                                                 datatype.getTypeArguments().get(1), 
                                                                 ImmutableMap.<Object, Object>copyOf(udtValue.getMap(fieldname, fieldtype1, UDTValue.class)), 
                                                                 fieldtype1, 
                                                                 fieldtype2));

                    } else if (isBuildInType(datatype.getTypeArguments().get(1))) {
                        return Optional.ofNullable(fromUdtValues(datatype.getTypeArguments().get(0), 
                                                                 datatype.getTypeArguments().get(1), 
                                                                 ImmutableMap.<Object, Object>copyOf(udtValue.getMap(fieldname, UDTValue.class, fieldtype2)), 
                                                                 fieldtype1, 
                                                                 fieldtype2));
                        
                    } else {
                        return Optional.ofNullable(fromUdtValues(datatype.getTypeArguments().get(0), 
                                                                 datatype.getTypeArguments().get(1), 
                                                                 ImmutableMap.<Object, Object>copyOf(udtValue.getMap(fieldname, UDTValue.class, UDTValue.class)),
                                                                 fieldtype1, 
                                                                 fieldtype2));
                    }
                }
                            
            // udt    
            } else {
                return Optional.ofNullable(fromUdtValue(datatype, 
                                                        udtValue, 
                                                        fieldtype1));
            }
        }
        

        
        public <T> T fromUdtValue(DataType datatype, UDTValue udtValue, Class<T> type) {
            return fromValues(type, (name, clazz1, clazz2) -> fromUdtValue(((UserType) datatype).getFieldType(name), 
                                                                           udtValue, 
                                                                           clazz1, 
                                                                           clazz2,
                                                                           name));
        }


        
        <T> ImmutableSet<T> fromUdtValues(DataType datatype, 
                                          ImmutableSet<UDTValue> udtValues, 
                                          Class<T> type) {
            Set<T> elements = Sets.newHashSet();
            
            for (UDTValue elementUdtValue : udtValues) {
                T element = fromValues(type, (name, clazz1, clazz2) -> fromUdtValue(((UserType) datatype).getFieldType(name), 
                                                                                    elementUdtValue, 
                                                                                    clazz1, 
                                                                                    clazz2, 
                                                                                    name));
                elements.add(element);
            }
            
            return ImmutableSet.copyOf(elements);
        }


        
        
        public <T> ImmutableList<T> fromUdtValues(DataType datatype, 
                                                  ImmutableList<UDTValue> udtValues, 
                                                  Class<T> type) {
            List<T> elements = Lists.newArrayList();
            
            for (UDTValue elementUdtValue : udtValues) {
                T element = fromValues(type, (name, clazz1, clazz2) -> fromUdtValue(((UserType) datatype).getFieldType(name), 
                                                                                     elementUdtValue, 
                                                                                     clazz1,
                                                                                     clazz2, 
                                                                                     name));
                elements.add(element);
            }
            
            return ImmutableList.copyOf(elements);
        }

        
        
        @SuppressWarnings("unchecked")
        public <K, V> ImmutableMap<K, V> fromUdtValues(DataType keyDatatype, 
                                                       DataType valueDatatype, 
                                                       ImmutableMap<?, ?> udtValues, 
                                                       Class<K> keystype, 
                                                       Class<V> valuesType) {
            
            Map<K, V> elements = Maps.newHashMap();
            
            for (Entry<?, ?> entry : udtValues.entrySet()) {
                
                K keyElement;
                if (keystype.isAssignableFrom(entry.getKey().getClass())) {
                    keyElement = (K) entry.getKey(); 
                } else {
                    keyElement = fromValues(keystype, (name, clazz1, clazz2) -> fromUdtValue(((UserType) keyDatatype).getFieldType(name), 
                                                                                             (UDTValue) entry.getKey(), 
                                                                                             clazz1, 
                                                                                             clazz2, 
                                                                                             name));
                }
                
                V valueElement;
                if (valuesType.isAssignableFrom(entry.getValue().getClass())) {
                    valueElement = (V) entry.getValue(); 
                } else {
                    valueElement = fromValues(valuesType, (name, clazz1, clazz2) -> fromUdtValue(((UserType) valueDatatype).getFieldType(name), 
                                                                                                  (UDTValue) entry.getValue(), 
                                                                                                  clazz1, 
                                                                                                  clazz2, 
                                                                                                  name));
                }

                elements.put(keyElement, valueElement);
            }
            
            return ImmutableMap.copyOf(elements);
        }
        
        
        @SuppressWarnings("unchecked")
        public Object toUdtValue(DataType datatype, Object value) {
            
            // build-in type (will not be converted)
            if (isBuildInType(datatype)) {
                return value;
                
            // udt collection
            } else if (datatype.isCollection()) {
               
               // set 
               if (Set.class.isAssignableFrom(datatype.getName().asJavaClass())) {
                   DataType elementDataType = datatype.getTypeArguments().get(0);
                   
                   Set<Object> udt = Sets.newHashSet();
                   if (value != null) {
                       for (Object element : (Set<Object>) value) {
                           udt.add(toUdtValue(elementDataType, element));
                       }
                   }
                   
                   return ImmutableSet.copyOf(udt);
                   
               // list 
               } else if (List.class.isAssignableFrom(datatype.getName().asJavaClass())) {
                   DataType elementDataType = datatype.getTypeArguments().get(0);
                   
                   List<Object> udt = Lists.newArrayList();
                   if (value != null) {
                       for (Object element : (List<Object>) value) {
                           udt.add(toUdtValue(elementDataType, element));
                       }
                   }
                   
                   return ImmutableList.copyOf(udt);
                  
               // map
               } else {
                   DataType keyDataType = datatype.getTypeArguments().get(0);
                   DataType valueDataType = datatype.getTypeArguments().get(1);
                   
                   Map<Object, Object> udt = Maps.newHashMap();
                   if (value != null) {
                       for (Entry<Object, Object> entry : ((Map<Object, Object>) value).entrySet()) {
                             udt.put(toUdtValue(keyDataType, entry.getKey()), 
                                     toUdtValue(valueDataType, entry.getValue()));
                       }
                   
                   }
                   return ImmutableMap.copyOf(udt);  
               }
        
               
            // udt
            } else {
                if (value == null) {
                    return value;
                    
                } else {
                    UserType usertype = getUserType(((UserType) datatype).getTypeName());
                    UDTValue udtValue = usertype.newValue();
                    
                    for (Entry<String, Optional<Object>> entry : ctx.toValues(value).entrySet()) {
                        DataType fieldType = usertype.getFieldType(entry.getKey());
                                
                        if (entry.getValue().isPresent()) {
                            Object vl = entry.getValue().get();
                            
                            if (!isBuildInType(usertype.getFieldType(entry.getKey()))) {
                                vl = toUdtValue(fieldType, vl);
                            }
                            
                            udtValue.setBytesUnsafe(entry.getKey(), fieldType.serialize(vl, ctx.getProtocolVersion()));
                        }
                    }
                    
                    return udtValue;
                }
            }
        }
    }
    
    
    
    protected class ResultImpl implements Result {
        private final ResultSet rs;
        
        public ResultImpl(ResultSet rs) {
            this.rs = rs;
        }
        
        @Override
        public boolean wasApplied() {
            return rs.wasApplied();
        }
        
        @Override
        public ExecutionInfo getExecutionInfo() {
            return rs.getExecutionInfo();
        }
        
        @Override
        public ImmutableList<ExecutionInfo> getAllExecutionInfo() {
            return ImmutableList.copyOf(rs.getAllExecutionInfo());
        }
        

        public String toString() {
            StringBuilder builder = new StringBuilder(); 
            for (ExecutionInfo info : getAllExecutionInfo())  {

                builder.append("queried=" + info.getQueriedHost());
                builder.append("\r\ntried=")
                       .append(Joiner.on(",").join(info.getTriedHosts()));


                if (info.getAchievedConsistencyLevel() != null) {
                    builder.append("\r\nachievedConsistencyLevel=" + info.getAchievedConsistencyLevel());
                }
                
                if (info.getQueryTrace() != null) {
                    builder.append("\r\ntraceid=" + info.getQueryTrace().getTraceId());
                    builder.append("\r\nevents:\r\n" + Joiner.on("\r\n").join(info.getQueryTrace().getEvents()));
                }
            }
            return builder.toString();
        }
    }
    
    
    private final class RecordImpl extends Record {
        
        public RecordImpl(Result result, Row row) {
            super(result, row);
        }
        

        @SuppressWarnings("unchecked")
        public <T> Optional<T> getObject(String name, Class<T> elementsClass) {
            if (isNull(name)) {
                return Optional.empty();
            }

            DataType datatype = getColumnDefinitions().getType(name);
            
            if (datatype != null) {
                if (isBuildInType(datatype)) {
                    return (Optional<T>) getBytesUnsafe(name).map(bytes -> datatype.deserialize(bytes, ctx.getProtocolVersion()));
                } else {
                    return Optional.ofNullable(valueMapper.fromUdtValue(datatype, getUDTValue(name).get(), elementsClass));
                }
            }
            
            return Optional.empty();
        }
        
        
        public <T> Optional<ImmutableSet<T>> getSet(String name, Class<T> elementsClass) {
            if (isNull(name)) {
                return Optional.empty();
            }

            DataType datatype = getColumnMetadata(name).getType();
            if (isBuildInType(datatype)) {
                return Optional.of(getRow().getSet(name, elementsClass)).map(set -> ImmutableSet.copyOf(set));
            } else {
                return Optional.of(getRow().getSet(name, UDTValue.class)).map(udtValues -> (ImmutableSet<T>) valueMapper.fromUdtValues(datatype.getTypeArguments().get(0), ImmutableSet.copyOf(udtValues), elementsClass));
            }
        }
        
     
        public <T> Optional<ImmutableList<T>> getList(String name, Class<T> elementsClass) {
            if (isNull(name)) {
                return Optional.empty();
            }
            
            DataType datatype = getColumnMetadata(name).getType();
            if (isBuildInType(datatype)) {
                return Optional.ofNullable(getRow().getList(name, elementsClass)).map(list -> ImmutableList.copyOf(list));
            } else {
                return Optional.of(getRow().getList(name, UDTValue.class)).map(udtValues -> (ImmutableList<T>) valueMapper.fromUdtValues(datatype.getTypeArguments().get(0), ImmutableList.copyOf(udtValues), elementsClass));
            }
        }
        
        
        public <K, V> Optional<ImmutableMap<K, V>> getMap(String name, Class<K> keysClass, Class<V> valuesClass) {
            if (isNull(name)) {
                return Optional.empty();
            }
            
            DataType datatype = getColumnMetadata(name).getType();
            if (isBuildInType(datatype)) {
                return Optional.ofNullable(getRow().getMap(name, keysClass, valuesClass)).map(map -> ImmutableMap.copyOf(map));
                
            } else {
                if (isBuildInType(datatype.getTypeArguments().get(0))) {
                    return Optional.of(getRow().getMap(name, keysClass, UDTValue.class)).map(udtValues -> (ImmutableMap<K, V>) valueMapper.fromUdtValues(datatype.getTypeArguments().get(0), datatype.getTypeArguments().get(1), ImmutableMap.copyOf(udtValues), keysClass, valuesClass));

                } else if (isBuildInType(datatype.getTypeArguments().get(1))) {
                    return Optional.of(getRow().getMap(name, UDTValue.class, valuesClass)).map(udtValues -> (ImmutableMap<K, V>) valueMapper.fromUdtValues(datatype.getTypeArguments().get(0), datatype.getTypeArguments().get(1), ImmutableMap.copyOf(udtValues), keysClass, valuesClass));
                    
                } else {
                    return isNull(name) ? Optional.empty() : Optional.of(getRow().getMap(name, UDTValue.class, UDTValue.class)).map(udtValues -> (ImmutableMap<K, V>) valueMapper.fromUdtValues(datatype.getTypeArguments().get(0), datatype.getTypeArguments().get(1), ImmutableMap.copyOf(udtValues), keysClass, valuesClass));
                }
            }
        }
        
        
        public String toString() {
            ToStringHelper toStringHelper = MoreObjects.toStringHelper(this);
            getRow().getColumnDefinitions().asList()
                                      .forEach(definition -> toString(definition.getName(), definition.getType()).ifPresent(value -> toStringHelper.add(definition.getName(), value)));
            return toStringHelper.toString();
        }
        
        
        private Optional<String> toString(String name, DataType dataType) {
            if (isNull(name)) {
                return Optional.empty();
            } else {
                StringBuilder builder = new StringBuilder();
                builder.append(dataType.deserialize(getRow().getBytesUnsafe(name), ctx.getProtocolVersion()));

                return Optional.of(builder.toString());
            }
        }
    }
    
    
    private final class RecordListImpl implements RecordList {
        private final ResultSet rs;

        private final Iterator<Row> iterator;
        private final AtomicReference<DatabaseSubscription> subscriptionRef = new AtomicReference<>();
        
        public RecordListImpl(ResultSet rs) {
            this.rs = rs;
            this.iterator = rs.iterator();
        }

        @Override
        public ExecutionInfo getExecutionInfo() {
            return rs.getExecutionInfo();
        }
        
        @Override
        public ImmutableList<ExecutionInfo> getAllExecutionInfo() {
            return ImmutableList.copyOf(rs.getAllExecutionInfo());
        }

        @Override
        public boolean wasApplied() {
            return rs.wasApplied();
        }
        
        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }
        
        @Override
        public Record next() {
            return new RecordImpl(this, iterator.next());
        }
        
        @Override
        public void subscribe(Subscriber<? super Record> subscriber) {
            synchronized (subscriptionRef) {
                if (subscriptionRef.get() == null) {
                    DatabaseSubscription subscription = new DatabaseSubscription(subscriber);
                    subscriptionRef.set(subscription);
                    subscriber.onSubscribe(subscription);
                } else {
                    subscriber.onError(new IllegalStateException("subription alreday exists. Multi-subscribe is not supported")); 
                }
            }
        }
        
        
        private final class DatabaseSubscription implements Subscription {
            private final Subscriber<? super Record> subscriber;
            
            private final AtomicLong numPendingReads = new AtomicLong();
            private final AtomicReference<Runnable> runningDatabaseQuery = new AtomicReference<>();
            
            public DatabaseSubscription(Subscriber<? super Record> subscriber) {
                this.subscriber = subscriber;
            }
            
            public void request(long n) {
                if (n > 0) {
                    numPendingReads.addAndGet(n);
                    processReadRequests();
                }
            }
            
            
            private void processReadRequests() {
                synchronized (this) {
                    long available = rs.getAvailableWithoutFetching();
                    long numToRead = numPendingReads.get();

                    // no records available?
                    if (available == 0) {
                        requestDatabaseForMoreRecords();
                      
                    // all requested available 
                    } else if (available >= numToRead) {
                        numPendingReads.addAndGet(-numToRead);
                        for (int i = 0; i < numToRead; i++) {
                            subscriber.onNext(next());
                        }                    
                        
                    // requested partly available                        
                    } else {
                        requestDatabaseForMoreRecords();
                        numPendingReads.addAndGet(-available);
                        for (int i = 0; i < available; i++) {
                            subscriber.onNext(next());
                        }
                    }
                }
            }
            
            
            private void requestDatabaseForMoreRecords() {
                if (rs.isFullyFetched()) {
                    cancel();
                }
                
                synchronized (this) {
                    if (runningDatabaseQuery.get() == null) {
                        Runnable databaseRequest = () -> { runningDatabaseQuery.set(null); processReadRequests(); };
                        runningDatabaseQuery.set(databaseRequest);
                        
                        ListenableFuture<Void> future = rs.fetchMoreResults();
                        future.addListener(databaseRequest, ForkJoinPool.commonPool());
                    }
                }
            }
       
            
            @Override
            public void cancel() {
                subscriber.onComplete();
            }
        }
    }
}


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



import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BatchStatement.Type;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Select.Selection;
import com.datastax.driver.core.ConsistencyLevel;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.unitedinternet.troilus.QueryFactory.ColumnToFetch;
import com.unitedinternet.troilus.QueryFactory.ValueToMutate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


 

@SuppressWarnings("rawtypes")
public class DaoImpl implements Dao {
    
    private static final Logger LOG = LoggerFactory.getLogger(DaoImpl.class);

    private final QueryFactory queryFactory = new QueryFactoryImpl();
    private final Context defaultContext;

    
    
    public DaoImpl(Context defaultContext) {
        this.defaultContext = defaultContext;
    }
 
    
    protected Context getDefaultContext() {
        return defaultContext;
    } 
    
    
    
    @Override
    public Dao withConsistency(ConsistencyLevel consistencyLevel) {
        return new DaoImpl(getDefaultContext().withConsistency(consistencyLevel));
    }
    
    @Override
    public Dao withSerialConsistency(ConsistencyLevel consistencyLevel) {
        return new DaoImpl(getDefaultContext().withSerialConsistency(consistencyLevel));
    }
    
    
    
    @Override
    public Insertion writeEntity(Object entity) {
        return newInsertion(getDefaultContext(), ImmutableList.of(), false).values(getDefaultContext().toValues(entity));
    }
    
    protected InsertWithValues newInsertion(Context ctx, ImmutableList<? extends ValueToMutate> valuesToMutate, boolean ifNotExists) {
        return new InsertQuery(ctx, queryFactory, valuesToMutate, ifNotExists);
    }
    
    
    @Override
    public UpdateWithValues writeWhere(Clause... clauses) {
        return newUpdate(getDefaultContext(), ImmutableList.of(), ImmutableMap.of(), ImmutableList.copyOf(clauses), ImmutableList.of());
    }
    
    protected UpdateWithValues newUpdate(Context ctx, ImmutableList<? extends ValueToMutate> valuesToMutate, ImmutableMap<String, Object> keys, ImmutableList<Clause> whereConditions, ImmutableList<Clause> ifConditions) {
        return new UpdateQuery(ctx, queryFactory, valuesToMutate, keys, whereConditions, ifConditions);
    }
    

    @Override
    public WriteWithValues writeWithKey(String keyName, Object keyValue) {
        return newWrite(getDefaultContext(), ImmutableMap.of(keyName, keyValue), ImmutableList.of());
    }
    
    @Override
    public WriteWithValues writeWithKey(String keyName1, Object keyValue1, String keyName2, Object keyValue2) {
        return newWrite(getDefaultContext(), ImmutableMap.of(keyName1, keyValue1, keyName2, keyValue2), ImmutableList.of());
    }
    
    @Override
    public WriteWithValues writeWithKey(String keyName1, Object keyValue1, String keyName2, Object keyValue2, String keyName3, Object keyValue3) {
        return newWrite(getDefaultContext(), ImmutableMap.of(keyName1, keyValue1, keyName2, keyValue2, keyName3, keyValue3), ImmutableList.of());
    }

    @Override
    public WriteWithValues writeWithKey(String keyName1, Object keyValue1, String keyName2, Object keyValue2, String keyName3, Object keyValue3, String keyName4, Object keyValue4) {
        return newWrite(getDefaultContext(), ImmutableMap.of(keyName1, keyValue1, keyName2, keyValue2, keyName3, keyValue3, keyName4, keyValue4), ImmutableList.of());
    }

    protected WriteWithValues newWrite(Context ctx, ImmutableMap<String, Object> keys, ImmutableList<? extends ValueToMutate> valuesToInsert) {
        return new WriteQuery(ctx, queryFactory, keys, valuesToInsert);
    }
    
    
    @Override
    public Deletion deleteWhere(Clause... whereConditions) {
        return newDeletion(getDefaultContext(), 
                           ImmutableMap.of(),
                           ImmutableList.copyOf(whereConditions),
                           ImmutableList.of());
    };
    
    @Override
    public Deletion deleteWithKey(String keyName, Object keyValue) {
        
        return newDeletion(getDefaultContext(), 
                           ImmutableMap.of(keyName, keyValue),
                           ImmutableList.of(),
                           ImmutableList.of());
    }

    @Override
    public Deletion deleteWithKey(String keyName1, Object keyValue1, 
                                  String keyName2, Object keyValue2) {
        
        return newDeletion(getDefaultContext(), 
                           ImmutableMap.of(keyName1, keyValue1,
                                           keyName2, keyValue2),
                           ImmutableList.of(),
                           ImmutableList.of());
    }
    
    @Override
    public Deletion deleteWithKey(String keyName1, Object keyValue1, 
                                  String keyName2, Object keyValue2, 
                                  String keyName3, Object keyValue3) {
        
        return newDeletion(getDefaultContext(), 
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
        
        return newDeletion(getDefaultContext(), 
                           ImmutableMap.of(keyName1, keyValue1, 
                                           keyName2, keyValue2, 
                                           keyName3, keyValue3, 
                                           keyName4, keyValue4),
                           ImmutableList.of(),
                           ImmutableList.of());
    }
    
    protected Deletion newDeletion(Context ctx, ImmutableMap<String, Object> keyNameValuePairs, ImmutableList<Clause> whereConditions, ImmutableList<Clause> ifConditions) {
        return new DeleteQuery(ctx, queryFactory, keyNameValuePairs, whereConditions, ifConditions);
    }
    
    
    protected BatchMutation newBatchMutation(Context ctx, Type type, ImmutableList<Mutation<?>> mutations) {
        return new MutationBatchQuery(ctx, queryFactory, type, mutations);
    }
    
    
    @Override
    public SingleReadWithUnit<Optional<Record>> readWithKey(String keyName, Object keyValue) {
        return newSingleSelection(getDefaultContext(), ImmutableMap.of(keyName, keyValue), Optional.empty());
    }
    
    @Override
    public SingleReadWithUnit<Optional<Record>> readWithKey(String keyName1, Object keyValue1, String keyName2, Object keyValue2) {
        return newSingleSelection(getDefaultContext(), ImmutableMap.of(keyName1, keyValue1, keyName2, keyValue2), Optional.of(ImmutableSet.of()));
    }
    
    @Override
    public SingleReadWithUnit<Optional<Record>> readWithKey(String keyName1, Object keyValue1, String keyName2, Object keyValue2, String keyName3, Object keyValue3) {
        return newSingleSelection(getDefaultContext(), ImmutableMap.of(keyName1, keyValue1, keyName2, keyValue2, keyName3, keyValue3), Optional.of(ImmutableSet.of()));
    }
    
    @Override
    public SingleReadWithUnit<Optional<Record>> readWithKey(String keyName1, Object keyValue1, String keyName2, Object keyValue2, String keyName3, Object keyValue3, String keyName4, Object keyValue4) {
        return newSingleSelection(getDefaultContext(), ImmutableMap.of(keyName1, keyValue1, keyName2, keyValue2, keyName3, keyValue3, keyName4, keyValue4), Optional.of(ImmutableSet.of()));
    }
    
    protected SingleReadWithUnit<Optional<Record>> newSingleSelection(Context ctx, 
                                                                      ImmutableMap<String, Object> keyNameValuePairs, 
                                                                      Optional<ImmutableSet<ColumnToFetch>> optionalColumnsToFetch) {
        return new SingleReadQuery(ctx, queryFactory, keyNameValuePairs, optionalColumnsToFetch);
    }
    
    
    protected <E> SingleRead<Optional<E>> newSingleSelection(Context ctx, SingleRead<Optional<Record>> read, Class<?> clazz) {
        return new SingleEntityReadQuery<E>(ctx, queryFactory, read, clazz);
    }

    
    private final class QueryFactoryImpl implements QueryFactory {
        
        @Override
        public InsertWithValues newInsertion(Context ctx, ImmutableList<? extends ValueToMutate> valuesToMutate, boolean ifNotExists) {
            return DaoImpl.this.newInsertion(ctx, valuesToMutate, ifNotExists);
        }
        
        @Override
        public UpdateWithValues newUpdate(Context ctx, ImmutableList<? extends ValueToMutate> valuesToMutate, ImmutableMap<String, Object> keys, ImmutableList<Clause> whereConditions, ImmutableList<Clause> ifConditions) {
            return DaoImpl.this.newUpdate(ctx, valuesToMutate, keys, whereConditions, ifConditions);
        }
        
        @Override
        public WriteWithValues newWrite(Context ctx, ImmutableMap<String, Object> keys, ImmutableList<? extends ValueToMutate> valuesToInsert) {
            return DaoImpl.this.newWrite(ctx, keys, valuesToInsert);
        }
        
        @Override
        public Deletion newDeletion(Context ctx, ImmutableMap<String, Object> keyNameValuePairs, ImmutableList<Clause> whereConditions, ImmutableList<Clause> ifConditions) {
            return DaoImpl.this.newDeletion(ctx, keyNameValuePairs, whereConditions, ifConditions);
        }
        
        @Override
        public <E> SingleRead<Optional<E>> newSingleSelection(Context ctx, SingleRead<Optional<Record>> read, Class<?> clazz) {
            return DaoImpl.this.newSingleSelection(ctx, read, clazz);
        }
        
        @Override
        public SingleReadWithUnit<Optional<Record>> newSingleSelection(Context ctx, ImmutableMap<String, Object> keyNameValuePairs, Optional<ImmutableSet<ColumnToFetch>> optionalColumnsToFetch) {
            return DaoImpl.this.newSingleSelection(ctx, keyNameValuePairs, optionalColumnsToFetch);
        }
        
        @Override
        public ListRead<Count> newCountRead(Context ctx, ImmutableSet<Clause> clauses, Optional<Integer> optionalLimit, Optional<Boolean> optionalAllowFiltering, Optional<Integer> optionalFetchSize, Optional<Boolean> optionalDistinct) {
            return DaoImpl.this.newCountRead(ctx, clauses, optionalLimit, optionalAllowFiltering, optionalFetchSize, optionalDistinct);
        }
        
        @Override
        public <E> ListRead<EntityList<E>> newListSelection(Context ctx, ListRead<RecordList> read, Class<?> clazz) {
            return DaoImpl.this.newListSelection(ctx, read, clazz);
        }
        
        @Override
        public ListReadWithUnit<RecordList> newListSelection(Context ctx, 
                                                             ImmutableSet<Clause> clauses,
                                                             Optional<ImmutableSet<ColumnToFetch>> columnsToFetch,
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
        
        @Override
        public BatchMutation newBatchMutation(Context ctx, Type type, ImmutableList<Mutation<?>> mutations) {
            return DaoImpl.this.newBatchMutation(ctx, type, mutations);
        }
    }
    

    
    private static class InsertQuery implements InsertWithValues {
        private final Context ctx;
        private final QueryFactory queryFactory;
        private final ImmutableList<? extends ValueToMutate> valuesToMutate;
        private final boolean ifNotExists;

        
        public InsertQuery(Context ctx, QueryFactory queryFactory, ImmutableList<? extends ValueToMutate> valuesToMutate, boolean ifNotExists) {
            this.ctx = ctx;
            this.queryFactory = queryFactory;
            this.valuesToMutate = valuesToMutate;
            this.ifNotExists = ifNotExists;
        }
                
        
        @Override
        public final  InsertWithValues values(ImmutableMap<String, ? extends Object> additionalvaluesToMutate) {
            InsertWithValues write = this;
            for (String name : additionalvaluesToMutate.keySet()) {
                write = write.value(name, additionalvaluesToMutate.get(name));
            }
            return write;
        }
        
          
        @Override
        public InsertWithValues value(String name, Object value) {
            if (ctx.isOptional(value)) {
                Optional<Object> optional = (Optional<Object>) value;
                if (optional.isPresent()) {
                    return value(name, optional.get());
                } else {
                    return this;
                }
            }
            
            ImmutableList<? extends ValueToMutate> newValuesToInsert;
            
            if (ctx.isBuildInType(ctx.getColumnMetadata(name).getType())) {
                newValuesToInsert = ImmutableList.<ValueToMutate>builder().addAll(valuesToMutate).add(new BuildinValueToMutate(name, value)).build();
            } else {
                newValuesToInsert = ImmutableList.<ValueToMutate>builder().addAll(valuesToMutate).add(new UDTValueToMutate(name, value)).build();
            }
            
            return queryFactory.newInsertion(ctx, newValuesToInsert, ifNotExists);
        }
           
        
        @Override
        public Insertion withEnableTracking() {
            return queryFactory.newInsertion(ctx.withEnableTracking(), valuesToMutate, ifNotExists);
        }
        
        @Override
        public Insertion withDisableTracking() {
            return queryFactory.newInsertion(ctx.withDisableTracking(), valuesToMutate, ifNotExists);
        }
        
        @Override
        public Insertion withConsistency(ConsistencyLevel consistencyLevel) {
            return queryFactory.newInsertion(ctx.withConsistency(consistencyLevel), valuesToMutate, ifNotExists);
        }
        
        
        @Override
        public Insertion withSerialConsistency(ConsistencyLevel consistencyLevel) {
            return queryFactory.newInsertion(ctx.withSerialConsistency(consistencyLevel), valuesToMutate, ifNotExists);
        }
        
        
        @Override
        public Insertion ifNotExits() {
            return queryFactory.newInsertion(ctx, valuesToMutate, true);
        }
        
        @Override
        public Insertion withTtl(Duration ttl) {
            return queryFactory.newInsertion(ctx.withTtl(ttl), valuesToMutate, ifNotExists);
        }

        @Override
        public Insertion withWritetime(long writetimeMicrosSinceEpoch) {
            return queryFactory.newInsertion(ctx.withWritetime(writetimeMicrosSinceEpoch), valuesToMutate, ifNotExists);
        }
        
        @Override
        public BatchMutation combinedWith(Mutation other) {
            return queryFactory.newBatchMutation(ctx, Type.LOGGED, ImmutableList.of(this, other));
        }
      
        
        @Override
        public Statement getStatement() {
            
            // statement
            Insert insert = insertInto(ctx.getTable());
            
            List<Object> values = Lists.newArrayList();
            valuesToMutate.forEach(valueToInsert -> values.add(valueToInsert.addPreparedToStatement(ctx, insert)));
            
            
            if (ifNotExists) {
                insert.ifNotExists();
                ctx.getSerialConsistencyLevel().ifPresent(serialCL -> insert.setSerialConsistencyLevel(serialCL));
            }

            ctx.getTtl().ifPresent(ttl-> {
                                            insert.using(QueryBuilder.ttl(bindMarker()));
                                            values.add((int) ttl.getSeconds());
                                         });

            PreparedStatement stmt = ctx.prepare(insert);
            return stmt.bind(values.toArray());
        }
        
        
        public Result execute() {
            try {
                return executeAsync().get(Long.MAX_VALUE, TimeUnit.DAYS);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                throw Exceptions.unwrapIfNecessary(e);
            } 
        }
        
        
        @Override
        public CompletableFuture<Result> executeAsync() {
            return ctx.performAsync(getStatement()).thenApply(resultSet -> {
                    if (ifNotExists) {
                        // check cas result column '[applied]'
                        if (!resultSet.wasApplied()) {
                            throw new IfConditionException("duplicated entry");  
                        }
                    } 
                    return Result.newResult(resultSet);
                });
        }
        
        @Override
        public String toString() {
            return getStatement().toString();
        }
    }

    
    
 
    private static class UpdateQuery implements UpdateWithValues {
        private final Context ctx;
        private final QueryFactory queryFactory;
        private final ImmutableMap<String, Object> keys;
        private final ImmutableList<Clause> whereConditions;
        private final ImmutableList<? extends ValueToMutate> valuesToMutate;
        private final ImmutableList<Clause> ifConditions;
        
        public UpdateQuery(Context ctx, QueryFactory queryFactory, ImmutableList<? extends ValueToMutate> valuesToMutate, ImmutableMap<String, Object> keys, ImmutableList<Clause> whereConditions, ImmutableList<Clause> ifConditions) {
            this.ctx = ctx;
            this.queryFactory = queryFactory;
            this.valuesToMutate = valuesToMutate;
            this.keys = keys;
            this.whereConditions = whereConditions;
            this.ifConditions = ifConditions;
        }
        
        
        @Override
        public Update onlyIf(Clause... conditions) {
            return queryFactory.newUpdate(ctx, valuesToMutate, keys, whereConditions, ImmutableList.copyOf(conditions)) ;
        }
        
        @Override
        public final  UpdateWithValues values(ImmutableMap<String, ? extends Object> additionalvaluesToMutate) {
            UpdateWithValues write = this;
            for (String name : additionalvaluesToMutate.keySet()) {
                write = write.value(name, additionalvaluesToMutate.get(name));
            }
            return write;
        }
        
     
        @Override
        public UpdateWithValues value(String name, Object value) {
            if (ctx.isOptional(value)) {
                Optional<Object> optional = (Optional<Object>) value;
                if (optional.isPresent()) {
                    return value(name, optional.get());
                } else {
                    return this;
                }
            }
            
            ImmutableList<? extends ValueToMutate> newValuesToInsert;
            
            if (ctx.isBuildInType(ctx.getColumnMetadata(name).getType())) {
                newValuesToInsert = ImmutableList.<ValueToMutate>builder().addAll(valuesToMutate).add(new BuildinValueToMutate(name, value)).build();
            } else {
                newValuesToInsert = ImmutableList.<ValueToMutate>builder().addAll(valuesToMutate).add(new UDTValueToMutate(name, value)).build();
            }
            
            return queryFactory.newUpdate(ctx, newValuesToInsert, keys, whereConditions, ifConditions);
        }
           
        
        @Override
        public Update withEnableTracking() {
            return queryFactory.newUpdate(ctx.withEnableTracking(), valuesToMutate, keys, whereConditions, ifConditions);
        }
        
        @Override
        public Update withDisableTracking() {
            return queryFactory.newUpdate(ctx.withDisableTracking(), valuesToMutate, keys, whereConditions, ifConditions);
        }
        
        
        @Override
        public Update withConsistency(ConsistencyLevel consistencyLevel) {
            return queryFactory.newUpdate(ctx.withConsistency(consistencyLevel), valuesToMutate, keys, whereConditions, ifConditions);
        }
        
        
        @Override
        public Update withSerialConsistency(ConsistencyLevel consistencyLevel) {
            return queryFactory.newUpdate(ctx.withSerialConsistency(consistencyLevel), valuesToMutate, keys, whereConditions, ifConditions);
        }
        
        
        @Override
        public Update withTtl(Duration ttl) {
            return queryFactory.newUpdate(ctx.withTtl(ttl), valuesToMutate, keys, whereConditions, ifConditions);
        }

        @Override
        public Update withWritetime(long writetimeMicrosSinceEpoch) {
            return queryFactory.newUpdate(ctx.withWritetime(writetimeMicrosSinceEpoch), valuesToMutate, keys, whereConditions, ifConditions);
        }
        
        @Override
        public BatchMutation combinedWith(Mutation other) {
            return queryFactory.newBatchMutation(ctx, Type.LOGGED, ImmutableList.of(this, other));
        }
      
        
        @Override
        public Statement getStatement() {
            
            // statement
            com.datastax.driver.core.querybuilder.Update update = update(ctx.getTable());
            
            ifConditions.forEach(condition -> update.onlyIf(condition));

            // key-based update
            if (whereConditions.isEmpty()) {
                List<Object> values = Lists.newArrayList();
                valuesToMutate.forEach(valueToInsert -> values.add(valueToInsert.addPreparedToStatement(ctx, update)));
                
                keys.keySet().forEach(keyname -> { update.where(eq(keyname, bindMarker())); values.add(keys.get(keyname)); } );
                
                
                ifConditions.forEach(condition -> update.onlyIf(condition));
       
                ctx.getTtl().ifPresent(ttl-> {
                                                update.using(QueryBuilder.ttl(bindMarker()));
                                                values.add((int) ttl.getSeconds());
                                             });
                
                PreparedStatement stmt = ctx.prepare(update);
                return stmt.bind(values.toArray());

                
            // where condition-based update
            } else {
                valuesToMutate.forEach(valueToInsert -> valueToInsert.addToStatement(ctx, update));
                
                ctx.getTtl().ifPresent(ttl-> update.using(QueryBuilder.ttl((int) ttl.getSeconds())));
                
                com.datastax.driver.core.querybuilder.Update.Where where = null;
                
                for (Clause whereClause : whereConditions) {
                    if (where == null) {
                        where = update.where(whereClause);
                    } else {
                        where = where.and(whereClause);
                    }
                }
                
                return update;
            }
        }
        
        
        public Result execute() {
            try {
                return executeAsync().get(Long.MAX_VALUE, TimeUnit.DAYS);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                throw Exceptions.unwrapIfNecessary(e);
            } 
        }
        
        
        @Override
        public CompletableFuture<Result> executeAsync() {
            return ctx.performAsync(getStatement()).thenApply(resultSet -> {
                if (!ifConditions.isEmpty()) {
                    // check cas result column '[applied]'
                    if (!resultSet.wasApplied()) {
                        throw new IfConditionException("if condition does not match");  
                    }
                } 
                return Result.newResult(resultSet);
            });

        }
        
        @Override
        public String toString() {
            return getStatement().toString();
        }
    }

    
    
    private static class WriteQuery implements WriteWithValues {
        private final Context ctx;
        private final QueryFactory queryFactory;
        private final ImmutableMap<String, Object> keys;
        private final ImmutableList<? extends ValueToMutate> valuesToMutate;
        
        public WriteQuery(Context ctx, QueryFactory queryFactory, ImmutableMap<String, Object> keys, ImmutableList<? extends ValueToMutate> valuesToMutate) {
            this.ctx = ctx;
            this.queryFactory = queryFactory;
            this.keys = keys;
            this.valuesToMutate = valuesToMutate;
        }
        
   
        @Override
        public Update onlyIf(Clause... conditions) {
            return queryFactory.newUpdate(ctx, valuesToMutate, keys, ImmutableList.of(), ImmutableList.copyOf(conditions)); 
        }

        @Override
        public Insertion ifNotExits() {
            return queryFactory.newInsertion(ctx, valuesToMutate, false).values(keys).ifNotExits();
        }
        
        @Override
        public final  WriteWithValues values(ImmutableMap<String, ? extends Object> valuesToMutate) {
            WriteWithValues write = this;
            for (String name : valuesToMutate.keySet()) {
                write = write.value(name, valuesToMutate.get(name));
            }
            return write;
        }
        
     
        @Override
        public WriteWithValues value(String name, Object value) {
            
            if (ctx.isOptional(value)) {
                Optional<Object> optional = (Optional<Object>) value;
                if (optional.isPresent()) {
                    return value(name, optional.get());
                } else {
                    return this;
                }
            }
            
            ImmutableList<? extends ValueToMutate> newValuesToInsert;
            
            if (ctx.isBuildInType(ctx.getColumnMetadata(name).getType())) {
                newValuesToInsert = ImmutableList.<ValueToMutate>builder().addAll(valuesToMutate).add(new BuildinValueToMutate(name, value)).build();
            } else {
                newValuesToInsert = ImmutableList.<ValueToMutate>builder().addAll(valuesToMutate).add(new UDTValueToMutate(name, value)).build();
            }
            
            return queryFactory.newWrite(ctx, keys, newValuesToInsert);
        }

        @Override
        public Write withEnableTracking() {
            return queryFactory.newWrite(ctx.withEnableTracking(), keys, valuesToMutate);
        }
        
        @Override
        public Write withDisableTracking() {
            return queryFactory.newWrite(ctx.withDisableTracking(), keys, valuesToMutate);
        }
        
        
        @Override
        public Write withConsistency(ConsistencyLevel consistencyLevel) {
            return queryFactory.newWrite(ctx.withConsistency(consistencyLevel), keys, valuesToMutate);
        }
        
        
        @Override
        public Write withSerialConsistency(ConsistencyLevel consistencyLevel) {
            return queryFactory.newWrite(ctx.withSerialConsistency(consistencyLevel), keys, valuesToMutate);
        }
        
        
        @Override
        public Write withTtl(Duration ttl) {
            return queryFactory.newWrite(ctx.withTtl(ttl), keys, valuesToMutate);
        }

        @Override
        public Write withWritetime(long writetimeMicrosSinceEpoch) {
            return queryFactory.newWrite(ctx.withWritetime(writetimeMicrosSinceEpoch), keys, valuesToMutate);
        }
        
        @Override
        public BatchMutation combinedWith(Mutation other) {
            return queryFactory.newBatchMutation(ctx, Type.LOGGED, ImmutableList.of(this, other));
        }
      
        
        @Override
        public Statement getStatement() {
            
            // statement
            com.datastax.driver.core.querybuilder.Update update = update(ctx.getTable());
            
            
            
            List<Object> values = Lists.newArrayList();
            valuesToMutate.forEach(valueToInsert -> values.add(valueToInsert.addPreparedToStatement(ctx, update)));
            
            keys.keySet().forEach(keyname -> { update.where(eq(keyname, bindMarker())); values.add(keys.get(keyname)); } );
            
    
            ctx.getTtl().ifPresent(ttl-> {
                                            update.using(QueryBuilder.ttl(bindMarker()));
                                            values.add((int) ttl.getSeconds());
                                         });

            PreparedStatement stmt = ctx.prepare(update);
            return stmt.bind(values.toArray());
        }
        
        
        public Result execute() {
            try {
                return executeAsync().get(Long.MAX_VALUE, TimeUnit.DAYS);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                throw Exceptions.unwrapIfNecessary(e);
            } 
        }
        
        
        @Override
        public CompletableFuture<Result> executeAsync() {
            return ctx.performAsync(getStatement()).thenApply(resultSet -> Result.newResult(resultSet));
        }
        
        @Override
        public String toString() {
            return getStatement().toString();
        }
    }

    


  
    
    
    private static final class BuildinValueToMutate implements ValueToMutate {
        private final String name;
        private final Object value;
        
        @SuppressWarnings("unchecked")
        public BuildinValueToMutate(String name, Object value) {
            this.name = name;
            if (value instanceof Optional) {
                this.value = ((Optional) value).orElse(null);
            } else {
                this.value = value;
            }
        }
        
        
        @Override
        public String toString() {
            return name + "=" + value;
        }
        
        
        @Override
        public Object addPreparedToStatement(Context ctx, Insert insert) {
            insert.value(name, bindMarker());
            return value;
        }
        
        @Override
        public void addToStatement(Context ctx, Insert insert) {
            insert.value(name,  value);
        }

        public Object addPreparedToStatement(Context ctx, com.datastax.driver.core.querybuilder.Update update) {
            update.with(set(name, bindMarker()));
            return value;
        }
        
        
        @Override
        public void addToStatement(Context ctx, com.datastax.driver.core.querybuilder.Update update) {
            update.with(set(name, value));
        }
    }
   
    
    
    private static final class UDTValueToMutate implements ValueToMutate {
        private final String columnName;
        private final Object value;
        
        @SuppressWarnings("unchecked")
        public UDTValueToMutate(String columnName, Object value) {
            this.columnName = columnName;
            if (value instanceof Optional) {
                this.value = ((Optional) value).orElse(null);
            } else {
                this.value = value;
            }
        }
        
        
        @Override
        public String toString() {
            return columnName + "=" + value;
        }
        
        
        @Override
        public Object addPreparedToStatement(Context ctx, Insert insert) {
            insert.value(columnName, bindMarker());
            return UDTValueMapper.toUdtValue(ctx, ctx.getColumnMetadata(columnName).getType(), value);
        }

        @Override
        public void addToStatement(Context ctx, Insert insert) {
            insert.value(columnName, value);
        }
        
        public Object addPreparedToStatement(Context ctx, com.datastax.driver.core.querybuilder.Update update) {
            update.with(set(columnName, bindMarker()));
            return UDTValueMapper.toUdtValue(ctx, ctx.getColumnMetadata(columnName).getType(), value);
        }
        
        @Override
        public void addToStatement(Context ctx, com.datastax.driver.core.querybuilder.Update update) {
            update.with(set(columnName, UDTValueMapper.toUdtValue(ctx, ctx.getColumnMetadata(columnName).getType(), value)));
        }
    }
   
   
    
    private static class DeleteQuery implements Deletion {
        private final Context ctx;
        private final QueryFactory queryFactory;
        private final ImmutableMap<String, Object> keyNameValuePairs;
        private final ImmutableList<Clause> whereConditions;
        private final ImmutableList<Clause> ifConditions;
         
        public DeleteQuery(Context ctx, QueryFactory queryFactory, ImmutableMap<String, Object> keyNameValuePairs, ImmutableList<Clause> whereConditions, ImmutableList<Clause> ifConditions) {
            this.ctx = ctx;
            this.queryFactory = queryFactory;
            this.keyNameValuePairs = keyNameValuePairs;
            this.whereConditions = whereConditions;
            this.ifConditions = ifConditions;
        }
        
        @Override
        public Deletion onlyIf(Clause... conditions) {
            return queryFactory.newDeletion(ctx.withEnableTracking(), keyNameValuePairs, whereConditions, ImmutableList.copyOf(conditions));
        }
        
        @Override
        public Deletion withEnableTracking() {
            return queryFactory.newDeletion(ctx.withEnableTracking(), keyNameValuePairs, whereConditions, ifConditions);
        }
        
        @Override
        public Deletion withDisableTracking() {
            return queryFactory.newDeletion(ctx.withDisableTracking(), keyNameValuePairs, whereConditions, ifConditions);
        }
        
        
        @Override
        public Deletion withConsistency(ConsistencyLevel consistencyLevel) {
            return queryFactory.newDeletion(ctx.withConsistency(consistencyLevel), keyNameValuePairs, whereConditions, ifConditions);
        }
        
        @Override
        public Deletion withSerialConsistency(ConsistencyLevel consistencyLevel) {
            return queryFactory.newDeletion(ctx.withSerialConsistency(consistencyLevel), keyNameValuePairs, whereConditions, ifConditions);
        }
        
        @Override
        public BatchMutation combinedWith(Mutation other) {
            return queryFactory.newBatchMutation(ctx, Type.LOGGED, ImmutableList.of(this, other));
        }
        
        
        @Override
        public Statement getStatement() {
            Delete delete = delete().from(ctx.getTable());

            // key-based delete    
            if (whereConditions.isEmpty()) {
                ifConditions.forEach(condition -> delete.onlyIf(condition));
                
                Delete.Where where = null;
                
                if (!keyNameValuePairs.isEmpty()) {
                    for (Clause whereClause : keyNameValuePairs.keySet().stream().map(name -> eq(name, bindMarker())).collect(Immutables.toSet())) {
                        if (where == null) {
                            where = delete.where(whereClause);
                        } else {
                            where = where.and(whereClause);
                        }
                    }
                }
                
                return ctx.prepare(delete).bind(keyNameValuePairs.values().toArray());

                
            // where condition-based delete    
            } else {
                Delete.Where where = null;
                
                ifConditions.forEach(condition -> delete.onlyIf(condition));
                
                for (Clause whereClause : whereConditions) {
                    if (where == null) {
                        where = delete.where(whereClause);
                    } else {
                        where = where.and(whereClause);
                    }
                }
               
                return delete;
            }
        }
        
        
        public Result execute() {
            try {
                return executeAsync().get(Long.MAX_VALUE, TimeUnit.DAYS);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                throw Exceptions.unwrapIfNecessary(e);
            } 
        }
        
        
        public CompletableFuture<Result> executeAsync() {
            return ctx.performAsync(getStatement()).thenApply(resultSet -> Result.newResult(resultSet));
        }
    }


    
     
    
    private final static class MutationBatchQuery implements BatchMutation {
        private final Context ctx;
        private final QueryFactory queryFactory;
        private final ImmutableList<Mutation<?>> mutations;
        private final Type type;  
        
        public MutationBatchQuery(Context ctx, QueryFactory queryFactory, Type type, ImmutableList<Mutation<?>> mutations) {
            this.ctx = ctx;
            this.queryFactory = queryFactory;
            this.type = type;
            this.mutations = mutations;
        }
                
        @Override
        public BatchMutation withEnableTracking() {
            return queryFactory.newBatchMutation(ctx.withEnableTracking(), type, mutations);
        }
        
        @Override
        public BatchMutation withDisableTracking() {
            return queryFactory.newBatchMutation(ctx.withDisableTracking(), type, mutations);
        }
        
        
        @Override
        public Query<Result> withLockedBatchType() {
            return queryFactory.newBatchMutation(ctx, Type.LOGGED, mutations);
        }
        
        @Override
        public Query<Result> withUnlockedBatchType() {
            return queryFactory.newBatchMutation(ctx, Type.UNLOGGED, mutations);
        }
        
         
        @Override
        public BatchMutation combinedWith(Mutation<?> other) {
            return queryFactory.newBatchMutation(ctx, type, Immutables.merge(mutations, other));
        }
        
        @Override
        public Statement getStatement() {
            BatchStatement batchStmt = new BatchStatement(type);
            mutations.forEach(mutation -> batchStmt.add(((Batchable) mutation).getStatement()));
            return batchStmt;
        }
        
        public Result execute() {
            try {
                return executeAsync().get(Long.MAX_VALUE, TimeUnit.DAYS);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                throw Exceptions.unwrapIfNecessary(e);
            } 
        }
        
        public CompletableFuture<Result> executeAsync() {
            return ctx.performAsync(getStatement()).thenApply(resultSet -> Result.newResult(resultSet));
        }
    }
    
    
    
    
    


    private static class SingleReadQuery implements SingleReadWithUnit<Optional<Record>> {
        private final Context ctx;
        private final QueryFactory queryFactory;
        private final ImmutableMap<String, Object> keyNameValuePairs;
        private final Optional<ImmutableSet<ColumnToFetch>> optionalColumnsToFetch;
         
        
        public SingleReadQuery(Context ctx, QueryFactory queryFactory, ImmutableMap<String, Object> keyNameValuePairs, Optional<ImmutableSet<ColumnToFetch>> optionalColumnsToFetch) {
            this.ctx = ctx;
            this.queryFactory = queryFactory;
            this.keyNameValuePairs = keyNameValuePairs;
            this.optionalColumnsToFetch = optionalColumnsToFetch;
        }
         
        
        @Override
        public SingleRead<Optional<Record>> all() {
            return queryFactory.newSingleSelection(ctx, keyNameValuePairs, Optional.empty());
        }
        
        @Override
        public <E> SingleRead<Optional<E>> asEntity(Class<E> objectClass) {
            return queryFactory.newSingleSelection(ctx, this, objectClass);
        }
        
        @Override
        public SingleReadWithUnit<Optional<Record>> column(String name) {
            return queryFactory.newSingleSelection(ctx, keyNameValuePairs, Immutables.merge(optionalColumnsToFetch, ColumnToFetch.create(name, false, false)));
        }

        @Override
        public SingleReadWithColumns<Optional<Record>> columnWithMetadata(String name) {
            return queryFactory.newSingleSelection(ctx, keyNameValuePairs, Immutables.merge(optionalColumnsToFetch, ColumnToFetch.create(name, true, true)));
        }
        
        @Override
        public SingleReadWithUnit<Optional<Record>> columns(String... names) {
            return columns(ImmutableSet.copyOf(names));
        }
        
        @Override 
        public SingleReadWithUnit<Optional<Record>> columns(ImmutableCollection<String> namesToRead) {
            return queryFactory.newSingleSelection(ctx, keyNameValuePairs, Immutables.merge(optionalColumnsToFetch, ColumnToFetch.create(namesToRead)));
        }
        
        @Override
        public SingleRead<Optional<Record>> withEnableTracking() {
            return queryFactory.newSingleSelection(ctx.withEnableTracking(), keyNameValuePairs, optionalColumnsToFetch);
        }
        
        @Override
        public SingleRead<Optional<Record>> withDisableTracking() {
            return queryFactory.newSingleSelection(ctx.withDisableTracking(), keyNameValuePairs, optionalColumnsToFetch);
        }
        
        @Override
        public SingleRead<Optional<Record>> withConsistency(ConsistencyLevel consistencyLevel) {
            return queryFactory.newSingleSelection(ctx.withConsistency(consistencyLevel), keyNameValuePairs, optionalColumnsToFetch);
        }
       
        
        @Override
        public Optional<Record> execute() {
            try {
                return executeAsync().get(Long.MAX_VALUE, TimeUnit.DAYS);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                throw Exceptions.unwrapIfNecessary(e);
            }
        }
        
        
        @Override
        public CompletableFuture<Optional<Record>> executeAsync() {
            
            Selection selection = select();
            
            if (optionalColumnsToFetch.isPresent()) {
                optionalColumnsToFetch.get().forEach(column -> column.accept(selection));

                // add key columns for paranoia checks
                keyNameValuePairs.keySet().forEach(name -> { if(!optionalColumnsToFetch.get().contains(name))  ColumnToFetch.create(name, false, false).accept(selection); });  
                
            } else {
                selection.all();
            }
            
            
            
            Select select = selection.from(ctx.getTable());
            Select.Where where = null;
            for (Clause whereClause : keyNameValuePairs.keySet().stream().map(name -> eq(name, bindMarker())).collect(Immutables.toSet())) {
                if (where == null) {
                    where = select.where(whereClause);
                } else {
                    where = where.and(whereClause);
                }
            }

            Statement statement = ctx.prepare(select).bind(keyNameValuePairs.values().toArray());
            
            
            return ctx.performAsync(statement)
                      .thenApply(resultSet -> {
                                                  Row row = resultSet.one();
                                                  if (row == null) {
                                                      return Optional.empty();
                                                      
                                                  } else {
                                                      Record record = new Record(ctx, Result.newResult(resultSet), row);
                                                      
                                                      // paranioa check
                                                      keyNameValuePairs.forEach((name, value) -> { 
                                                                                                  ByteBuffer in = DataType.serializeValue(value, ctx.getProtocolVersion());
                                                                                                  ByteBuffer out = record.getBytesUnsafe(name).get();
                                                          
                                                                                                  if (in.compareTo(out) != 0) {
                                                                                                       LOG.warn("Dataswap error for " + name);
                                                                                                       throw new ProtocolErrorException("Dataswap error for " + name); 
                                                                                                  }
                                                                                                 });
                                                      
                                                      if (!resultSet.isExhausted()) {
                                                          throw new TooManyResultsException("more than one record exists");
                                                      }
                                                      
                                                      return Optional.of(record); 
                                                  }
                      });
        }
    }
     
    
   
    
    private static class SingleEntityReadQuery<E> implements SingleRead<Optional<E>> {
        private final Context ctx;
        private final QueryFactory queryFactory;
        private final SingleRead<Optional<Record>> read;
        private final Class<?> clazz;
        
        public SingleEntityReadQuery(Context ctx, QueryFactory queryFactory, SingleRead<Optional<Record>> read, Class<?> clazz) {
            this.ctx = ctx;
            this.queryFactory = queryFactory;
            this.read = read;
            this.clazz = clazz;
        }
        
        
        @Override
        public SingleRead<Optional<E>> withEnableTracking() {
            return queryFactory.newSingleSelection(ctx, read.withEnableTracking(), clazz);
        }
        
        
        @Override
        public SingleRead<Optional<E>> withDisableTracking() {
            return queryFactory.newSingleSelection(ctx, read.withDisableTracking(), clazz);
        }
        
        
        @Override
        public SingleRead<Optional<E>> withConsistency(ConsistencyLevel consistencyLevel) {
            return queryFactory.newSingleSelection(ctx, read.withConsistency(consistencyLevel), clazz);
        }

        
        @Override
        public Optional<E> execute() {
            try {
                return executeAsync().get(Long.MAX_VALUE, TimeUnit.DAYS);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                throw Exceptions.unwrapIfNecessary(e);
            } 
        }
        
        
        @Override
        public CompletableFuture<Optional<E>> executeAsync() {
            return read.executeAsync().thenApply(optionalRecord -> optionalRecord.map(record -> ctx.fromValues(clazz, record.getAccessor())));
        }        
    }
     
   

    
    
    @Override
    public ListReadWithUnit<RecordList> readWhere(Clause... clauses) {
        return newListSelection(getDefaultContext(), 
                                ImmutableSet.copyOf(clauses), 
                                Optional.of(ImmutableSet.of()), 
                                Optional.empty(), 
                                Optional.empty(), 
                                Optional.empty(),
                                Optional.empty());
    }
     
    
    @Override
    public ListReadWithUnit<RecordList> readAll() {
        return newListSelection(getDefaultContext(), 
                                ImmutableSet.of(), 
                                Optional.of(ImmutableSet.of()), 
                                Optional.empty(), 
                                Optional.empty(), 
                                Optional.empty(),
                                Optional.empty());
    }
    
    

    protected ListRead<Count> newCountRead(Context ctx, 
                                                 ImmutableSet<Clause> clauses, 
                                                 Optional<Integer> optionalLimit, 
                                                 Optional<Boolean> optionalAllowFiltering,    
                                                 Optional<Integer> optionalFetchSize,    
                                                 Optional<Boolean> optionalDistinct) {
        return new CountReadQuery(ctx, queryFactory, clauses, optionalLimit, optionalAllowFiltering, optionalFetchSize, optionalDistinct);
    }
    
    
    
    protected ListReadWithUnit<RecordList> newListSelection(Context ctx, 
                                                            ImmutableSet<Clause> clauses, 
                                                            Optional<ImmutableSet<ColumnToFetch>> columnsToFetch, 
                                                            Optional<Integer> optionalLimit, 
                                                            Optional<Boolean> optionalAllowFiltering,
                                                            Optional<Integer> optionalFetchSize,    
                                                            Optional<Boolean> optionalDistinct) {
        return new ListReadQuery(ctx, queryFactory, clauses, columnsToFetch, optionalLimit, optionalAllowFiltering, optionalFetchSize, optionalDistinct);
    }

    
    
    private static class ListReadQuery implements ListReadWithUnit<RecordList> {
        private final Context ctx;
        private final QueryFactory queryFactory;
        private final ImmutableSet<Clause> clauses;
        private final Optional<ImmutableSet<ColumnToFetch>> columnsToFetch;
        private final Optional<Integer> optionalLimit;
        private final Optional<Boolean> optionalAllowFiltering;
        private final Optional<Integer> optionalFetchSize;
        private final Optional<Boolean> optionalDistinct;


        public ListReadQuery(Context ctx,
                             QueryFactory queryFactory,
                             ImmutableSet<Clause> clauses, 
                             Optional<ImmutableSet<ColumnToFetch>> columnsToFetch, 
                             Optional<Integer> optionalLimit, 
                             Optional<Boolean> optionalAllowFiltering,
                             Optional<Integer> optionalFetchSize,
                             Optional<Boolean> optionalDistinct) {
            this.ctx = ctx;
            this.queryFactory = queryFactory;
            this.clauses = clauses;
            this.columnsToFetch = columnsToFetch;
            this.optionalLimit = optionalLimit;
            this.optionalAllowFiltering = optionalAllowFiltering;
            this.optionalFetchSize = optionalFetchSize;
            this.optionalDistinct = optionalDistinct;
        }
        
        
        @Override
        public ListRead<RecordList> all() {
            return queryFactory.newListSelection(ctx, 
                                                 clauses, 
                                                 Optional.empty(),
                                                 optionalLimit, 
                                                 optionalAllowFiltering, 
                                                 optionalFetchSize,
                                                 optionalDistinct);
        }
        
        
        @Override
        public ListRead<RecordList> withEnableTracking() {
            return queryFactory.newListSelection(ctx.withEnableTracking(), 
                                    clauses, 
                                    columnsToFetch,
                                    optionalLimit, 
                                    optionalAllowFiltering, 
                                    optionalFetchSize,
                                    optionalDistinct);
        }
        
        @Override
        public ListRead<RecordList> withDisableTracking() {
            return queryFactory.newListSelection(ctx.withDisableTracking(), 
                                    clauses, 
                                    columnsToFetch, 
                                    optionalLimit, 
                                    optionalAllowFiltering, 
                                    optionalFetchSize,
                                    optionalDistinct);
        }
        
        
        @Override
        public ListRead<RecordList> withConsistency(ConsistencyLevel consistencyLevel) {
            return queryFactory.newListSelection(ctx.withConsistency(consistencyLevel), 
                                    clauses, 
                                    columnsToFetch, 
                                    optionalLimit, 
                                    optionalAllowFiltering, 
                                    optionalFetchSize,
                                    optionalDistinct);
        }
        
        @Override 
        public ListReadWithUnit<RecordList> columns(ImmutableCollection<String> namesToRead) {
            return queryFactory.newListSelection(ctx, 
                                    clauses, 
                                    Immutables.merge(columnsToFetch, ColumnToFetch.create(namesToRead)), 
                                    optionalLimit, 
                                    optionalAllowFiltering, 
                                    optionalFetchSize,
                                    optionalDistinct);
        }
        
        
        
        @Override
        public ListReadWithUnit<RecordList> column(String name) {
            return queryFactory.newListSelection(ctx, 
                                    clauses,  
                                    Immutables.merge(columnsToFetch, ColumnToFetch.create(name, false, false)), 
                                    optionalLimit, 
                                    optionalAllowFiltering,
                                    optionalFetchSize,
                                    optionalDistinct);
        }

        
        @Override
        public ListReadWithColumns<RecordList> columnWithMetadata(String name) {
            return queryFactory.newListSelection(ctx, 
                                    clauses,  
                                    Immutables.merge(columnsToFetch, ColumnToFetch.create(name, true, true)), 
                                    optionalLimit, 
                                    optionalAllowFiltering,
                                    optionalFetchSize,
                                    optionalDistinct);
        }
        

        @Override
        public ListRead<RecordList> withLimit(int limit) {
            return queryFactory.newListSelection(ctx,
                                    clauses, 
                                    columnsToFetch, 
                                    Optional.of(limit), 
                                    optionalAllowFiltering, 
                                    optionalFetchSize,
                                    optionalDistinct);
        }
        
        @Override
        public ListRead<RecordList> withAllowFiltering() {
            return queryFactory.newListSelection(ctx, 
                                    clauses, 
                                    columnsToFetch, 
                                    optionalLimit, 
                                    Optional.of(true), 
                                    optionalFetchSize,
                                    optionalDistinct);
        }

        @Override
        public ListRead<RecordList> withFetchSize(int fetchSize) {
            return queryFactory.newListSelection(ctx, 
                                    clauses, 
                                    columnsToFetch, 
                                    optionalLimit, 
                                    optionalAllowFiltering, 
                                    Optional.of(fetchSize),
                                    optionalDistinct);
        }
        
        @Override
        public ListRead<RecordList> withDistinct() {
            return queryFactory.newListSelection(ctx, 
                                    clauses, 
                                    columnsToFetch, 
                                    optionalLimit, 
                                    optionalAllowFiltering, 
                                    optionalFetchSize,
                                    Optional.of(true));
        }
        
       
        @Override
        public ListRead<Count> count() {
            return queryFactory.newCountRead(ctx,
                                             clauses, 
                                             optionalLimit, 
                                             optionalAllowFiltering, 
                                             optionalFetchSize, 
                                             optionalDistinct);
        }
        
      
        @Override
        public <E> ListRead<EntityList<E>> asEntity(Class<E> objectClass) {
            return queryFactory.newListSelection(ctx, this, objectClass) ;
        }

        
        @Override
        public ListReadWithUnit<RecordList> columns(String... names) {
            return columns(ImmutableSet.copyOf(names));
        }
        
        
                @Override
        public RecordList execute() {
            try {
                return executeAsync().get(Long.MAX_VALUE, TimeUnit.DAYS);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                throw Exceptions.unwrapIfNecessary(e);
            }
        }

   
        @Override
        public CompletableFuture<RecordList> executeAsync() {
   
            Select.Selection selection = select();

            optionalDistinct.ifPresent(distinct -> { if (distinct) selection.distinct(); });

            
            if (columnsToFetch.isPresent()) {
                columnsToFetch.get().forEach(column -> column.accept(selection));
            } else {
                selection.all();
            }
            
            Select select = selection.from(ctx.getTable());
            Select.Where where = null;
            for (Clause clause : clauses) {
                if (where == null) {
                    where = select.where(clause);
                } else {
                    where = where.and(clause);
                }
            }

            optionalLimit.ifPresent(limit -> select.limit(limit));
            optionalAllowFiltering.ifPresent(allowFiltering -> { if (allowFiltering)  select.allowFiltering(); });
            optionalFetchSize.ifPresent(fetchSize -> select.setFetchSize(fetchSize));
            
            return ctx.performAsync(select)
                      .thenApply(resultSet -> RecordList.newRecordList(ctx, resultSet));
        }        
    }  
    
    

    
    protected <E> ListRead<EntityList<E>> newListSelection(Context ctx, ListRead<RecordList> read, Class<?> clazz) {
        return new ListEntityReadQuery<>(ctx, queryFactory, read, clazz);
    }
    
    
    
    private static class ListEntityReadQuery<E> implements ListRead<EntityList<E>> {
        private final Context ctx;
        private final QueryFactory queryFactory;
        private final ListRead<RecordList> read;
        private final Class<?> clazz;
        
        public ListEntityReadQuery(Context ctx, QueryFactory queryFactory, ListRead<RecordList> read, Class<?> clazz) {
            this.ctx = ctx;
            this.queryFactory = queryFactory;
            this.read = read;
            this.clazz = clazz;
        }
    
        @Override
        public ListRead<EntityList<E>> withEnableTracking() {
            return queryFactory.newListSelection(ctx.withEnableTracking(), read, clazz);
        }
        
        @Override
        public ListRead<EntityList<E>> withDisableTracking() {
            return queryFactory.newListSelection(ctx.withDisableTracking(), read, clazz);
        }
        
    
        @Override
        public ListRead<EntityList<E>> withConsistency( ConsistencyLevel consistencyLevel) {
            return queryFactory.newListSelection(ctx.withConsistency(consistencyLevel), read, clazz);
        }
    
        @Override
        public ListRead<EntityList<E>> withDistinct() {
            return queryFactory.newListSelection(ctx, read.withDistinct(), clazz);
        }
        
        @Override
        public ListRead<EntityList<E>> withFetchSize(int fetchSize) {
            return queryFactory.newListSelection(ctx, read.withFetchSize(fetchSize), clazz);
        }
        
        @Override
        public ListRead<EntityList<E>> withAllowFiltering() {
            return queryFactory.newListSelection(ctx, read.withAllowFiltering(), clazz);
        }
        
        @Override
        public ListRead<EntityList<E>> withLimit(int limit) {
            return queryFactory.newListSelection(ctx, read.withLimit(limit), clazz);
        }

        
        @Override
        public EntityList<E> execute() {
            try {
                return executeAsync().get(Long.MAX_VALUE, TimeUnit.DAYS);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                throw Exceptions.unwrapIfNecessary(e);
            }
        }
        
        
        @Override
        public CompletableFuture<EntityList<E>> executeAsync() {
            return read.executeAsync().thenApply(recordIterator -> EntityList.newEntityList(ctx, recordIterator, clazz));
        }
    }
   
    
    
    private static class CountReadQuery implements ListRead<Count> {
        private final Context ctx;
        private final QueryFactory queryFactory;
        private final ImmutableSet<Clause> clauses;
        private final Optional<Integer> optionalLimit;
        private final Optional<Boolean> optionalAllowFiltering;
        private final Optional<Integer> optionalFetchSize;
        private final Optional<Boolean> optionalDistinct;


        public CountReadQuery(Context ctx, 
                              QueryFactory queryFactory,
                              ImmutableSet<Clause> clauses, 
                              Optional<Integer> optionalLimit, 
                              Optional<Boolean> optionalAllowFiltering,
                              Optional<Integer> optionalFetchSize,
                              Optional<Boolean> optionalDistinct) {
            this.ctx = ctx;
            this.queryFactory = queryFactory;
            this.clauses = clauses;
            this.optionalLimit = optionalLimit;
            this.optionalAllowFiltering = optionalAllowFiltering;
            this.optionalFetchSize = optionalFetchSize;
            this.optionalDistinct = optionalDistinct;
        }
        

        @Override
        public ListRead<Count> withEnableTracking() {
            return queryFactory.newCountRead(ctx.withEnableTracking(), 
                                             clauses, 
                                             optionalLimit, 
                                             optionalAllowFiltering, 
                                             optionalFetchSize,
                                             optionalDistinct);
        }
        
        @Override
        public ListRead<Count> withDisableTracking() {
            return queryFactory.newCountRead(ctx.withDisableTracking(), 
                                             clauses, 
                                             optionalLimit, 
                                             optionalAllowFiltering, 
                                             optionalFetchSize,
                                             optionalDistinct);
        }
        
        
        @Override
        public ListRead<Count> withConsistency(ConsistencyLevel consistencyLevel) {
            return queryFactory.newCountRead(ctx.withConsistency(consistencyLevel), 
                                             clauses, 
                                             optionalLimit, 
                                             optionalAllowFiltering, 
                                             optionalFetchSize,
                                             optionalDistinct);
        }
        

        @Override
        public ListRead<Count> withLimit(int limit) {
            return queryFactory.newCountRead(ctx,
                                             clauses, 
                                             Optional.of(limit), 
                                             optionalAllowFiltering, 
                                             optionalFetchSize,
                                             optionalDistinct);
        }
        
        
        @Override
        public ListRead<Count> withAllowFiltering() {
            return queryFactory.newCountRead(ctx, 
                                             clauses, 
                                             optionalLimit, 
                                             Optional.of(true), 
                                             optionalFetchSize,
                                             optionalDistinct);
        }

        @Override
        public ListRead<Count> withFetchSize(int fetchSize) {
            return queryFactory.newCountRead(ctx, 
                                             clauses, 
                                             optionalLimit, 
                                             optionalAllowFiltering, 
                                             Optional.of(fetchSize),
                                             optionalDistinct);
        }
        
        @Override
        public ListRead<Count> withDistinct() {
            return queryFactory.newCountRead(ctx, 
                                             clauses, 
                                             optionalLimit, 
                                             optionalAllowFiltering, 
                                             optionalFetchSize,
                                             Optional.of(true));
        }
        
        
        @Override
        public Count execute() {
            try {
                return executeAsync().get(Long.MAX_VALUE, TimeUnit.DAYS);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                throw Exceptions.unwrapIfNecessary(e);
            }
        }

    
        public CompletableFuture<Count> executeAsync() {
            Select.Selection selection = select();

            optionalDistinct.ifPresent(distinct -> { if (distinct) selection.distinct(); });

     
            selection.countAll();
            
            Select select = selection.from(ctx.getTable());
            Select.Where where = null;
            for (Clause clause : clauses) {
                if (where == null) {
                    where = select.where(clause);
                } else {
                    where = where.and(clause);
                }
            }

            optionalLimit.ifPresent(limit -> select.limit(limit));
            optionalAllowFiltering.ifPresent(allowFiltering -> { if (allowFiltering)  select.allowFiltering(); });
            optionalFetchSize.ifPresent(fetchSize -> select.setFetchSize(fetchSize));
            
            return ctx.performAsync(select)
                      .thenApply(resultSet -> Count.newCountResult(resultSet));
        }        
    }    
}


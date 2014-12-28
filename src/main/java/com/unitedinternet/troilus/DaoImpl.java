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
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

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
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


 

@SuppressWarnings("rawtypes")
public class DaoImpl implements Dao {
    
    private static final Logger LOG = LoggerFactory.getLogger(DaoImpl.class);
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
    
    
    
    ///////////////////////////////
    // Write
    
    
    public Insertion writeEntity(Object entity) {
        return newInsertion(getDefaultContext(), ImmutableList.of()).values(getDefaultContext().toValues(entity));
    }

    private InsertWithValues newInsertion(Context ctx, ImmutableList<? extends ValueToMutate> valuesToMutate) {
        return newInsertion(ctx, valuesToMutate, false);
    }
 
    protected InsertWithValues newInsertion(Context ctx, ImmutableList<? extends ValueToMutate> valuesToMutate, boolean ifNotExists) {
        return new InsertQuery(ctx, valuesToMutate, ifNotExists);
    }
    


    
    private class InsertQuery implements InsertWithValues {
        private final Context ctx;
        private final ImmutableList<? extends ValueToMutate> valuesToMutate;
        private final boolean ifNotExists;

        
        public InsertQuery(Context ctx, ImmutableList<? extends ValueToMutate> valuesToMutate, boolean ifNotExists) {
            this.ctx = ctx;
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
            ImmutableList<? extends ValueToMutate> newValuesToInsert;
            
            if (ctx.isBuildInType(ctx.getColumnMetadata(name).getType())) {
                newValuesToInsert = ImmutableList.<ValueToMutate>builder().addAll(valuesToMutate).add(new BuildinValueToMutate(name, value)).build();
            } else {
                newValuesToInsert = ImmutableList.<ValueToMutate>builder().addAll(valuesToMutate).add(new UDTValueToMutate(name, value)).build();
            }
            
            return newInsertion(ctx, newValuesToInsert, ifNotExists);
        }
           
        
        @Override
        public Insertion withEnableTracking() {
            return newInsertion(ctx.withEnableTracking(), valuesToMutate, ifNotExists);
        }
        
        @Override
        public Insertion withDisableTracking() {
            return newInsertion(ctx.withDisableTracking(), valuesToMutate, ifNotExists);
        }
        
        @Override
        public Insertion withConsistency(ConsistencyLevel consistencyLevel) {
            return newInsertion(ctx.withConsistency(consistencyLevel), valuesToMutate, ifNotExists);
        }
        
        
        @Override
        public Insertion withSerialConsistency(ConsistencyLevel consistencyLevel) {
            return newInsertion(ctx.withSerialConsistency(consistencyLevel), valuesToMutate, ifNotExists);
        }
        
        
        @Override
        public Insertion ifNotExits() {
            return newInsertion(ctx, valuesToMutate, true);
        }
        
        @Override
        public Insertion withTtl(Duration ttl) {
            return newInsertion(ctx.withTtl(ttl), valuesToMutate, ifNotExists);
        }

        @Override
        public Insertion withWritetime(long writetimeMicrosSinceEpoch) {
            return newInsertion(ctx.withWritetime(writetimeMicrosSinceEpoch), valuesToMutate, ifNotExists);
        }
        
        @Override
        public BatchMutation combinedWith(Mutation other) {
            return newBatchMutation(ctx, Type.LOGGED, ImmutableList.of(this, other));
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
                    return new ResultImpl(resultSet);
                });
        }
        
        @Override
        public String toString() {
            return getStatement().toString();
        }
    }

    
    @Override
    public UpdateWithValuesAndCondition writeWhere(Clause... clauses) {
        return newUpdate(getDefaultContext(), ImmutableList.of(), ImmutableList.copyOf(clauses), ImmutableList.of());
    }
    

    private UpdateWithValuesAndCondition newUpdate(Context ctx, ImmutableList<? extends ValueToMutate> valuesToMutate, ImmutableList<Clause> whereConditions, ImmutableList<Clause> ifConditions) {
        return new ConditionBasedUpdateQuery(ctx, valuesToMutate, whereConditions, ifConditions);
    }

    
    
    protected UpdateWithValues newUpdate(Context ctx, ImmutableMap<String, Object> keys, ImmutableList<? extends ValueToMutate> valuesToInsert, ImmutableList<Clause> ifConditions) {
        return new KeyBasedUpdateQuery(ctx, keys, valuesToInsert, ifConditions);
    }

    

    
    private class ConditionBasedUpdateQuery implements UpdateWithValuesAndCondition {
        private final Context ctx;
        private final ImmutableList<? extends ValueToMutate> valuesToMutate;
        private final ImmutableList<Clause> ifConditions;
        private final ImmutableList<Clause> whereConditions;
        
        public ConditionBasedUpdateQuery(Context ctx, ImmutableList<? extends ValueToMutate> valuesToMutate, ImmutableList<Clause> whereConditions, ImmutableList<Clause> ifConditions) {
            this.ctx = ctx;
            this.valuesToMutate = valuesToMutate;
            this.whereConditions = whereConditions;
            this.ifConditions = ifConditions;
        }
        
        
        @Override
        public UpdateWithValuesAndCondition and(Clause where) {
            return newUpdate(ctx, valuesToMutate, whereConditions, Immutables.merge(whereConditions, where));
        }
        
        
        @Override
        public Update onlyIf(Clause... conditions) {
            return newUpdate(ctx, valuesToMutate, whereConditions, ImmutableList.copyOf(conditions)) ;
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
            ImmutableList<? extends ValueToMutate> newValuesToInsert;
            
            if (ctx.isBuildInType(ctx.getColumnMetadata(name).getType())) {
                newValuesToInsert = ImmutableList.<ValueToMutate>builder().addAll(valuesToMutate).add(new BuildinValueToMutate(name, value)).build();
            } else {
                newValuesToInsert = ImmutableList.<ValueToMutate>builder().addAll(valuesToMutate).add(new UDTValueToMutate(name, value)).build();
            }
            
            return newUpdate(ctx, newValuesToInsert, whereConditions, ifConditions);
        }
           
        
        @Override
        public Update withEnableTracking() {
            return newUpdate(ctx.withEnableTracking(), valuesToMutate, whereConditions, ifConditions);
        }
        
        @Override
        public Update withDisableTracking() {
            return newUpdate(ctx.withDisableTracking(), valuesToMutate, whereConditions, ifConditions);
        }
        
        
        @Override
        public Update withConsistency(ConsistencyLevel consistencyLevel) {
            return newUpdate(ctx.withConsistency(consistencyLevel), valuesToMutate, whereConditions, ifConditions);
        }
        
        
        @Override
        public Update withSerialConsistency(ConsistencyLevel consistencyLevel) {
            return newUpdate(ctx.withSerialConsistency(consistencyLevel), valuesToMutate, whereConditions, ifConditions);
        }
        
        
        @Override
        public Update withTtl(Duration ttl) {
            return newUpdate(ctx.withTtl(ttl), valuesToMutate, whereConditions, ifConditions);
        }

        @Override
        public Update withWritetime(long writetimeMicrosSinceEpoch) {
            return newUpdate(ctx.withWritetime(writetimeMicrosSinceEpoch), valuesToMutate, whereConditions, ifConditions);
        }
        
        @Override
        public BatchMutation combinedWith(Mutation other) {
            return newBatchMutation(ctx, Type.LOGGED, ImmutableList.of(this, other));
        }
      
        
        @Override
        public Statement getStatement() {
            
            // statement
            com.datastax.driver.core.querybuilder.Update update = update(ctx.getTable());
            
            valuesToMutate.forEach(valueToInsert -> valueToInsert.addToStatement(ctx, update));
            
            ifConditions.forEach(condition -> update.onlyIf(condition));
   
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
                return new ResultImpl(resultSet);
            });

        }
        
        @Override
        public String toString() {
            return getStatement().toString();
        }
    }


    
    
    private class KeyBasedUpdateQuery implements UpdateWithValues {
        private final Context ctx;
        private final ImmutableList<? extends ValueToMutate> valuesToMutate;
        private final ImmutableMap<String, Object> keys;
        private final ImmutableList<Clause> ifConditions;
        
        public KeyBasedUpdateQuery(Context ctx, ImmutableMap<String, Object> keys, ImmutableList<? extends ValueToMutate> valuesToMutate, ImmutableList<Clause> ifConditions) {
            this.ctx = ctx;
            this.valuesToMutate = valuesToMutate;
            this.keys = keys;
            this.ifConditions = ifConditions;
        }
        
        
        
        @Override
        public Update onlyIf(Clause... conditions) {
            return newUpdate(ctx, keys, valuesToMutate, ImmutableList.copyOf(conditions)) ;
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
            ImmutableList<? extends ValueToMutate> newValuesToInsert;
            
            if (ctx.isBuildInType(ctx.getColumnMetadata(name).getType())) {
                newValuesToInsert = ImmutableList.<ValueToMutate>builder().addAll(valuesToMutate).add(new BuildinValueToMutate(name, value)).build();
            } else {
                newValuesToInsert = ImmutableList.<ValueToMutate>builder().addAll(valuesToMutate).add(new UDTValueToMutate(name, value)).build();
            }
            
            return newUpdate(ctx, keys, newValuesToInsert, ifConditions);
        }
           
        
        @Override
        public Update withEnableTracking() {
            return newUpdate(ctx.withEnableTracking(), keys, valuesToMutate, ifConditions);
        }
        
        @Override
        public Update withDisableTracking() {
            return newUpdate(ctx.withDisableTracking(), keys, valuesToMutate, ifConditions);
        }
        
        
        @Override
        public Update withConsistency(ConsistencyLevel consistencyLevel) {
            return newUpdate(ctx.withConsistency(consistencyLevel), keys, valuesToMutate, ifConditions);
        }
        
        
        @Override
        public Update withSerialConsistency(ConsistencyLevel consistencyLevel) {
            return newUpdate(ctx.withSerialConsistency(consistencyLevel), keys, valuesToMutate, ifConditions);
        }
        
        
        @Override
        public Update withTtl(Duration ttl) {
            return newUpdate(ctx.withTtl(ttl), keys, valuesToMutate, ifConditions);
        }

        @Override
        public Update withWritetime(long writetimeMicrosSinceEpoch) {
            return newUpdate(ctx.withWritetime(writetimeMicrosSinceEpoch), keys, valuesToMutate, ifConditions);
        }
        
        @Override
        public BatchMutation combinedWith(Mutation other) {
            return newBatchMutation(ctx, Type.LOGGED, ImmutableList.of(this, other));
        }
      
        
        @Override
        public Statement getStatement() {
            
            // statement
            com.datastax.driver.core.querybuilder.Update update = update(ctx.getTable());
            
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
                return new ResultImpl(resultSet);
            });

        }
        
        @Override
        public String toString() {
            return getStatement().toString();
        }
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
        return new WriteQuery(ctx, keys, valuesToInsert);
    }

    
    private class WriteQuery implements WriteWithValues {
        private final Context ctx;
        private final ImmutableMap<String, Object> keys;
        private final ImmutableList<? extends ValueToMutate> valuesToMutate;
        
        public WriteQuery(Context ctx, ImmutableMap<String, Object> keys, ImmutableList<? extends ValueToMutate> valuesToMutate) {
            this.ctx = ctx;
            this.keys = keys;
            this.valuesToMutate = valuesToMutate;
        }
        
   
        @Override
        public Update onlyIf(Clause... conditions) {
            return newUpdate(ctx, keys, valuesToMutate, ImmutableList.copyOf(conditions)); 
        }

        @Override
        public Insertion ifNotExits() {
            return newInsertion(ctx, valuesToMutate, false).values(keys).ifNotExits();
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
            
            ImmutableList<? extends ValueToMutate> newValuesToInsert;
            
            if (ctx.isBuildInType(ctx.getColumnMetadata(name).getType())) {
                newValuesToInsert = ImmutableList.<ValueToMutate>builder().addAll(valuesToMutate).add(new BuildinValueToMutate(name, value)).build();
            } else {
                newValuesToInsert = ImmutableList.<ValueToMutate>builder().addAll(valuesToMutate).add(new UDTValueToMutate(name, value)).build();
            }
            
            return newWrite(ctx, keys, newValuesToInsert);
        }

        @Override
        public Write withEnableTracking() {
            return newWrite(ctx.withEnableTracking(), keys, valuesToMutate);
        }
        
        @Override
        public Write withDisableTracking() {
            return newWrite(ctx.withDisableTracking(), keys, valuesToMutate);
        }
        
        
        @Override
        public Write withConsistency(ConsistencyLevel consistencyLevel) {
            return newWrite(ctx.withConsistency(consistencyLevel), keys, valuesToMutate);
        }
        
        
        @Override
        public Write withSerialConsistency(ConsistencyLevel consistencyLevel) {
            return newWrite(ctx.withSerialConsistency(consistencyLevel), keys, valuesToMutate);
        }
        
        
        @Override
        public Write withTtl(Duration ttl) {
            return newWrite(ctx.withTtl(ttl), keys, valuesToMutate);
        }

        @Override
        public Write withWritetime(long writetimeMicrosSinceEpoch) {
            return newWrite(ctx.withWritetime(writetimeMicrosSinceEpoch), keys, valuesToMutate);
        }
        
        @Override
        public BatchMutation combinedWith(Mutation other) {
            return newBatchMutation(ctx, Type.LOGGED, ImmutableList.of(this, other));
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
            return ctx.performAsync(getStatement()).thenApply(resultSet -> new ResultImpl(resultSet));
        }
        
        @Override
        public String toString() {
            return getStatement().toString();
        }
    }

    
    private static class ResultImpl implements Result {
        private final ResultSet rs;
        
        public ResultImpl(ResultSet rs) {
            this.rs = rs;
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


  
    
    private static interface ValueToMutate {
        Object addPreparedToStatement(Context ctx, Insert insert);

        void addToStatement(Context ctx, Insert insert);
        
        Object addPreparedToStatement(Context ctx, com.datastax.driver.core.querybuilder.Update update);
        
        void addToStatement(Context ctx, com.datastax.driver.core.querybuilder.Update update);
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
   
   
    
    
    ///////////////////////////////
    // DELETE
    
    public DeletionWithCondition deleteWhere(Clause clause) {
        return newDeletion(getDefaultContext(), 
                           ImmutableList.of(clause));
    };
    
    
    @Override
    public Deletion deleteWithKey(String keyName, Object keyValue) {
        
        return newDeletion(getDefaultContext(), 
                           ImmutableMap.of(keyName, keyValue));
    }

    @Override
    public Deletion deleteWithKey(String keyName1, Object keyValue1, 
                                  String keyName2, Object keyValue2) {
        
        return newDeletion(getDefaultContext(), 
                           ImmutableMap.of(keyName1, keyValue1,
                                           keyName2, keyValue2));
    }
    
    @Override
    public Deletion deleteWithKey(String keyName1, Object keyValue1, 
                                  String keyName2, Object keyValue2, 
                                  String keyName3, Object keyValue3) {
        
        return newDeletion(getDefaultContext(), 
                           ImmutableMap.of(keyName1, keyValue1, 
                                           keyName2, keyValue2, 
                                           keyName3, keyValue3));
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
                                           keyName4, keyValue4));
    }
    
    protected DeletionWithCondition newDeletion(Context ctx, ImmutableList<Clause> whereConditions) {
        return new ConditionBasedDeleteQuery(ctx, whereConditions);
    }
    
    
    protected Deletion newDeletion(Context ctx, ImmutableMap<String, Object> keyNameValuePairs) {
        return new KeyBasedDeleteQuery(ctx, keyNameValuePairs);
    }
    
    
    private class KeyBasedDeleteQuery implements Deletion {
        private final Context ctx;
        private final ImmutableMap<String, Object> keyNameValuePairs;
        
        public KeyBasedDeleteQuery(Context ctx, ImmutableMap<String, Object> keyNameValuePairs) {
            this.ctx = ctx;
            this.keyNameValuePairs = keyNameValuePairs;
        }
        
        @Override
        public Deletion withEnableTracking() {
            return newDeletion(ctx.withEnableTracking(), keyNameValuePairs);
        }
        
        @Override
        public Deletion withDisableTracking() {
            return newDeletion(ctx.withDisableTracking(), keyNameValuePairs);
        }
        
        
        @Override
        public Deletion withConsistency(ConsistencyLevel consistencyLevel) {
            return newDeletion(ctx.withConsistency(consistencyLevel), keyNameValuePairs);
        }
        
        @Override
        public Deletion withSerialConsistency(ConsistencyLevel consistencyLevel) {
            return newDeletion(ctx.withSerialConsistency(consistencyLevel), keyNameValuePairs);
        }
        
        @Override
        public BatchMutation combinedWith(Mutation other) {
            return newBatchMutation(ctx, Type.LOGGED, ImmutableList.of(this, other));
        }
        
        
        @Override
        public Statement getStatement() {
            Delete delete = delete().from(ctx.getTable());

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
        }
        
        
        public Result execute() {
            try {
                return executeAsync().get(Long.MAX_VALUE, TimeUnit.DAYS);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                throw Exceptions.unwrapIfNecessary(e);
            } 
        }
        
        
        public CompletableFuture<Result> executeAsync() {
            return ctx.performAsync(getStatement()).thenApply(resultSet -> new ResultImpl(resultSet));
        }
    }

    
    
    private class ConditionBasedDeleteQuery implements DeletionWithCondition {
        private final Context ctx;
        private final ImmutableList<Clause> whereConditions;
        
        public ConditionBasedDeleteQuery(Context ctx, ImmutableList<Clause> whereConditions) {
            this.ctx = ctx;
            this.whereConditions = whereConditions;
        }
        
        @Override
        public DeletionWithCondition and(Clause where) {
            return newDeletion(ctx.withEnableTracking(), Immutables.merge(whereConditions, where));
        }
        
        @Override
        public Deletion withEnableTracking() {
            return newDeletion(ctx.withEnableTracking(), whereConditions);
        }
        
        @Override
        public Deletion withDisableTracking() {
            return newDeletion(ctx.withDisableTracking(), whereConditions);
        }
        
        
        @Override
        public Deletion withConsistency(ConsistencyLevel consistencyLevel) {
            return newDeletion(ctx.withConsistency(consistencyLevel), whereConditions);
        }
        
        @Override
        public Deletion withSerialConsistency(ConsistencyLevel consistencyLevel) {
            return newDeletion(ctx.withSerialConsistency(consistencyLevel), whereConditions);
        }
        
        @Override
        public BatchMutation combinedWith(Mutation other) {
            return newBatchMutation(ctx, Type.LOGGED, ImmutableList.of(this, other));
        }
        
        
        @Override
        public Statement getStatement() {
            Delete delete = delete().from(ctx.getTable());

            Delete.Where where = null;
            
            for (Clause whereClause : whereConditions) {
                if (where == null) {
                    where = delete.where(whereClause);
                } else {
                    where = where.and(whereClause);
                }
            }
           
            return delete;
        }
        
        
        public Result execute() {
            try {
                return executeAsync().get(Long.MAX_VALUE, TimeUnit.DAYS);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                throw Exceptions.unwrapIfNecessary(e);
            } 
        }
        
        
        public CompletableFuture<Result> executeAsync() {
            return ctx.performAsync(getStatement()).thenApply(resultSet -> new ResultImpl(resultSet));
        }
    }

  

    
    
    protected BatchMutation newBatchMutation(Context ctx, Type type, ImmutableList<Mutation<?>> mutations) {
        return new MutationBatchQuery(ctx, type, mutations);
    }
    
     
    
    private final class MutationBatchQuery implements BatchMutation {
        private final Context ctx;
        private final ImmutableList<Mutation<?>> mutations;
        private final Type type;  
        
        public MutationBatchQuery(Context ctx, Type type, ImmutableList<Mutation<?>> mutations) {
            this.ctx = ctx;
            this.type = type;
            this.mutations = mutations;
        }
                
        @Override
        public BatchMutation withEnableTracking() {
            return newBatchMutation(ctx.withEnableTracking(), type, mutations);
        }
        
        @Override
        public BatchMutation withDisableTracking() {
            return newBatchMutation(ctx.withDisableTracking(), type, mutations);
        }
        
        
        @Override
        public Query<Result> withLockedBatchType() {
            return newBatchMutation(ctx, Type.LOGGED, mutations);
        }
        
        @Override
        public Query<Result> withUnlockedBatchType() {
            return newBatchMutation(ctx, Type.UNLOGGED, mutations);
        }
        
         
        @Override
        public BatchMutation combinedWith(Mutation<?> other) {
            return newBatchMutation(ctx, type, Immutables.merge(mutations, other));
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
            return ctx.performAsync(getStatement()).thenApply(resultSet -> new ResultImpl(resultSet));
        }
    }
    
    
    
    
    

    
    ///////////////////////////////
    // READ
    

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
        return new SingleReadQuery(ctx, keyNameValuePairs, optionalColumnsToFetch);
    }
    

    private class SingleReadQuery implements SingleReadWithUnit<Optional<Record>> {
        private final Context ctx;
        private final ImmutableMap<String, Object> keyNameValuePairs;
        private final Optional<ImmutableSet<ColumnToFetch>> optionalColumnsToFetch;
         
        
        public SingleReadQuery(Context ctx, ImmutableMap<String, Object> keyNameValuePairs, Optional<ImmutableSet<ColumnToFetch>> optionalColumnsToFetch) {
            this.ctx = ctx;
            this.keyNameValuePairs = keyNameValuePairs;
            this.optionalColumnsToFetch = optionalColumnsToFetch;
        }
         
        
        @Override
        public SingleRead<Optional<Record>> all() {
            return this;
        }
        
        @Override
        public <E> SingleRead<Optional<E>> asEntity(Class<E> objectClass) {
            return newSingleSelection(ctx, this, objectClass);
        }
        
        @Override
        public SingleReadWithUnit<Optional<Record>> column(String name) {
            return newSingleSelection(ctx, keyNameValuePairs, Immutables.merge(optionalColumnsToFetch, ColumnToFetch.create(name, false, false)));
        }

        @Override
        public SingleReadWithColumns<Optional<Record>> columnWithMetadata(String name) {
            return newSingleSelection(ctx, keyNameValuePairs, Immutables.merge(optionalColumnsToFetch, ColumnToFetch.create(name, true, true)));
        }
        
        @Override
        public SingleReadWithUnit<Optional<Record>> columns(String... names) {
            return columns(ImmutableSet.copyOf(names));
        }
        
        @Override 
        public SingleReadWithUnit<Optional<Record>> columns(ImmutableCollection<String> namesToRead) {
            return newSingleSelection(ctx, keyNameValuePairs, Immutables.merge(optionalColumnsToFetch, ColumnToFetch.create(namesToRead)));
        }
        
        @Override
        public SingleRead<Optional<Record>> withEnableTracking() {
            return newSingleSelection(ctx.withEnableTracking(), keyNameValuePairs, optionalColumnsToFetch);
        }
        
        @Override
        public SingleRead<Optional<Record>> withDisableTracking() {
            return newSingleSelection(ctx.withDisableTracking(), keyNameValuePairs, optionalColumnsToFetch);
        }
        
        @Override
        public SingleRead<Optional<Record>> withConsistency(ConsistencyLevel consistencyLevel) {
            return newSingleSelection(ctx.withConsistency(consistencyLevel), keyNameValuePairs, optionalColumnsToFetch);
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
                                                      Record record = new Record(ctx, new ResultImpl(resultSet), row);
                                                      
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
     
    
    
    protected <E> SingleRead<Optional<E>> newSingleSelection(Context ctx, SingleRead<Optional<Record>> read, Class<?> clazz) {
        return new SingleEntityReadQuery<E>(ctx, read, clazz);
    }

    
    
    private class SingleEntityReadQuery<E> implements SingleRead<Optional<E>> {
        private final Context ctx;
        private final SingleRead<Optional<Record>> read;
        private final Class<?> clazz;
        
        public SingleEntityReadQuery(Context ctx, SingleRead<Optional<Record>> read, Class<?> clazz) {
            this.ctx = ctx;
            this.read = read;
            this.clazz = clazz;
        }
        
        
        @Override
        public SingleRead<Optional<E>> withEnableTracking() {
            return newSingleSelection(ctx, read.withEnableTracking(), clazz);
        }
        
        
        @Override
        public SingleRead<Optional<E>> withDisableTracking() {
            return newSingleSelection(ctx, read.withDisableTracking(), clazz);
        }
        
        
        @Override
        public SingleRead<Optional<E>> withConsistency(ConsistencyLevel consistencyLevel) {
            return newSingleSelection(ctx, read.withConsistency(consistencyLevel), clazz);
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
            return read.executeAsync().thenApply(optionalRecord -> optionalRecord.map(record -> ctx.fromValues(clazz, new RecordAccessorAdapter(record))));
        }        
    }
     
   

    
    private static class ColumnToFetch implements Consumer<Select.Selection> {
        private final String name;
        private final boolean isFetchWritetime;
        private final boolean isFetchTtl;
        
        private ColumnToFetch(String name, boolean isFetchWritetime, boolean isFetchTtl) {
            this.name = name;
            this.isFetchWritetime = isFetchWritetime;
            this.isFetchTtl = isFetchTtl;
        }
        
        public static ColumnToFetch create(String name, boolean isFetchWritetime, boolean isFetchTtl) {
            return new ColumnToFetch(name, isFetchWritetime, isFetchTtl);
        }
        
        public static ImmutableSet<ColumnToFetch> create(ImmutableCollection<String> names) {
            return names.stream().map(name -> new ColumnToFetch(name, false, false)).collect(Immutables.toSet());
        }

        @Override
        public void accept(Select.Selection selection) {
             selection.column(name);

             if (isFetchTtl) {
                 selection.ttl(name);
             }

             if (isFetchWritetime) {
                 selection.writeTime(name);
             }
        }
    }
    
    
    
    @Override
    public ListReadWithUnit<ListResult<Record>> readWhere(Clause... clauses) {
        return newListSelection(getDefaultContext(), 
                                ImmutableSet.copyOf(clauses), 
                                Optional.of(ImmutableSet.of()), 
                                Optional.empty(), 
                                Optional.empty(), 
                                Optional.empty(),
                                Optional.empty());
    }
     
    
    @Override
    public ListReadWithUnit<ListResult<Record>> readAll() {
        return newListSelection(getDefaultContext(), 
                                ImmutableSet.of(), 
                                Optional.of(ImmutableSet.of()), 
                                Optional.empty(), 
                                Optional.empty(), 
                                Optional.empty(),
                                Optional.empty());
    }
    
    protected ListReadWithUnit<ListResult<Record>> newListSelection(Context ctx, 
                                                                     ImmutableSet<Clause> clauses, 
                                                                     Optional<ImmutableSet<ColumnToFetch>> columnsToFetch, 
                                                                     Optional<Integer> optionalLimit, 
                                                                     Optional<Boolean> optionalAllowFiltering,
                                                                     Optional<Integer> optionalFetchSize,    
                                                                     Optional<Boolean> optionalDistinct) {
        return new ListReadQuery(ctx, clauses, columnsToFetch, optionalLimit, optionalAllowFiltering, optionalFetchSize, optionalDistinct);
    }

    
    
    private class ListReadQuery implements ListReadWithUnit<ListResult<Record>> {
        private final Context ctx;
        private final ImmutableSet<Clause> clauses;
        private final Optional<ImmutableSet<ColumnToFetch>> columnsToFetch;
        private final Optional<Integer> optionalLimit;
        private final Optional<Boolean> optionalAllowFiltering;
        private final Optional<Integer> optionalFetchSize;
        private final Optional<Boolean> optionalDistinct;


        public ListReadQuery(Context ctx, 
                                  ImmutableSet<Clause> clauses, 
                                  Optional<ImmutableSet<ColumnToFetch>> columnsToFetch, 
                                  Optional<Integer> optionalLimit, 
                                  Optional<Boolean> optionalAllowFiltering,
                                  Optional<Integer> optionalFetchSize,
                                  Optional<Boolean> optionalDistinct) {
            this.ctx = ctx;
            this.clauses = clauses;
            this.columnsToFetch = columnsToFetch;
            this.optionalLimit = optionalLimit;
            this.optionalAllowFiltering = optionalAllowFiltering;
            this.optionalFetchSize = optionalFetchSize;
            this.optionalDistinct = optionalDistinct;
        }
        
        
        @Override
        public ListRead<ListResult<Record>> all() {
            return this;
        }
        
        
        @Override
        public SingleRead<ListResult<Record>> withEnableTracking() {
            return newListSelection(ctx.withEnableTracking(), 
                                    clauses, 
                                    columnsToFetch,
                                    optionalLimit, 
                                    optionalAllowFiltering, 
                                    optionalFetchSize,
                                    optionalDistinct);
        }
        
        @Override
        public SingleRead<ListResult<Record>> withDisableTracking() {
            return newListSelection(ctx.withDisableTracking(), 
                                    clauses, 
                                    columnsToFetch, 
                                    optionalLimit, 
                                    optionalAllowFiltering, 
                                    optionalFetchSize,
                                    optionalDistinct);
        }
        
        
        @Override
        public ListRead<ListResult<Record>> withConsistency(ConsistencyLevel consistencyLevel) {
            return newListSelection(ctx.withConsistency(consistencyLevel), 
                                    clauses, 
                                    columnsToFetch, 
                                    optionalLimit, 
                                    optionalAllowFiltering, 
                                    optionalFetchSize,
                                    optionalDistinct);
        }
        
        @Override 
        public ListReadWithUnit<ListResult<Record>> columns(ImmutableCollection<String> namesToRead) {
            return newListSelection(ctx, 
                                    clauses, 
                                    Immutables.merge(columnsToFetch, ColumnToFetch.create(namesToRead)), 
                                    optionalLimit, 
                                    optionalAllowFiltering, 
                                    optionalFetchSize,
                                    optionalDistinct);
        }
        
        
        
        @Override
        public ListReadWithUnit<ListResult<Record>> column(String name) {
            return newListSelection(ctx, 
                                    clauses,  
                                    Immutables.merge(columnsToFetch, ColumnToFetch.create(name, false, false)), 
                                    optionalLimit, 
                                    optionalAllowFiltering,
                                    optionalFetchSize,
                                    optionalDistinct);
        }

        
        @Override
        public ListReadWithColumns<ListResult<Record>> columnWithMetadata(String name) {
            return newListSelection(ctx, 
                                    clauses,  
                                    Immutables.merge(columnsToFetch, ColumnToFetch.create(name, true, true)), 
                                    optionalLimit, 
                                    optionalAllowFiltering,
                                    optionalFetchSize,
                                    optionalDistinct);
        }
        

        @Override
        public ListRead<ListResult<Record>> withLimit(int limit) {
            return newListSelection(ctx,
                                    clauses, 
                                    columnsToFetch, 
                                    Optional.of(limit), 
                                    optionalAllowFiltering, 
                                    optionalFetchSize,
                                    optionalDistinct);
        }
        
        @Override
        public ListRead<ListResult<Record>> withAllowFiltering() {
            return newListSelection(ctx, 
                                    clauses, 
                                    columnsToFetch, 
                                    optionalLimit, 
                                    Optional.of(true), 
                                    optionalFetchSize,
                                    optionalDistinct);
        }

        @Override
        public ListRead<ListResult<Record>> withFetchSize(int fetchSize) {
            return newListSelection(ctx, 
                                    clauses, 
                                    columnsToFetch, 
                                    optionalLimit, 
                                    optionalAllowFiltering, 
                                    Optional.of(fetchSize),
                                    optionalDistinct);
        }
        
        @Override
        public ListRead<ListResult<Record>> withDistinct() {
            return newListSelection(ctx, 
                                    clauses, 
                                    columnsToFetch, 
                                    optionalLimit, 
                                    optionalAllowFiltering, 
                                    optionalFetchSize,
                                    Optional.of(true));
        }
        
       
      
        @Override
        public <E> ListRead<ListResult<E>> asEntity(Class<E> objectClass) {
            return newListSelection(ctx, this, objectClass) ;
        }

        
        @Override
        public ListReadWithUnit<ListResult<Record>> columns(String... names) {
            return columns(ImmutableSet.copyOf(names));
        }
        
        
                @Override
        public ListResult<Record> execute() {
            try {
                return executeAsync().get(Long.MAX_VALUE, TimeUnit.DAYS);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                throw Exceptions.unwrapIfNecessary(e);
            }
        }

   
        @Override
        public CompletableFuture<ListResult<Record>> executeAsync() {
   
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
                      .thenApply(resultSet -> new RecordsImpl(resultSet));
        }        
        
        
        private final class RecordsImpl extends ResultImpl implements ListResult<Record> {
            private final Iterator<Row> iterator;
            private final ResultSet rs;
            private final AtomicReference<DatabaseSubscription> subscriptionRef = new AtomicReference<>();
            
            public RecordsImpl(ResultSet rs) {
                super(rs);
                this.rs = rs;
                this.iterator = rs.iterator();
            }
       
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }
            
            @Override
            public Record next() {
                return new Record(ctx, this, iterator.next());
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
    
    
    
    protected <E> ListRead<ListResult<E>> newListSelection(Context ctx, ListRead<ListResult<Record>> read, Class<?> clazz) {
        return new ListEntityReadQuery<>(ctx, read, clazz);
    }
    
    
    private class ListEntityReadQuery<E> implements ListRead<ListResult<E>> {
        private final Context ctx;
        private final ListRead<ListResult<Record>> read;
        private final Class<?> clazz;
        
        public ListEntityReadQuery(Context ctx, ListRead<ListResult<Record>> read, Class<?> clazz) {
            this.ctx = ctx;
            this.read = read;
            this.clazz = clazz;
        }
    
        @Override
        public SingleRead<ListResult<E>> withEnableTracking() {
            return newListSelection(ctx.withEnableTracking(), read, clazz);
        }
        
        @Override
        public SingleRead<ListResult<E>> withDisableTracking() {
            return newListSelection(ctx.withDisableTracking(), read, clazz);
        }
        
    
        @Override
        public SingleRead<ListResult<E>> withConsistency( ConsistencyLevel consistencyLevel) {
            return newListSelection(ctx.withConsistency(consistencyLevel), read, clazz);
        }
    
        @Override
        public ListRead<ListResult<E>> withDistinct() {
            return newListSelection(ctx, read.withDistinct(), clazz);
        }
        
        @Override
        public ListRead<ListResult<E>> withFetchSize(int fetchSize) {
            return newListSelection(ctx, read.withFetchSize(fetchSize), clazz);
        }
        
        @Override
        public ListRead<ListResult<E>> withAllowFiltering() {
            return newListSelection(ctx, read.withAllowFiltering(), clazz);
        }
        
        @Override
        public ListRead<ListResult<E>> withLimit(int limit) {
            return newListSelection(ctx, read.withLimit(limit), clazz);
        }

        
        @Override
        public ListResult<E> execute() {
            try {
                return executeAsync().get(Long.MAX_VALUE, TimeUnit.DAYS);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                throw Exceptions.unwrapIfNecessary(e);
            }
        }
        
        
        @Override
        public CompletableFuture<ListResult<E>> executeAsync() {
            return read.executeAsync().thenApply(recordIterator -> new ResultIteratorImpl<>(ctx, recordIterator, clazz));
        }
        
        
        
        private final class ResultIteratorImpl<F>implements ListResult<F> {
            private final Context ctx;
            private final ListResult<Record> recordIterator;
            private final Class<?> clazz;

            
            public ResultIteratorImpl(Context ctx, ListResult<Record> recordIterator, Class<?> clazz) {
                this.ctx = ctx;
                this.recordIterator = recordIterator;
                this.clazz = clazz;
            }

            @Override
            public ExecutionInfo getExecutionInfo() {
                return recordIterator.getExecutionInfo();
            }
            
            @Override
            public ImmutableList<ExecutionInfo> getAllExecutionInfo() {
                return recordIterator.getAllExecutionInfo();
            }
            
            
            @Override
            public boolean hasNext() {
                return recordIterator.hasNext();
            }
        
            
            @Override
            public F next() {
                return ctx.fromValues(clazz, new RecordAccessorAdapter(recordIterator.next()));
            }
            
          
            @Override
            public void subscribe(Subscriber<? super F> subscriber) {
                recordIterator.subscribe(new MappingSubscriber<F>(ctx, clazz, subscriber));
            }
            
            private final class MappingSubscriber<G> implements Subscriber<Record> {
                private final Context ctx;
                private final Class<?> clazz;
                
                private Subscriber<? super G> subscriber;
                
                public MappingSubscriber(Context ctx, Class<?> clazz, Subscriber<? super G> subscriber) {
                    this.ctx = ctx;
                    this.clazz = clazz;
                    this.subscriber = subscriber;
                }
                
                @Override
                public void onSubscribe(Subscription subscription) {
                    subscriber.onSubscribe(subscription);
                }
                
                @Override
                public void onNext(Record record) {
                    subscriber.onNext(ctx.fromValues(clazz, new RecordAccessorAdapter(record)));
                }

                @Override
                public void onError(Throwable t) {
                    subscriber.onError(t);
                }
                
                @Override
                public void onComplete() {
                    subscriber.onComplete();
                }
            }
        }
    }
    
    
    
    private static final class RecordAccessorAdapter implements TriFunction<String, Class<?>, Class<?>, Optional<?>> {
        
        private final Record record;
        
        public RecordAccessorAdapter(Record record) {
            this.record = record;
        }
        
        
        @Override
        public Optional<?> apply(String name, Class<?> clazz1, Class<?> clazz2) {
            return record.getObject(name, clazz1);
        }
    }
}


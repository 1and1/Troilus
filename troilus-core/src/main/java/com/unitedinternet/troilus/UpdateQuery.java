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



import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.unitedinternet.troilus.Dao.CounterBatchMutation;
import com.unitedinternet.troilus.Dao.CounterBatchable;
import com.unitedinternet.troilus.Dao.Insertion;
import com.unitedinternet.troilus.Dao.Update;
import com.unitedinternet.troilus.Dao.UpdateWithValuesAndCounter;
import com.unitedinternet.troilus.Dao.Write;
import com.unitedinternet.troilus.Dao.WriteWithCounter;
import com.unitedinternet.troilus.Dao.CounterMutation;
import com.unitedinternet.troilus.interceptor.InsertQueryData;
import com.unitedinternet.troilus.interceptor.UpdateQueryData;
import com.unitedinternet.troilus.interceptor.UpdateQueryPreInterceptor;


 
class UpdateQuery extends MutationQuery<WriteWithCounter> implements WriteWithCounter, UpdateWithValuesAndCounter  {

    private final UpdateQueryData data;
    
    
    public UpdateQuery(Context ctx, UpdateQueryData data) {
        super(ctx);
        this.data = data;
    }
    
    @Override
    protected UpdateQuery newQuery(Context newContext) {
        return new UpdateQuery(newContext, data);
    }
    
    
    public InsertionQuery entity(Object entity) {
        return new InsertionQuery(getContext(), 
                                 new InsertQueryData().withValuesToMutate(getContext().toValues(entity)));
    }
        
    
    @Override
    public UpdateQuery value(String name, Object value) {
        return new UpdateQuery(getContext(), 
                               data.withValuesToMutate(Immutables.merge(data.getValuesToMutate(), name, toOptional(value))));
    }

    
    @Override
    public UpdateQuery values(ImmutableMap<String, Object> nameValuePairsToAdd) {
        return new UpdateQuery(getContext(), 
                              data.withValuesToMutate(Immutables.merge(data.getValuesToMutate(), Immutables.transform(nameValuePairsToAdd, name -> name, value -> toOptional(value)))));
    }
    
    

    @Override
    public UpdateQuery removeSetValue(String name, Object value) {
        ImmutableSet<Object> values = data.getSetValuesToRemove().get(name);
        values = (values == null) ? ImmutableSet.of(value) : Immutables.merge(values, value);

        return new UpdateQuery(getContext(), 
                               data.withSetValuesToRemove(Immutables.merge(data.getSetValuesToRemove(), name, values)));
    }

    
    @Override
    public UpdateQuery addSetValue(String name, Object value) {
        ImmutableSet<Object> values = data.getSetValuesToAdd().get(name);
        values = (values == null) ? ImmutableSet.of(value): Immutables.merge(values, value);

        return new UpdateQuery(getContext(), 
                               data.withSetValuesToAdd(Immutables.merge(data.getSetValuesToAdd(), name, values)));
    }
   
    
    @Override
    public Write prependListValue(String name, Object value) {
        ImmutableList<Object> values = data.getListValuesToPrepend().get(name);
        values = (values == null) ? ImmutableList.of(value) : Immutables.merge(values, value);

        return new UpdateQuery(getContext(), 
                               data.withListValuesToPrepend(Immutables.merge(data.getListValuesToPrepend(), name, values)));
    } 
    
    

    @Override
    public Write appendListValue(String name, Object value) {
        ImmutableList<Object> values = data.getListValuesToAppend().get(name);
        values = (values == null) ? ImmutableList.of(value) : Immutables.merge(values, value);

        return new UpdateQuery(getContext(), 
                               data.withListValuesToAppend(Immutables.merge(data.getListValuesToAppend(), name, values)));
    }
    
    
    
    @Override
    public Write removeListValue(String name, Object value) {
        ImmutableList<Object> values = data.getListValuesToRemove().get(name);
        values = (values == null) ? ImmutableList.of(value) : Immutables.merge(values, value);

        return new UpdateQuery(getContext(), 
                               data.withListValuesToRemove(Immutables.merge(data.getListValuesToRemove(), name, values)));
    }
   
    
    @Override
    public Write putMapValue(String name, Object key, Object value) {
        ImmutableMap<Object, Optional<Object>> values = data.getMapValuesToMutate().get(name);
        values = (values == null) ? ImmutableMap.of(key, toOptional(value)) : Immutables.merge(values, key, toOptional(value));

        return new UpdateQuery(getContext(), 
                               data.withMapValuesToMutate(Immutables.merge(data.getMapValuesToMutate(), name, values)));
    }
    
    

    @Override
    public Update<Write> onlyIf(Clause... conditions) {
        return new UpdateQuery(getContext(), 
                               data.withOnlyIfConditions(ImmutableList.<Clause>builder().addAll(data.getOnlyIfConditions()).addAll(ImmutableList.copyOf(conditions)).build()));
    }


    @Override
    public Insertion ifNotExits() {
        return new InsertionQuery(getContext(), new InsertQueryData().withValuesToMutate(Immutables.merge(data.getValuesToMutate(), Immutables.transform(data.getKeys(), name -> name, value -> toOptional(value))))
                                                                     .withIfNotExits(true));
    }

        
    @Override
    public CounterMutationQuery incr(String name) {
        return incr(name, 1);
    }
    
    @Override
    public CounterMutationQuery incr(String name, long value) {
        return new CounterMutationQuery(getContext(), 
                                        new CounterMutationQueryData(data.getKeys(), data.getWhereConditions(), name, value));  
    }
    
    
    @Override
    public CounterMutationQuery decr(String name) {
        return decr(name, 1);
    }
    
    @Override
    public CounterMutationQuery decr(String name, long value) {
        return new CounterMutationQuery(getContext(), 
                                        new CounterMutationQueryData(data.getKeys(), data.getWhereConditions(), name, 0 - value));  
    }
    
    

    private Statement toStatement(UpdateQueryData queryData) {
        com.datastax.driver.core.querybuilder.Update update = update(getContext().getTable());
        
        queryData.getOnlyIfConditions().forEach(condition -> update.onlyIf(condition));

        
        // key-based update
        if (queryData.getWhereConditions().isEmpty()) {
            List<Object> values = Lists.newArrayList();
            
            queryData.getValuesToMutate().forEach((name, optionalValue) -> { update.with(set(name, bindMarker())); values.add(toStatementValue(name, optionalValue.orElse(null))); });

            queryData.getSetValuesToAdd().forEach((name, vals) -> { update.with(addAll(name, bindMarker())); values.add(toStatementValue(name, vals)); });
            queryData.getSetValuesToRemove().forEach((name, vals) -> { update.with(removeAll(name, bindMarker())); values.add(toStatementValue(name, vals)); });
            
            queryData.getListValuesToPrepend().forEach((name, vals) -> { update.with(prependAll(name, bindMarker())); values.add(toStatementValue(name, vals)); });
            queryData.getListValuesToAppend().forEach((name, vals) -> { update.with(appendAll(name, bindMarker())); values.add(toStatementValue(name, vals)); });
            queryData.getListValuesToRemove().forEach((name, vals) -> { update.with(discardAll(name, bindMarker())); values.add(toStatementValue(name, vals)); });

            queryData.getMapValuesToMutate().forEach((name, map) -> { update.with(putAll(name, bindMarker())); values.add(toStatementValue(name, map)); });
            
            
            queryData.getKeys().keySet().forEach(keyname -> { update.where(eq(keyname, bindMarker())); values.add(queryData.getKeys().get(keyname)); } );
            
            queryData.getOnlyIfConditions().forEach(condition -> update.onlyIf(condition));
            getTtl().ifPresent(ttl-> { update.using(QueryBuilder.ttl(bindMarker())); values.add((int) ttl.getSeconds()); });
            
            return prepare(update).bind(values.toArray());

            
        // where condition-based update
        } else {
            queryData.getValuesToMutate().forEach((name, optionalValue) -> update.with(set(name, toStatementValue(name, optionalValue.orElse(null)))));
        
            queryData.getSetValuesToAdd().forEach((name, vals) -> update.with(addAll(name, toStatementValue(name, vals))));
            queryData.getSetValuesToRemove().forEach((name, vals) -> update.with(removeAll(name, toStatementValue(name, vals))));

            queryData.getListValuesToPrepend().forEach((name, vals) -> update.with(prependAll(name, toStatementValue(name, vals))));
            queryData.getListValuesToAppend().forEach((name, vals) -> update.with(appendAll(name, toStatementValue(name, vals))));
            queryData.getListValuesToRemove().forEach((name, vals) -> update.with(discardAll(name, toStatementValue(name, vals))));
            
            queryData.getMapValuesToMutate().forEach((name, map) -> update.with(putAll(name, toStatementValue(name, map))));

            getTtl().ifPresent(ttl-> update.using(QueryBuilder.ttl((int) ttl.getSeconds())));
            queryData.getWhereConditions().forEach(whereClause -> update.where(whereClause));
            
            return update;
        }
    }
    
    @Override
    protected Statement getStatement() {
        UpdateQueryData queryData = data;
        for (UpdateQueryPreInterceptor interceptor : getContext().getInterceptors(UpdateQueryPreInterceptor.class)) {
            queryData = interceptor.onPreUpdate(queryData);
        }
        
        return toStatement(queryData);
    }
    
    
    @Override
    public CompletableFuture<Result> executeAsync() {
        return super.executeAsync()
                    .thenApply(result ->  {
                                            // check cas result column '[applied]'
                                            if (!data.getOnlyIfConditions().isEmpty() && !result.wasApplied()) {
                                                throw new IfConditionException("if condition does not match");  
                                            } 
                                            return result;
                                          });
    }
    
    
    
    private boolean isOptional(Object obj) {
        if (obj == null) {
            return false;
        } else {
            return (Optional.class.isAssignableFrom(obj.getClass()));
        }
    }
 

    @SuppressWarnings("unchecked")
    private <T> Optional<T> toOptional(T obj) {
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
 

    
    
    
    

        
    private static class CounterMutationQueryData {
        
        private final ImmutableMap<String, Object> keys;
        private final ImmutableList<Clause> whereConditions;

        private final String name;
        private final long diff;

        

        public CounterMutationQueryData(ImmutableMap<String, Object> keys,
                                        ImmutableList<Clause> whereConditions,
                                        String name,
                                        long diff) {
            this.keys = keys;
            this.whereConditions = whereConditions;
            this.name = name; 
            this.diff = diff;
        }
       
        
        public ImmutableMap<String, Object> getKeys() {
            return keys;
        }

        public ImmutableList<Clause> getWhereConditions() {
            return whereConditions;
        }

        public String getName() {
            return name;
        }

        public long getDiff() {
            return diff;
        }
    }
        
    
    private static final class CounterMutationQuery extends MutationQuery<CounterMutation> implements CounterMutation {
        
        private final CounterMutationQueryData data;

        public CounterMutationQuery(Context ctx, CounterMutationQueryData data) {
            super(ctx);
            this.data = data;
        }
        
        @Override
        protected CounterMutation newQuery(Context newContext) {
            return new CounterMutationQuery(newContext, data);
        }
   
        @Override
        public CounterBatchMutation combinedWith(CounterBatchable other) {
            return new CounterBatchMutationQuery(getContext(), ImmutableList.of(this, other));
        }
   
        
        private Statement toStatement(CounterMutationQueryData queryData) {
            com.datastax.driver.core.querybuilder.Update update = update(getContext().getTable());
            
            // key-based update
            if (queryData.getWhereConditions().isEmpty()) {
                List<Object> values = Lists.newArrayList();
                
                if (queryData.getDiff() > 0) {
                    update.with(QueryBuilder.incr(queryData.getName(), bindMarker()));
                    values.add(queryData.getDiff());
                    
                } else {
                    update.with(QueryBuilder.decr(queryData.getName(), bindMarker()));
                    values.add(0 - queryData.getDiff());
                }
         
                queryData.getKeys().keySet().forEach(keyname -> { update.where(eq(keyname, bindMarker())); values.add(queryData.getKeys().get(keyname)); } );
                
                getTtl().ifPresent(ttl-> { update.using(QueryBuilder.ttl(bindMarker())); values.add((int) ttl.getSeconds()); });
                
                return prepare(update).bind(values.toArray());

                
            // where condition-based update
            } else {
                
                if (queryData.getDiff() > 0) {
                    update.with(QueryBuilder.incr(queryData.getName(), queryData.getDiff()));
                    
                } else {
                    update.with(QueryBuilder.decr(queryData.getName(), 0 - queryData.getDiff()));
                }
                                
                getTtl().ifPresent(ttl-> update.using(QueryBuilder.ttl((int) ttl.getSeconds())));
                
                queryData.getWhereConditions().forEach(whereClause -> update.where(whereClause));
                
                return update;
            }
        }
        
        @Override 
        protected Statement getStatement() {
            return toStatement(data);
        }
    }
}


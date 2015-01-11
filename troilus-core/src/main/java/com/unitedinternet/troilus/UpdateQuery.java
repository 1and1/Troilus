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
import java.util.Map;
import java.util.Optional;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;

import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.unitedinternet.troilus.Dao.CounterBatchMutation;
import com.unitedinternet.troilus.Dao.CounterBatchable;
import com.unitedinternet.troilus.Dao.Insertion;
import com.unitedinternet.troilus.Dao.Update;
import com.unitedinternet.troilus.Dao.UpdateWithValuesAndCounter;
import com.unitedinternet.troilus.Dao.Write;
import com.unitedinternet.troilus.Dao.WriteWithCounter;
import com.unitedinternet.troilus.Dao.CounterMutation;
import com.unitedinternet.troilus.interceptor.WriteQueryData;
import com.unitedinternet.troilus.interceptor.WriteQueryPreInterceptor;


 
class UpdateQuery extends MutationQuery<WriteWithCounter> implements WriteWithCounter, UpdateWithValuesAndCounter  {

    private final WriteQueryData data;
    
    
    public UpdateQuery(Context ctx, WriteQueryData data) {
        super(ctx);
        this.data = data;
    }
    
    @Override
    protected UpdateQuery newQuery(Context newContext) {
        return new UpdateQuery(newContext, data);
    }
    
    
    public InsertionQuery entity(Object entity) {
        return new InsertionQuery(getContext(), 
                                  new WriteQueryData().valuesToMutate(mapOptional(getContext().getBeanMapper().toValues(entity))));
    }
    
    private ImmutableMap<String, Optional<Object>> mapOptional(ImmutableMap<String, com.google.common.base.Optional<Object>> m) {
        return Immutables.transform(m, name -> name, guavaOptional -> Optional.ofNullable(guavaOptional.orNull())); 
    }
    
    
    @Override
    public UpdateQuery value(String name, Object value) {
        return new UpdateQuery(getContext(), 
                               data.valuesToMutate(Immutables.merge(data.getValuesToMutate(), name, toOptional(value))));
    }
    

    @Override
    public <T> Write value(Name<T> name, T value) {
        return value(name.getName(), value);
    }
    
    @Override
    public UpdateQuery values(ImmutableMap<String, Object> nameValuePairsToAdd) {
        return new UpdateQuery(getContext(), 
                               data.valuesToMutate(Immutables.merge(data.getValuesToMutate(), Immutables.transform(nameValuePairsToAdd, name -> name, value -> toOptional(value)))));
    }


    @Override
    public UpdateQuery removeSetValue(String name, Object value) {
        ImmutableSet<Object> values = data.getSetValuesToRemove().get(name);
        values = (values == null) ? ImmutableSet.of(value) : Immutables.merge(values, value);

        return new UpdateQuery(getContext(), 
                               data.setValuesToRemove(Immutables.merge(data.getSetValuesToRemove(), name, values)));
    }

    
    @Override
    public UpdateQuery addSetValue(String name, Object value) {
        ImmutableSet<Object> values = data.getSetValuesToAdd().get(name);
        values = (values == null) ? ImmutableSet.of(value): Immutables.merge(values, value);

        return new UpdateQuery(getContext(), 
                               data.setValuesToAdd(Immutables.merge(data.getSetValuesToAdd(), name, values)));
    }
   
    
    @Override
    public Write prependListValue(String name, Object value) {
        ImmutableList<Object> values = data.getListValuesToPrepend().get(name);
        values = (values == null) ? ImmutableList.of(value) : Immutables.merge(values, value);

        return new UpdateQuery(getContext(), 
                               data.listValuesToPrepend(Immutables.merge(data.getListValuesToPrepend(), name, values)));
    } 
    
    

    @Override
    public Write appendListValue(String name, Object value) {
        ImmutableList<Object> values = data.getListValuesToAppend().get(name);
        values = (values == null) ? ImmutableList.of(value) : Immutables.merge(values, value);

        return new UpdateQuery(getContext(), 
                               data.listValuesToAppend(Immutables.merge(data.getListValuesToAppend(), name, values)));
    }
    
    
    
    @Override
    public Write removeListValue(String name, Object value) {
        ImmutableList<Object> values = data.getListValuesToRemove().get(name);
        values = (values == null) ? ImmutableList.of(value) : Immutables.merge(values, value);

        return new UpdateQuery(getContext(), 
                               data.listValuesToRemove(Immutables.merge(data.getListValuesToRemove(), name, values)));
    }
   
    
    @Override
    public Write putMapValue(String name, Object key, Object value) {
        ImmutableMap<Object, Optional<Object>> values = data.getMapValuesToMutate().get(name);
        values = (values == null) ? ImmutableMap.of(key, toOptional(value)) : Immutables.merge(values, key, toOptional(value));

        return new UpdateQuery(getContext(), 
                               data.mapValuesToMutate(Immutables.merge(data.getMapValuesToMutate(), name, values)));
    }
    
    

    @Override
    public Update<Write> onlyIf(Clause... conditions) {
        return new UpdateQuery(getContext(), 
                               data.onlyIfConditions(ImmutableList.<Clause>builder().addAll(data.getOnlyIfConditions())
                                                                                    .addAll(ImmutableList.copyOf(conditions))
                                                                                    .build()));
    }


    @Override
    public Insertion ifNotExits() {
        return new InsertionQuery(getContext(), new WriteQueryData().valuesToMutate(Immutables.merge(data.getValuesToMutate(), Immutables.transform(data.getKeyNameValuePairs(), name -> name, value -> toOptional(value))))
                                                                    .ifNotExists(Optional.of(true)));
    }

        
    @Override
    public CounterMutationQuery incr(String name) {
        return incr(name, 1);
    }
    
    @Override
    public CounterMutationQuery incr(String name, long value) {
        return new CounterMutationQuery(getContext(), 
                                        new CounterMutationQueryData().keys(data.getKeyNameValuePairs())
                                                                      .whereConditions(data.getWhereConditions())
                                                                      .name(name)
                                                                      .diff(value));  
    }
    
    
    @Override
    public CounterMutationQuery decr(String name) {
        return decr(name, 1);
    }
    
    @Override
    public CounterMutationQuery decr(String name, long value) {
        return new CounterMutationQuery(getContext(), 
                                        new CounterMutationQueryData().keys(data.getKeyNameValuePairs())
                                                                      .whereConditions(data.getWhereConditions())
                                                                      .name(name)
                                                                      .diff(0 - value));  
    }
    
    
    private Object toStatementValue(String name, Object value) {
        return getContext().toStatementValue(name, value);
    }
    
    
    private ImmutableSet<Object> toStatementValue(String name, ImmutableSet<Object> values) {
        return values.stream().map(value -> toStatementValue(name, value)).collect(Immutables.toSet());
    }

    
    private ImmutableList<Object> toStatementValue(String name, ImmutableList<Object> values) {
        return values.stream().map(value -> toStatementValue(name, value)).collect(Immutables.toList());
    }
  
    
    private Map<Object, Object> toStatementValue(String name, ImmutableMap<Object, Optional<Object>> map) {
        Map<Object, Object> m = Maps.newHashMap();
        for (Entry<Object, Optional<Object>> entry : map.entrySet()) {
            m.put(toStatementValue(name, toStatementValue(name, entry.getKey())), toStatementValue(name, entry.getValue().orElse(null)));
        }
        return m;
    }
    

    

    private Statement toStatement(WriteQueryData queryData) {
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
            
            
            queryData.getKeyNameValuePairs().keySet().forEach(keyname -> { update.where(eq(keyname, bindMarker())); values.add(getContext().toStatementValue(keyname, queryData.getKeyNameValuePairs().get(keyname))); } );
            
            queryData.getOnlyIfConditions().forEach(condition -> update.onlyIf(condition));
            
            if (getContext().getTtlSec() != null) {
                update.using(QueryBuilder.ttl(bindMarker())); 
                values.add(getContext().getTtlSec().intValue()); 
            }
            
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

            if (getContext().getTtlSec() != null) {
                update.using(QueryBuilder.ttl(getContext().getTtlSec().intValue()));
            }

            queryData.getWhereConditions().forEach(whereClause -> update.where(whereClause));
            
            return update;
        }
    }
    
    @Override
    protected Statement getStatement() {
        WriteQueryData queryData = data;
        for (WriteQueryPreInterceptor interceptor : getContext().getInterceptorRegistry().getInterceptors(WriteQueryPreInterceptor.class)) {
            queryData = interceptor.onPreWrite(queryData);
        }
        
        return toStatement(queryData);
    }
    
    
    @Override
    public CompletableFuture<Result> executeAsync() {
        return new CompletableDbFuture(performAsync(getStatement()))
                        .thenApply(resultSet -> Result.newResult(resultSet))
                        .thenApply(result ->  {
                                                // check cas result column '[applied]'
                                                if (!data.getOnlyIfConditions().isEmpty() && !result.wasApplied()) {
                                                    throw new IfConditionException("if condition does not match");  
                                                } 
                                                return result;
                                              });
    }
  
    
    private boolean isOptional(Object obj) {
        return (obj == null) ? false 
                             : (Optional.class.isAssignableFrom(obj.getClass()));
    }
 

    @SuppressWarnings("unchecked")
    private <T> Optional<T> toOptional(T obj) {
        return (obj == null) ? Optional.empty() 
                             : isOptional(obj) ? (Optional<T>) obj: Optional.of(obj);
    }
 

    
            
    private static class CounterMutationQueryData {
        
        private final ImmutableMap<String, Object> keys;
        private final ImmutableList<Clause> whereConditions;

        private final String name;
        private final long diff;

        
        public CounterMutationQueryData() {
            this(ImmutableMap.of(),
                 ImmutableList.of(),
                 null,
                 0);
        }
        private CounterMutationQueryData(ImmutableMap<String, Object> keys,
                                        ImmutableList<Clause> whereConditions,
                                        String name,
                                        long diff) {
            this.keys = keys;
            this.whereConditions = whereConditions;
            this.name = name; 
            this.diff = diff;
        }
       
        
        public CounterMutationQueryData keys(ImmutableMap<String, Object> keys) {
            return new CounterMutationQueryData(keys,
                                                this.whereConditions, 
                                                this.name,
                                                this.diff);
        }
        
        public CounterMutationQueryData whereConditions(ImmutableList<Clause> whereConditions) {
            return new CounterMutationQueryData(this.keys,
                                                whereConditions, 
                                                this.name,
                                                this.diff);
        }
        
        public CounterMutationQueryData name(String name) {
            return new CounterMutationQueryData(this.keys,
                                                this.whereConditions, 
                                                name,
                                                this.diff);
        }
        
        public CounterMutationQueryData diff(long diff) {
            return new CounterMutationQueryData(this.keys,
                                                this.whereConditions, 
                                                this.name,
                                                diff);
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
                
                if (getContext().getTtlSec() != null) {
                    update.using(QueryBuilder.ttl(bindMarker())); 
                    values.add(getContext().getTtlSec().intValue());
                }
                
                return prepare(update).bind(values.toArray());

                
            // where condition-based update
            } else {
                
                if (queryData.getDiff() > 0) {
                    update.with(QueryBuilder.incr(queryData.getName(), queryData.getDiff()));
                    
                } else {
                    update.with(QueryBuilder.decr(queryData.getName(), 0 - queryData.getDiff()));
                }
                                
                if (getContext().getTtlSec() != null) {
                    update.using(QueryBuilder.ttl(getContext().getTtlSec().intValue()));
                }
                
                queryData.getWhereConditions().forEach(whereClause -> update.where(whereClause));
                
                return update;
            }
        }
        
        @Override 
        protected Statement getStatement() {
            return toStatement(data);
        }

        
        public CompletableFuture<Result> executeAsync() {
            return new CompletableDbFuture(performAsync(getStatement()))
                            .thenApply(resultSet -> Result.newResult(resultSet));
        }
    }
}


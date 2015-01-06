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
import com.unitedinternet.troilus.Dao.CounterMutation;;


 
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
                                 new InsertQueryData(getContext().toValues(entity), false));
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
        return new InsertionQuery(getContext(), 
                                  new InsertQueryData(Immutables.merge(data.getValuesToMutate(), Immutables.transform(data.getKeys(), name -> name, value -> toOptional(value))), true));
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
    
    
    @Override
    protected Statement getStatement(Context ctx) {
        UpdateQueryData d = data;
        for (UpdateQueryBeforeInterceptor interceptor : ctx.getInterceptors(UpdateQueryBeforeInterceptor.class)) {
            d = interceptor.onBeforeUpdate(d);
        }
        
        return d.toStatement(ctx);
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
 

    
    
    
    

        
    private static class CounterMutationQueryData extends QueryData {
        
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

        
        @Override
        protected Statement toStatement(Context ctx) {
            com.datastax.driver.core.querybuilder.Update update = update(ctx.getTable());
            
            // key-based update
            if (whereConditions.isEmpty()) {
                List<Object> values = Lists.newArrayList();
                
                if (diff > 0) {
                    update.with(QueryBuilder.incr(name, bindMarker()));
                    values.add(diff);
                    
                } else {
                    update.with(QueryBuilder.decr(name, bindMarker()));
                    values.add(0 - diff);
                }
         
                keys.keySet().forEach(keyname -> { update.where(eq(keyname, bindMarker())); values.add(keys.get(keyname)); } );
                
                ctx.getTtl().ifPresent(ttl-> { update.using(QueryBuilder.ttl(bindMarker())); values.add((int) ttl.getSeconds()); });
                
                return ctx.prepare(update).bind(values.toArray());

                
            // where condition-based update
            } else {
                
                if (diff > 0) {
                    update.with(QueryBuilder.incr(name, diff));
                    
                } else {
                    update.with(QueryBuilder.decr(name, 0 - diff));
                }
                                
                ctx.getTtl().ifPresent(ttl-> update.using(QueryBuilder.ttl((int) ttl.getSeconds())));
                whereConditions.forEach(whereClause -> update.where(whereClause));
                
                return update;
            }
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
   
        
        @Override 
        protected Statement getStatement(Context ctx) {
            return data.toStatement(ctx);
        }
    }
}


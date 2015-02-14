/*
 * Copyright 1&1 Internet AG, https://github.com/1and1/
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.oneandone.troilus;

import net.oneandone.troilus.java7.CounterMutation;
import net.oneandone.troilus.java7.UpdateWithUnitAndCounter;
import net.oneandone.troilus.java7.WriteWithCounter;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Clause;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;


 
/**
 * update query implementation
 */
class UpdateQuery extends WriteQuery<WriteWithCounter> implements WriteWithCounter, UpdateWithUnitAndCounter  {
     
    
    /**
     * @param ctx   the context 
     * @param data  the query data
     */
    UpdateQuery(Context ctx, WriteQueryDataImpl data) {
        super(ctx, data);
    }
     
    @Override
    protected UpdateQuery newQuery(Context newContext) {
        return new UpdateQuery(newContext, getData());
    }
    
    /**
     * @param entity   the entity to insert
     * @return the new insert query
     */@Override
     public UpdateQuery entity(Object entity) {
        ImmutableMap<String, Optional<Object>> values = getContext().getBeanMapper().toValues(entity, getContext().getDbSession().getColumnNames());
        return new UpdateQuery(getContext(), 
                getData().valuesToMutate(Immutables.join(getData().getValuesToMutate(), values)));
    }
    
    @Override
    public UpdateQuery withTtl(int ttlSec) {
        return newQuery(getContext().withTtl(ttlSec));
    }
    
    @Override
    public UpdateQuery value(String name, Object value) {
        return new UpdateQuery(getContext(), 
                getData().valuesToMutate(Immutables.join(getData().getValuesToMutate(), name, Optionals.toGuavaOptional(value))));
    }
    
    @Override
    public <T> UpdateQuery value(ColumnName<T> name, T value) {
        return value(name.getName(), value);
    }
    
    @Override
    public UpdateQuery values(ImmutableMap<String, Object> nameValuePairsToAdd) {
        return new UpdateQuery(getContext(), 
                               getData().valuesToMutate(Immutables.join(getData().getValuesToMutate(), Optionals.toGuavaOptional(nameValuePairsToAdd))));
    }

    @Override
    public UpdateQuery removeSetValue(String name, Object value) {
        ImmutableSet<Object> values = getData().getSetValuesToRemove().get(name);
        values = (values == null) ? ImmutableSet.of(value) : Immutables.join(values, value);

        return new UpdateQuery(getContext(), 
                               getData().setValuesToRemove(Immutables.join(getData().getSetValuesToRemove(), name, values)));
    }

    @Override
    public UpdateQuery addSetValue(String name, Object value) {
        ImmutableSet<Object> values = getData().getSetValuesToAdd().get(name);
        values = (values == null) ? ImmutableSet.of(value): Immutables.join(values, value);

        return new UpdateQuery(getContext(), 
                               getData().setValuesToAdd(Immutables.join(getData().getSetValuesToAdd(), name, values)));
    }
   
    @Override
    public UpdateQuery prependListValue(String name, Object value) {
        ImmutableList<Object> values = getData().getListValuesToPrepend().get(name);
        values = (values == null) ? ImmutableList.of(value) : Immutables.join(values, value);

        return new UpdateQuery(getContext(), 
                               getData().listValuesToPrepend(Immutables.join(getData().getListValuesToPrepend(), name, values)));
    } 
    
    @Override
    public UpdateQuery appendListValue(String name, Object value) {
        ImmutableList<Object> values = getData().getListValuesToAppend().get(name);
        values = (values == null) ? ImmutableList.of(value) : Immutables.join(values, value);

        return new UpdateQuery(getContext(), 
                               getData().listValuesToAppend(Immutables.join(getData().getListValuesToAppend(), name, values)));
    }
    
    @Override
    public UpdateQuery removeListValue(String name, Object value) {
        ImmutableList<Object> values = getData().getListValuesToRemove().get(name);
        values = (values == null) ? ImmutableList.of(value) : Immutables.join(values, value);

        return new UpdateQuery(getContext(), 
                               getData().listValuesToRemove(Immutables.join(getData().getListValuesToRemove(), name, values)));
    }
   
    @Override
    public UpdateQuery putMapValue(String name, Object key, Object value) {
        ImmutableMap<Object, Optional<Object>> values = getData().getMapValuesToMutate().get(name);
        values = (values == null) ? ImmutableMap.of(key, Optionals.toGuavaOptional(value)) : Immutables.join(values, key, Optionals.toGuavaOptional(value));

        return new UpdateQuery(getContext(), 
                               getData().mapValuesToMutate(Immutables.join(getData().getMapValuesToMutate(), name, values)));
    }
    
    @Override
    public UpdateQuery onlyIf(Clause... conditions) {
        return new UpdateQuery(getContext(), 
                               getData().onlyIfConditions(ImmutableList.<Clause>builder().addAll(getData().getOnlyIfConditions())
                                                                                         .addAll(ImmutableList.copyOf(conditions))
                                                                                         .build()));
    }

    @Override
    public InsertQuery ifNotExists() {
        return new InsertQuery(getContext(), new WriteQueryDataImpl().valuesToMutate(Immutables.join(getData().getValuesToMutate(), Optionals.toGuavaOptional(getData().getKeys())))
                                                                     .ifNotExists(true));
    }
        
    @Override
    public CounterMutationQuery incr(String name) {
        return incr(name, 1);
    }
    
    @Override
    public CounterMutationQuery incr(String name, long value) {
        return new CounterMutationQuery(getContext(), 
                                        new CounterMutationQueryData().keys(getData().getKeys())
                                                                      .whereConditions(getData().getWhereConditions())
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
                                        new CounterMutationQueryData().keys(getData().getKeys())
                                                                      .whereConditions(getData().getWhereConditions())
                                                                      .name(name)
                                                                      .diff(0 - value));  
    }
    
  
    
 
    /**
     * Counter mutation query implementation 
     *
     */
    static final class CounterMutationQuery extends AbstractQuery<CounterMutation> implements CounterMutation {
        
        private final CounterMutationQueryData data;

        /**
         * @param ctx    the context
         * @param data   the query data
         */
        CounterMutationQuery(Context ctx, CounterMutationQueryData data) {
            super(ctx);
            this.data = data;
        }
        
        @Override
        protected CounterMutationQuery newQuery(Context newContext) {
            return new CounterMutationQuery(newContext, data);
        }
   
        @Override
        public CounterMutationQuery withTtl(int ttlSec) {
            return newQuery(getContext().withTtl(ttlSec));
        }
        
        @Override
        public CounterBatchMutationQuery combinedWith(CounterBatchable other) {
            return new CounterBatchMutationQuery(getContext(), ImmutableList.of(this, other));
        }
   
        @Override
        public Result execute() {
            return ListenableFutures.getUninterruptibly(executeAsync());
        }
        
        @Override
        public ListenableFuture<Result> executeAsync() {
            ListenableFuture<ResultSet> future = performAsync(getStatementAsync());
            
            Function<ResultSet, Result> mapEntity = new Function<ResultSet, Result>() {
                @Override
                public Result apply(ResultSet resultSet) {
                    return newResult(resultSet);
                }
            };
            
            return Futures.transform(future, mapEntity);
        }
        
        @Override
        public ListenableFuture<Statement> getStatementAsync() {
            return data.toStatementAsync(getContext());
        }
    }
}


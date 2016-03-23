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


import java.util.Optional;

import java.util.concurrent.CompletableFuture;

import com.datastax.driver.core.Statement;
import com.google.common.collect.ImmutableMap;


/**
 * abstract write query implementation
 */
abstract class WriteQuery<Q> extends MutationQuery<Q> {
    private final WriteQueryData data;
  
    /**
     * @param ctx   the context
     * @param data  the data
     */
    WriteQuery(final Context ctx, final WriteQueryData data) {
        super(ctx);
        this.data = data;
    }
    
    protected WriteQueryData getData() {
        return data;
    }
    
    public CounterMutationQuery incr(final String name, final long value) {
        return new CounterMutationQuery(getContext(), 
                                        new CounterMutationQueryData(data.getTablename()).keys(getData().getKeys())
                                                                                         .whereConditions(getData().getWhereConditions())
                                                                                         .name(name)
                                                                                         .diff(value));  
    }
    
    public CounterMutationQuery decr(final String name, final long value) {
        return new CounterMutationQuery(getContext(), 
                                        new CounterMutationQueryData(data.getTablename()).keys(getData().getKeys())
                                                                                         .whereConditions(getData().getWhereConditions())
                                                                                         .name(name)
                                                                                         .diff(0 - value));  
    }
    
    protected ImmutableMap<Object, Optional<Object>> addToMap(final String name, 
                                                              final Object key, 
                                                              final Object value, 
                                                              final ImmutableMap<Object, Optional<Object>> values) {
        return (values == null) ? ImmutableMap.of(key, Optionals.toOptional(value)) 
                                : Immutables.join(values, key, Optionals.toOptional((value)));
    }
    
    @Override
    public CompletableFuture<Result> executeAsync() {
        return super.executeAsync().thenApply(result -> {
                                                            if (isLwt() && !result.wasApplied()) {
                                                                throw new IfConditionException(result, "duplicated entry");
                                                            }
                                                            return result;
                                                        });
    }

    private boolean isLwt() {
        return ((data.getIfNotExits() != null) && (data.getIfNotExits()) || !data.getOnlyIfConditions().isEmpty());                
    }
    
    public CompletableFuture<Statement> getStatementAsync(final DBSession dbSession) {
        return WriteQueryDataImpl.toStatementAsync(data, getExecutionSpec(), getUDTValueMapper(), dbSession);
    }
}
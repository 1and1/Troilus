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

import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.set;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.BatchStatement.Type;
import com.datastax.driver.core.querybuilder.Insert;
import com.google.common.collect.ImmutableList;


 
abstract class MutationQuery<Q> extends AbstractQuery<Q> implements Batchable {
    
    private final QueryFactory queryFactory;
    
    public MutationQuery(Context ctx, QueryFactory queryFactory) {
        super(ctx);
        this.queryFactory = queryFactory;
    }
    
    
    public Q withTtl(Duration ttl) {
        return newQuery(getContext().withTtl(ttl));
    }

    public Q withWritetime(long writetimeMicrosSinceEpoch) {
        return newQuery(getContext().withWritetime(writetimeMicrosSinceEpoch));
    }
       
    public Q withSerialConsistency(ConsistencyLevel consistencyLevel) {
        return newQuery(getContext().withSerialConsistency(consistencyLevel));
    }
    
    

    public BatchMutation combinedWith(Batchable other) {
        return queryFactory.newBatchMutation(getContext(), Type.LOGGED, ImmutableList.of(this, other));
    }
    
    
    
    public Result execute() {
        try {
            return executeAsync().get(Long.MAX_VALUE, TimeUnit.DAYS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw Exceptions.unwrapIfNecessary(e);
        } 
    }
    
    
    public CompletableFuture<Result> executeAsync() {
        return performAsync(getStatement()).thenApply(resultSet -> Result.newResult(resultSet));
    }
    
    
    @Override
    public String toString() {
        return getStatement().toString();
    }
    
    
    
  
 

    public static interface ValueToMutate {
        Object addPreparedToStatement(Insert insert);

        void addToStatement(Insert insert);
        
        Object addPreparedToStatement(com.datastax.driver.core.querybuilder.Update update);
        
        void addToStatement(com.datastax.driver.core.querybuilder.Update update);
    }
    

     
    
    protected final class BuildinValueToMutate implements ValueToMutate {
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
        public Object addPreparedToStatement(Insert insert) {
            insert.value(name, bindMarker());
            return value;
        }
        
        @Override
        public void addToStatement(Insert insert) {
            insert.value(name,  value);
        }

        public Object addPreparedToStatement(com.datastax.driver.core.querybuilder.Update update) {
            update.with(set(name, bindMarker()));
            return value;
        }
        
        
        @Override
        public void addToStatement(com.datastax.driver.core.querybuilder.Update update) {
            update.with(set(name, value));
        }
    }
   
    
    
    protected final class UDTValueToMutate implements ValueToMutate {
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
        public Object addPreparedToStatement(Insert insert) {
            insert.value(columnName, bindMarker());
            return UDTValueMapper.toUdtValue(getContext(), getColumnMetadata(columnName).getType(), value);
        }

        @Override
        public void addToStatement(Insert insert) {
            insert.value(columnName, value);
        }
        
        public Object addPreparedToStatement(com.datastax.driver.core.querybuilder.Update update) {
            update.with(set(columnName, bindMarker()));
            return UDTValueMapper.toUdtValue(getContext(), getColumnMetadata(columnName).getType(), value);
        }
        
        @Override
        public void addToStatement(com.datastax.driver.core.querybuilder.Update update) {
            update.with(set(columnName, UDTValueMapper.toUdtValue(getContext(), getColumnMetadata(columnName).getType(), value)));
        }
    }
}


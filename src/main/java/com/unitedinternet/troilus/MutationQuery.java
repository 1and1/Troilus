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
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.BatchStatement.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.unitedinternet.troilus.Dao.BatchMutation;


 
abstract class MutationQuery<Q> extends AbstractQuery<Q> implements Batchable {
    
    public MutationQuery(Context ctx) {
        super(ctx);
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
        return BatchMutationQuery.newBatchMutationQuery(getContext(), Type.LOGGED, ImmutableList.of(this, other));
    }
    
    
    @Override
    public void addTo(BatchStatement batchStatement) {
        batchStatement.add(getStatement());
    }
    

    public CompletableFuture<Result> executeAsync() {
        return performAsync(getStatement()).thenApply(resultSet -> Result.newResult(resultSet));
    }
    
    protected abstract Statement getStatement();
   
  
    
    protected ImmutableSet<Object> toStatementValue(String name, ImmutableSet<Object> values) {
        return values.stream().map(value -> toStatementValue(name, value)).collect(Immutables.toSet());
    }
  
    protected ImmutableList<Object> toStatementValue(String name, ImmutableList<Object> values) {
        return values.stream().map(value -> toStatementValue(name, value)).collect(Immutables.toList());
    }
  
    
    protected Object toStatementValue(String name, Object value) {
        if (value == null) {
            return value;
        }
    
        // map empty collection to null
        if (Collection.class.isAssignableFrom(value.getClass())) {
            if (((Collection<?>) value).isEmpty()) {
                return null;
            }
        }
        if (Map.class.isAssignableFrom(value.getClass())) {
            if (((Map<?, ?>) value).isEmpty()) {
                return null;
            }
        } 
        
        
        DataType dataType = getColumnMetadata(name).getType();
        if (isBuildInType(dataType)) {
            return value;
        } else {
            return UDTValueMapper.toUdtValue(getContext(), getColumnMetadata(name).getType(), value);
        }
    }
}


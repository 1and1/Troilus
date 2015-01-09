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
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.BatchStatement.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.unitedinternet.troilus.Dao.BatchMutation;
import com.unitedinternet.troilus.Dao.Batchable;



 
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
        return new BatchMutationQuery(getContext(), Type.LOGGED, ImmutableList.of(this, other));
    }
    
    
    @Override
    public void addTo(BatchStatement batchStatement) {
        batchStatement.add(getStatement());
    }
    

    public CompletableFuture<Result> executeAsync() {
        return performAsync(getStatement()).thenApply(resultSet -> new ResultImpl(resultSet));
    }
    
    protected abstract Statement getStatement();
   
  
    
    protected ImmutableSet<Object> toStatementValue(String name, ImmutableSet<Object> values) {
        return values.stream().map(value -> toStatementValue(name, value)).collect(Immutables.toSet());
    }
  
    protected ImmutableList<Object> toStatementValue(String name, ImmutableList<Object> values) {
        return values.stream().map(value -> toStatementValue(name, value)).collect(Immutables.toList());
    }
  
    protected Map<Object, Object> toStatementValue(String name, ImmutableMap<Object, Optional<Object>> map) {
        Map<Object, Object> m = Maps.newHashMap();
        for (Entry<Object, Optional<Object>> entry : map.entrySet()) {
            m.put(toStatementValue(name, toStatementValue(name, entry.getKey())), toStatementValue(name, entry.getValue().orElse(null)));
        }
        return m;
    }
    
    
    protected Object toStatementValue(String name, Object value) {
        if (isNullOrEmpty(value)) {
            return null;
        } 
        
        DataType dataType = getContext().getColumnMetadata(name).getType();
        return (getContext().isBuildInType(dataType)) ? value : getContext().getUDTValueMapper().toUdtValue(getContext().getColumnMetadata(name).getType(), value);
    }

    
    private boolean isNullOrEmpty(Object value) {
        return (value == null) || 
               (Collection.class.isAssignableFrom(value.getClass()) && ((Collection<?>) value).isEmpty()) || 
               (Map.class.isAssignableFrom(value.getClass()) && ((Map<?, ?>) value).isEmpty());
    }
}


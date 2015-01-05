/*
 * Copyright (c)  2014 1&1 Internet AG, Germany, http://www.1und1.de
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


import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.Optional;
import java.util.UUID;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.TupleValue;
import com.datastax.driver.core.UDTValue;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;



/**
 * Record
 *
 * @author grro
 */
public abstract class Record implements Result {
   
    private final Result result;
    private final Row row;
    
    
    Record(Result result, Row row) {
        this.result = result;
        this.row = row;
    }
    
    
    protected Row getRow() {
        return row;
    }
    
    @Override
    public ExecutionInfo getExecutionInfo() {
        return result.getExecutionInfo();
    }
    
    @Override
    public ImmutableList<ExecutionInfo> getAllExecutionInfo() {
        return result.getAllExecutionInfo();
    }
    

    @Override
    public boolean wasApplied() {
        return result.wasApplied();
    }

    public Optional<Long> getWritetime(String name) {
        try {
            return row.isNull("WRITETIME(" + name + ")") ? Optional.empty() : Optional.of(row.getLong("WRITETIME(" + name + ")"));
        } catch (IllegalArgumentException iae) {
            return Optional.empty();
        }
    }
  
    public Optional<Duration> getTtl(String name) {
        try {
            return row.isNull("TTL(" + name + ")") ? Optional.empty() : Optional.of(Duration.ofSeconds(row.getInt("TTL(" + name + ")")));
        } catch (IllegalArgumentException iae) {
            return Optional.empty();
        }
    }
    
    public ColumnDefinitions getColumnDefinitions() {
        return row.getColumnDefinitions();
    }
    
    protected boolean isNull(String name) {
        return row.isNull(name);
    }
     
    public Optional<Long> getLong(String name) {
        return isNull(name) ? Optional.empty() : Optional.of(row.getLong(name));
    }
    
     
    public Optional<String> getString(String name) {
        return isNull(name) ? Optional.empty() : Optional.of(row.getString(name));
    }
    

    public Optional<Boolean> getBool(String name) {
        return isNull(name) ? Optional.empty() : Optional.of(row.getBool(name));
    }
    
     
    public Optional<ByteBuffer> getBytes(String name) {
        return isNull(name) ? Optional.empty() : Optional.of(row.getBytes(name));
    }
    
     
    public Optional<ByteBuffer> getBytesUnsafe(String name) {
        return isNull(name) ? Optional.empty() : Optional.of(row.getBytesUnsafe(name));
    }
    
     
    public Optional<Float> getFloat(String name) {
        return isNull(name) ? Optional.empty() : Optional.of(row.getFloat(name));
    }
    
     
    public Optional<Date> getDate(String name) {
        return isNull(name) ? Optional.empty() : Optional.of(row.getDate(name));
    }
    
     
    public Optional<BigDecimal> getDecimal(String name) {
        return isNull(name) ? Optional.empty() : Optional.of(row.getDecimal(name));
    }
    
     
    public Optional<Integer> getInt(String name) {
        return isNull(name) ? Optional.empty() : Optional.of(row.getInt(name));
    }
    
     
    public Optional<InetAddress> getInet(String name) {
        return isNull(name) ? Optional.empty() : Optional.of(row.getInet(name));
    }
    
     
    public Optional<Instant> getInstant(String name) {
        return getLong(name).map(millis -> Instant.ofEpochMilli(millis));
    }
    
     
    public Optional<BigInteger> getVarint(String name) {
        return isNull(name) ? Optional.empty() : Optional.of(row.getVarint(name));
    }
    
     
    public Optional<TupleValue> getTupleValue(String name) {
        return isNull(name) ? Optional.empty() : Optional.of(row.getTupleValue(name));
    }
    
    
    public Optional<UUID> getUUID(String name) {
        return isNull(name) ? Optional.empty() : Optional.of(row.getUUID(name));
    }
   
    
    public Optional<UDTValue> getUDTValue(String name) {
        return isNull(name) ? Optional.empty() : Optional.of(row.getUDTValue(name));
    }

    public abstract <T> Optional<T> getObject(String name, Class<T> elementsClass);    
    
    public abstract <T> Optional<ImmutableSet<T>> getSet(String name, Class<T> elementsClass);
     
    public abstract <T> Optional<ImmutableList<T>> getList(String name, Class<T> elementsClass);
       
    public abstract <K, V> Optional<ImmutableMap<K, V>> getMap(String name, Class<K> keysClass, Class<V> valuesClass);
    

    
    TriFunction<String, Class<?>, Class<?>, Optional<?>> getAccessor() {
        return new RecordAccessorAdapter(this);
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
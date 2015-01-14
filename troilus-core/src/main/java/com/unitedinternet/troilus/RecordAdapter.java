/*
 * Copyright (c) 2015 1&1 Internet AG, Germany, http://www.1und1.de
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

import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.UDTValue;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;


 
class RecordAdapter implements Record {
    
    private final RecordImpl record;
    
    RecordAdapter(RecordImpl record) {
        this.record = record;
    }

    
    @Override
    public ExecutionInfo getExecutionInfo() {
        return record.getExecutionInfo();
    }
    
    @Override
    public ImmutableList<ExecutionInfo> getAllExecutionInfo() {
        return record.getAllExecutionInfo();
    }
    
    @Override
    public boolean wasApplied() {
        return record.wasApplied();
    }

    @Override
    public Optional<Long> getWritetime(String name) {
        return Optional.ofNullable(record.getWritetime(name));
    }
  
    
    @Override       
    public Optional<Duration> getTtl(String name) {
        return Optional.ofNullable(record.getTtl(name)).map(ttlSec -> Duration.ofSeconds(ttlSec));
    }
    

    @Override
    public boolean isNull(String name) {
        return record.isNull(name);
    }
     

    @Override
    public Optional<Long> getLong(String name) {
        return Optional.ofNullable(record.getLong(name));
    }
    

    @Override
    public Optional<String> getString(String name) {
        return Optional.ofNullable(record.getString(name));
    }
    

    @Override
    public Optional<Boolean> getBool(String name) {
        return Optional.ofNullable(record.getBool(name));
    }
    

    @Override
    public Optional<ByteBuffer> getBytes(String name) {
        return Optional.ofNullable(record.getBytes(name));
    }
     

    @Override
    public Optional<ByteBuffer> getBytesUnsafe(String name) {
        return Optional.ofNullable(record.getBytesUnsafe(name));
    }
    

    @Override
    public Optional<Float> getFloat(String name) {
        return Optional.ofNullable(record.getFloat(name));
    }


    @Override
    public Optional<Date> getDate(String name) {
        return Optional.ofNullable(record.getDate(name));
    }
     

    @Override
    public Optional<BigDecimal> getDecimal(String name) {
        return Optional.ofNullable(record.getDecimal(name));
    }
    

    @Override
    public Optional<Integer> getInt(String name) {
        return Optional.ofNullable(record.getInt(name));
    }
    

    @Override
    public Optional<InetAddress> getInet(String name) {
        return Optional.ofNullable(record.getInet(name));
    }
     

    @Override
    public Optional<BigInteger> getVarint(String name) {
        return Optional.ofNullable(record.getVarint(name));
    }
  

    @Override
    public Optional<UUID> getUUID(String name) {
        return Optional.ofNullable(record.getUUID(name));
    }
   

    @Override
    public Optional<UDTValue> getUDTValue(String name) {
        return Optional.ofNullable(record.getUDTValue(name));
    }
    
    
    @Override
    public Optional<Instant> getInstant(String name) {
        return getLong(name).map(millis -> Instant.ofEpochMilli(millis));
    }
  
    @Override
    public <T extends Enum<T>> Optional<T> getEnum(String name, Class<T> enumType) {
        return Optional.ofNullable(record.getEnum(name, enumType));
    }
    
    @Override
    public <T> Optional<T> getValue(Name<T> name) {
        return Optional.ofNullable(record.getValue(name));
    }

    @Override
    public <T> Optional<T> getObject(String name, Class<T> elementsClass) {
        return Optional.ofNullable(record.getObject(name, elementsClass));
    }
    
    
    public <T> Optional<ImmutableSet<T>> getSet(String name, Class<T> elementsClass) {
        return Optional.ofNullable(record.getSet(name, elementsClass));
    }
    
 
    public <T> Optional<ImmutableList<T>> getList(String name, Class<T> elementsClass) {
        return Optional.ofNullable(record.getList(name, elementsClass));
    }
    
    
    public <K, V> Optional<ImmutableMap<K, V>> getMap(String name, Class<K> keysClass, Class<V> valuesClass) {
        return Optional.ofNullable(record.getMap(name, keysClass, valuesClass));
    }
    
    
    public String toString() {
        return record.toString();
    }
    
        
    static PropertiesSource toPropertiesSource(Record record) {
        return new PropertiesSourceAdapter(record);
    }
    
    
    private static class PropertiesSourceAdapter implements PropertiesSource {
        
        private final Record record;
        
        public PropertiesSourceAdapter(Record record) {
            this.record = record;
        }
        
        @Override
        public <T> com.google.common.base.Optional<T> read(String name, Class<?> clazz1) {
            return read(name, clazz1, Object.class);
        }
            
        @SuppressWarnings("unchecked")
        @Override
        public <T> com.google.common.base.Optional<T> read(String name, Class<?> clazz1, Class<?> clazz2) {
            return GuavaOptionals.toOptional((Optional<T>) record.getObject(name, clazz1));
        }
    }
}

    
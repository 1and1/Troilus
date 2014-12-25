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


import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.TupleValue;
import com.datastax.driver.core.UDTValue;
import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;
import com.google.common.collect.ImmutableList;



/**
 * Record
 *
 * @author grro
 */
public class Record implements Result {
   
    private final ProtocolVersion protocolVersion;
    private final Result result;
    private final Row row;
    
    
    Record(ProtocolVersion protocolVersion, Result result, Row row) {
        this.protocolVersion = protocolVersion;
        this.result = result;
        this.row = row;
    }
    
    @Override
    public ExecutionInfo getExecutionInfo() {
        return result.getExecutionInfo();
    }
    
    @Override
    public ImmutableList<ExecutionInfo> getAllExecutionInfo() {
        return result.getAllExecutionInfo();
    }
    
    
    public ProtocolVersion getProtocolVersion() {
        return protocolVersion;
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
    
    private boolean isNull(String name) {
        return row.isNull(name);
    }
     
    Optional<Object> get(String name) {
        return isNull(name) ? Optional.empty() : Optional.of(row.getColumnDefinitions().getType(name).deserialize(row.getBytesUnsafe(name), protocolVersion));
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
    
     
    public <T> Optional<Set<T>> getSet(String name, Class<T> elementsClass) {
        return isNull(name) ? Optional.empty() : Optional.of(row.getSet(name, elementsClass));
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
    
     
    public <T> Optional<List<T>> getList(String name, Class<T> elementsClass) {
        return Optional.ofNullable(row.getList(name, elementsClass));
    }
    
     
    public <K, V> Optional<Map<K, V>> getMap(String name, Class<K> keysClass, Class<V> valuesClass) {
        return Optional.ofNullable(row.getMap(name, keysClass, valuesClass));
    }
    
    
    private Optional<String> toString(String name, DataType dataType) {
        if (isNull(name)) {
            return Optional.empty();
        } else {
            StringBuilder builder = new StringBuilder();
            builder.append(dataType.deserialize(row.getBytesUnsafe(name), protocolVersion));

            return Optional.of(builder.toString());
        }
    }
    
    
    public Optional<UDTValue> getUDTValue(String name) {
        return isNull(name) ? Optional.empty() : Optional.of(row.getUDTValue(name));
    }


    
    public String toString() {
        ToStringHelper toStringHelper = MoreObjects.toStringHelper(this);
        row.getColumnDefinitions().asList()
                                  .forEach(definition -> toString(definition.getName(), definition.getType()).ifPresent(value -> toStringHelper.add(definition.getName(), value)));
        return toStringHelper.toString();
    }
}




/*
 * Copyright (c)  2015 1&1 Internet AG, Germany, http://www.1und1.de
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
import com.datastax.driver.core.UDTValue;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;



/**
 * record result
 *
 * @author grro
 */
public abstract class Record extends Result implements PropertiesSource {
   
    private final Result result;
    private final Row row;
    
    
    /**
     * @param result  the common result data
     * @param row     the underlying row
     */
    Record(Result result, Row row) {
        this.result = result;
        this.row = row;
    }
    
    
    /**
     * @return the underlying row
     */
    Row getRow() {
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

    
    /**
     * @return  the columns returned in this ResultSet
     */
    ColumnDefinitions getColumnDefinitions() {
        return row.getColumnDefinitions();
    }
    
    
    /**
     * @param name  the column name 
     * @return the write time of this column or empty if no write time was requested
     */
    public Optional<Long> getWritetime(String name) {
        try {
            return row.isNull("WRITETIME(" + name + ")") ? Optional.empty() : Optional.of(row.getLong("WRITETIME(" + name + ")"));
        } catch (IllegalArgumentException iae) {
            return Optional.empty();
        }
    }
  
    /**
     * @param name  the column name 
     * @return the ttl of this column or empty if no ttl was requested or <code>null</code>
     */
    public Optional<Duration> getTtl(String name) {
        try {
            return row.isNull("TTL(" + name + ")") ? Optional.empty() : Optional.of(Duration.ofSeconds(row.getInt("TTL(" + name + ")")));
        } catch (IllegalArgumentException iae) {
            return Optional.empty();
        }
    }
    
  
    
    /**
     * @param name  the column name 
     * @return whether the value for column name in this row is NULL
     */
    public boolean isNull(String name) {
        return row.isNull(name);
    }
     
    /**
     * @param name the column name 
     * @return the value of column name as a long
     */
    public Optional<Long> getLong(String name) {
        return Optional.ofNullable(row.getLong(name));
    }
    
    /**
     * @param name the column name 
     * @return the value of column name as a string
     */
    public Optional<String> getString(String name) {
        return Optional.ofNullable(row.getString(name));
    }
    
    /**
     * @param name the column name 
     * @return the value of column name as a boolean
     */
    public Optional<Boolean> getBool(String name) {
        return Optional.ofNullable(row.getBool(name));
    }
    
    /**
     * @param name the column name 
     * @return the value of column name as a ByteBuffer
     */
    public Optional<ByteBuffer> getBytes(String name) {
        return Optional.ofNullable(row.getBytes(name));
    }
     
    /**
     * @param name the column name 
     * @return the value of column name as a ByteBuffer. This method always return the bytes composing the value, even if the column is not of type BLOB
     */
    public Optional<ByteBuffer> getBytesUnsafe(String name) {
        return Optional.ofNullable(row.getBytesUnsafe(name));
    }
    
    /**
     * @param name the column name 
     * @return value of column name as a float
     */
    public Optional<Float> getFloat(String name) {
        return Optional.ofNullable(row.getFloat(name));
    }
    
    /**
     * @param name the column name 
     * @return
     */
    public Optional<Date> getDate(String name) {
        return Optional.ofNullable(row.getDate(name));
    }
     
    /**
     * @param name the column name 
     * @return the value of column name as a date
     */
    public Optional<BigDecimal> getDecimal(String name) {
        return Optional.ofNullable(row.getDecimal(name));
    }
    
    /**
     * @param name the column name 
     * @return value of column name as an integer
     */
    public Optional<Integer> getInt(String name) {
        return Optional.ofNullable(row.getInt(name));
    }
    
    /**
     * @param name the column name 
     * @return value of column name as an InetAddress
     */
    public Optional<InetAddress> getInet(String name) {
        return Optional.ofNullable(row.getInet(name));
    }
     
    /**
     * @param name the column name 
     * @return the value of column name as a variable length integer
     */
    public Optional<BigInteger> getVarint(String name) {
        return Optional.ofNullable(row.getVarint(name));
    }
  
  
    /**
     * @param name the column name 
     * @return the value of column name as a UUID
     */
    public Optional<UUID> getUUID(String name) {
        return Optional.ofNullable(row.getUUID(name));
    }
   
    /**
     * @param name the column name 
     * @return the value for column name as a UDT value
     */
    public Optional<UDTValue> getUDTValue(String name) {
        return Optional.ofNullable(row.getUDTValue(name));
    }
    
    
    
    /**
     * @param name the column name 
     * @return the value for column name as an Instant value
     */
    public Optional<Instant> getInstant(String name) {
        return getLong(name).map(millis -> Instant.ofEpochMilli(millis));
    }
  
    
    /**
     * @param name      the column name 
     * @param enumType  the enum type
     * @return the value for column name as an enum value
     */
    public <T extends Enum<T>> Optional<T> getEnum(String name, Class<T> enumType) {
        return getObject(name, enumType);
    }
    
    /**
     * @param name the column name 
     * @return the value for column name as an value type according the name definition
     */
    public <T> Optional<T> getValue(Name<T> name) {
        return Optional.ofNullable(name.read(this).orNull());
    }

      
    /**
     * @param name   the column name 
     * @param typ    the class for value to retrieve.
     * @return  the value of column name 
     */
    public abstract <T> Optional<T> getObject(String name, Class<T> type);    
    
    
    /**
     * @param name           the column name 
     * @param elementsClass  the class for the elements of the set to retrieve
     * @return  the value of column name as a set
     */
    public abstract <T> Optional<ImmutableSet<T>> getSet(String name, Class<T> elementsClass);
     
    
    /**
     * @param name          the column name 
     * @param elementsClass the class for the elements of the list to retriev
     * @return  the value of column name as a list
     */
    public abstract <T> Optional<ImmutableList<T>> getList(String name, Class<T> elementsClass);
       
    /**
     * @param name           the column name 
     * @param keysClass      the class for the keys of the map to retrieve.
     * @param valuesClass    the class for the values of the map to retrieve
     * @return  the value of column name as a map
     */
    public abstract <K, V> Optional<ImmutableMap<K, V>> getMap(String name, Class<K> keysClass, Class<V> valuesClass);
    
     
    @Override
    public <T> com.google.common.base.Optional<T> read(String name, Class<?> clazz1) {
        return read(name, clazz1, Object.class);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> com.google.common.base.Optional<T> read(String name, Class<?> clazz1, Class<?> clazz2) {
        Optional<?> value = getObject(name, clazz1);
        if (value.isPresent()) {
            return com.google.common.base.Optional.of((T) value.get());
        } else {
            return com.google.common.base.Optional.absent();
        }
    }
}
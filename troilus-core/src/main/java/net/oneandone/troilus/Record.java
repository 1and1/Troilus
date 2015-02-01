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


import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.Optional;
import java.util.UUID;

import net.oneandone.troilus.ColumnName;
import net.oneandone.troilus.Result;

import com.datastax.driver.core.UDTValue;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;



/**
 * record result
 *
 * @author grro
 */
public interface Record extends Result {
   

    /**
     * @param name  the column name 
     * @return the write time of this column or empty if no write time was requested
     */
    Optional<Long> getWritetime(String name);
    
    /**
     * @param name  the column name 
     * @return the ttl of this column or empty if no ttl was requested or <code>null</code>
     */
    Optional<Duration> getTtl(String name);
    
    /**
     * @param name  the column name 
     * @return whether the value for column name in this row is NULL
     */
    boolean isNull(String name);
    
     
    /**
     * @param name the column name 
     * @return the value of column name as a long
     */
    Optional<Long> getLong(String name);
    
    /**
     * @param name the column name 
     * @return the value of column name as a string
     */
    Optional<String> getString(String name);
    
    /**
     * @param name the column name 
     * @return the value of column name as a boolean
     */
    Optional<Boolean> getBool(String name);
    
    /**
     * @param name the column name 
     * @return the value of column name as a ByteBuffer
     */
    Optional<ByteBuffer> getBytes(String name);
     
    /**
     * @param name the column name 
     * @return the value of column name as a ByteBuffer. This method always return the bytes composing the value, even if the column is not of type BLOB
     */
    Optional<ByteBuffer> getBytesUnsafe(String name);
    
    /**
     * @param name the column name 
     * @return value of column name as a float
     */
    Optional<Float> getFloat(String name);
    
    /**
     * @param name the column name 
     * @return value of column name as a date
     */
    Optional<Date> getDate(String name);
     
    /**
     * @param name the column name 
     * @return the value of column name as a dedcimal
     */
    Optional<BigDecimal> getDecimal(String name);
    
    /**
     * @param name the column name 
     * @return value of column name as an integer
     */
    Optional<Integer> getInt(String name);
    
    /**
     * @param name the column name 
     * @return value of column name as an InetAddress
     */
    Optional<InetAddress> getInet(String name);
     
    /**
     * @param name the column name 
     * @return the value of column name as a variable length integer
     */
    Optional<BigInteger> getVarint(String name);
  
  
    /**
     * @param name the column name 
     * @return the value of column name as a UUID
     */
    Optional<UUID> getUUID(String name);
   
    /**
     * @param name the column name 
     * @return the value for column name as a UDT value
     */
    Optional<UDTValue> getUDTValue(String name);
    
    /**
     * @param name the column name 
     * @return the value for column name as an Instant value
     */
    Optional<Instant> getInstant(String name);
    
    /**
     * @param name      the column name 
     * @param enumType  the enum type
     * @param <T> the enum type class
     * @return the value for column name as an enum value
     */
    <T extends Enum<T>> Optional<T> getEnum(String name, Class<T> enumType);
        
    /**
     * @param name the column name 
     * @param <T> the name type
     * @return the value for column name as an value type according the name definition
     */
    <T> Optional<T> getValue(ColumnName<T> name);
   
    /**
     * @param name   the column name 
     * @param type   the class for value to retrieve.
     * @param <T>  the object type
     * @return  the value of column name 
     */
    <T> Optional<T> getValue(String name, Class<T> type);    
    
    /**
     * @param name           the column name 
     * @param elementsClass  the class for the elements of the set to retrieve
     * @param <T>  the element type
     * @return  the value of column name as a set
     */
    <T> Optional<ImmutableSet<T>> getSet(String name, Class<T> elementsClass);
         
    /**
     * @param name          the column name 
     * @param elementsClass the class for the elements of the list to retriev
     * @param <T> the element type
     * @return  the value of column name as a list
     */
    <T> Optional<ImmutableList<T>> getList(String name, Class<T> elementsClass);
    
    /**
     * @param name          the column name 
     * @param keysClass     the class for the key element
     * @param valuesClass   the class for the value element
     * @param <K> the member key type
     * @param <V> the member value type
     * @return  the value of column name as a map
     */
    <K, V> Optional<ImmutableMap<K, V>> getMap(String name, Class<K> keysClass, Class<V> valuesClass);
}
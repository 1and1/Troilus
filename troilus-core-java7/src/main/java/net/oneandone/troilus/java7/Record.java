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
package net.oneandone.troilus.java7;


import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.UUID;

import net.oneandone.troilus.ColumnName;
import net.oneandone.troilus.Result;

import com.datastax.driver.core.TupleValue;
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
    Long getWritetime(String name);
    
    /**
     * @param name  the column name 
     * @return the ttl of this column or empty if no ttl was requested or NULL
     */
    Integer getTtl(String name);
    
    /**
     * @param name  the column name 
     * @return whether the value for column name in this row is NULL
     */
    boolean isNull(String name);
    
     
    /**
     * @param name the column name 
     * @return the value of column name as a long. If the value is NULL, 0L is returned.
     */
    long getLong(String name);
    
    /**
     * @param name the column name 
     * @return the value of column name as a string or null
     */
    String getString(String name);
    
    /**
     * @param name the column name 
     * @return the value of column name as a boolean. If the value is NULL, false is returned.
     */
    boolean getBool(String name);
    
    /**
     * @param name the column name 
     * @return the value of column name as a ByteBuffer or null
     */
    ByteBuffer getBytes(String name);
     
    /**
     * @param name the column name 
     * @return the value of column name as a ByteBuffer or null. This method always return the bytes composing the value, even if the column is not of type BLOB
     */
    ByteBuffer getBytesUnsafe(String name);
    
    /**
     * @param name the column name 
     * @return value of column name as a float. If the value is NULL, 0.0f is returned.
     */
    float getFloat(String name);
    
    /**
     * @param name the column name 
     * @return value of column name as a date. If the value is NULL, null is returned.
     */
    Date getDate(String name);
     
    
    /**
     * @param name the column name 
     * @return value of column name as time. If the value is NULL, 0 is returned.
     */
    long getTime(String name);

    
    /**
     * @param name the column name 
     * @return the value of column name as a decimal. If the value is NULL, null is returned
     */
    BigDecimal getDecimal(String name);
    
    /**
     * @param name the column name 
     * @return value of column name as an integer. If the value is NULL, 0 is returned
     */
    int getInt(String name);
    
    /**
     * @param name the column name 
     * @return value of column name as an InetAddress. If the value is NULL, null is returned.
     */
    InetAddress getInet(String name);
     
    /**
     * @param name the column name 
     * @return the value of column name as a variable length integer. If the value is NULL, null is returned.
     */
    BigInteger getVarint(String name);
  
  
    /**
     * @param name the column name 
     * @return the value of column name as a UUID. If the value is NULL, null is returned.
     */
    UUID getUUID(String name);
   
    /**
     * @param name the name to retrieve.
     * @return the value of {@code name} as a tuple value. If the value is NULL, null will be returned.
     */
    TupleValue getTupleValue(String name);
    
    /**
     * @param name the column name 
     * @return the value for column name as a UDT value. If the value is NULL, then null will be returned.
     */
    UDTValue getUDTValue(String name);
    
 
    /**
     * @param name      the column name 
     * @param enumType  the enum type
     * @param <T>       the enum type class
     * @return the value for column name as an enum value or null
     */
    <T extends Enum<T>> T getEnum(String name, Class<T> enumType);
        
    /**
     * @param name the column name
     * @param <T>  the name type 
     * @return the value for column name as an value type according the name definition or null
     */
    <T> T getValue(ColumnName<T> name);
   
    /**
     * @param name   the column name 
     * @param type   the class for value to retrieve.
     * @param <T>    the result type
     * @return  the value of column name or null
     */
    <T> T getValue(String name, Class<T> type);    
    
    /**
     * @param name           the column name 
     * @param elementsClass  the class for the elements of the set to retrieve
     * @param <T>            the set member type
     * @return  the value of column name as a set of an empty Set
     */
    <T> ImmutableSet<T> getSet(String name, Class<T> elementsClass);
         
    /**
     * @param name          the column name 
     * @param elementsClass the class for the elements of the list to retrieve
     * @param <T>           the list member type 
     * @return  the value of column name as a list or an empty List
     */
    <T> ImmutableList<T> getList(String name, Class<T> elementsClass);
    
    /**
     * @param name          the column name 
     * @param keysClass     the class for the key element
     * @param valuesClass   the class for the value element
     * @param <K>  the member key type
     * @param <V>  the member key value 
     * @return  the value of column name as a map or an empty Map
     */
    <K, V> ImmutableMap<K, V> getMap(String name, Class<K> keysClass, Class<V> valuesClass);
}
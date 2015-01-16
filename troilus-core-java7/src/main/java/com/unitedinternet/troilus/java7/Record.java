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
package com.unitedinternet.troilus.java7;


import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.UUID;

import com.datastax.driver.core.UDTValue;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.unitedinternet.troilus.Name;
import com.unitedinternet.troilus.Result;



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
     * @return the ttl of this column or empty if no ttl was requested or <code>null</code>
     */
    Integer getTtl(String name);
    
    /**
     * @param name  the column name 
     * @return whether the value for column name in this row is NULL
     */
    boolean isNull(String name);
    
     
    /**
     * @param name the column name 
     * @return the value of column name as a long
     */
    Long getLong(String name);
    
    /**
     * @param name the column name 
     * @return the value of column name as a string
     */
    String getString(String name);
    
    /**
     * @param name the column name 
     * @return the value of column name as a boolean
     */
    Boolean getBool(String name);
    
    /**
     * @param name the column name 
     * @return the value of column name as a ByteBuffer
     */
    ByteBuffer getBytes(String name);
     
    /**
     * @param name the column name 
     * @return the value of column name as a ByteBuffer. This method always return the bytes composing the value, even if the column is not of type BLOB
     */
    ByteBuffer getBytesUnsafe(String name);
    
    /**
     * @param name the column name 
     * @return value of column name as a float
     */
    Float getFloat(String name);
    
    /**
     * @param name the column name 
     * @return value of column name as a date
     */
    Date getDate(String name);
     
    /**
     * @param name the column name 
     * @return the value of column name as a dedcimal
     */
    BigDecimal getDecimal(String name);
    
    /**
     * @param name the column name 
     * @return value of column name as an integer
     */
    Integer getInt(String name);
    
    /**
     * @param name the column name 
     * @return value of column name as an InetAddress
     */
    InetAddress getInet(String name);
     
    /**
     * @param name the column name 
     * @return the value of column name as a variable length integer
     */
    BigInteger getVarint(String name);
  
  
    /**
     * @param name the column name 
     * @return the value of column name as a UUID
     */
    UUID getUUID(String name);
   
    /**
     * @param name the column name 
     * @return the value for column name as a UDT value
     */
    UDTValue getUDTValue(String name);
    
 
    /**
     * @param name      the column name 
     * @param enumType  the enum type
     * @return the value for column name as an enum value
     */
    <T extends Enum<T>> T getEnum(String name, Class<T> enumType);
        
    /**
     * @param name the column name 
     * @return the value for column name as an value type according the name definition
     */
    <T> T getValue(Name<T> name);
   
    /**
     * @param name   the column name 
     * @param typ    the class for value to retrieve.
     * @return  the value of column name 
     */
    <T> T getObject(String name, Class<T> type);    
    
    /**
     * @param name           the column name 
     * @param elementsClass  the class for the elements of the set to retrieve
     * @return  the value of column name as a set
     */
    <T> ImmutableSet<T> getSet(String name, Class<T> elementsClass);
         
    /**
     * @param name          the column name 
     * @param elementsClass the class for the elements of the list to retriev
     * @return  the value of column name as a list
     */
    <T> ImmutableList<T> getList(String name, Class<T> elementsClass);
    
    /**
     * @param name          the column name 
     * @param keysClass     the class for the key element
     * @param valuesClass   the class for the value element
     * @return  the value of column name as a map
     */
    <K, V> ImmutableMap<K, V> getMap(String name, Class<K> keysClass, Class<V> valuesClass);
}
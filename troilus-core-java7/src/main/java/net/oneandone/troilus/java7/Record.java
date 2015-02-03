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
     * @return the value of column name as a long or <code>null<code>
     */
    Long getLong(String name);
    
    /**
     * @param name the column name 
     * @return the value of column name as a string or <code>null<code>
     */
    String getString(String name);
    
    /**
     * @param name the column name 
     * @return the value of column name as a boolean or <code>null<code>
     */
    Boolean getBool(String name);
    
    /**
     * @param name the column name 
     * @return the value of column name as a ByteBuffer or <code>null<code>
     */
    ByteBuffer getBytes(String name);
     
    /**
     * @param name the column name 
     * @return the value of column name as a ByteBuffer or <code>null<code>. This method always return the bytes composing the value, even if the column is not of type BLOB
     */
    ByteBuffer getBytesUnsafe(String name);
    
    /**
     * @param name the column name 
     * @return value of column name as a float or <code>null<code>
     */
    Float getFloat(String name);
    
    /**
     * @param name the column name 
     * @return value of column name as a date or <code>null<code>
     */
    Date getDate(String name);
     
    /**
     * @param name the column name 
     * @return the value of column name as a dedcimal or <code>null<code>
     */
    BigDecimal getDecimal(String name);
    
    /**
     * @param name the column name 
     * @return value of column name as an integer or <code>null<code>
     */
    Integer getInt(String name);
    
    /**
     * @param name the column name 
     * @return value of column name as an InetAddress or <code>null<code>
     */
    InetAddress getInet(String name);
     
    /**
     * @param name the column name 
     * @return the value of column name as a variable length integer or <code>null<code>
     */
    BigInteger getVarint(String name);
  
  
    /**
     * @param name the column name 
     * @return the value of column name as a UUID or <code>null<code>
     */
    UUID getUUID(String name);
   
    /**
     * @param name the column name 
     * @return the value for column name as a UDT value or <code>null<code>
     */
    UDTValue getUDTValue(String name);
    
 
    /**
     * @param name      the column name 
     * @param enumType  the enum type
     * @param <T>       the enum type class
     * @return the value for column name as an enum value or <code>null<code>
     */
    <T extends Enum<T>> T getEnum(String name, Class<T> enumType);
        
    /**
     * @param name the column name
     * @param <T>  the name type 
     * @return the value for column name as an value type according the name definition or <code>null<code>
     */
    <T> T getValue(ColumnName<T> name);
   
    /**
     * @param name   the column name 
     * @param type   the class for value to retrieve.
     * @param <T>    the result type
     * @return  the value of column name or <code>null<code>
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
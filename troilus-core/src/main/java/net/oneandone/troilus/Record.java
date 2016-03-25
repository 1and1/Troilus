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
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.UUID;

import net.oneandone.troilus.ColumnName;
import net.oneandone.troilus.Result;

import com.datastax.driver.core.GettableByNameData;
import com.datastax.driver.core.TupleValue;
import com.datastax.driver.core.UDTValue;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;


/**
 * record result
 */
public interface Record extends GettableByNameData, Result {
   
    /**
     * @param name  the column name 
     * @return the write time of this column or empty if no write time was requested
     */
    default long getWritetime(final String name) {
        Long value = getLong("WRITETIME(" + name + ")");
        Preconditions.checkNotNull(value);
        return value;
    }

    /**
     * @param name  the column name 
     * @return the ttl of this column or empty if no ttl was requested or <code>null</code>
     */
    default Duration getTtl(final String name) {
        try {
            return Duration.ofSeconds(getInt("TTL(" + name + ")"));
        } catch (IllegalArgumentException iae) {
            return null;
        }
    }

    /**
     * @param name  the column name
     * @param dflt  the value which will be returned, if the database value is null 
     * @return the ttl of this column or default
     */
    default Duration getTtl(final String name, final Duration dflt) {
        Duration value = getTtl(name);
        return (value == null) ? dflt : value;
    }

    /**
     * @param name  the column name
     * @param dflt  the value which will be returned, if the database value is null
     * @return the value of column name as a byte.
     */
    default long getByte(final String name, final byte dflt) {
        return isNull(name) ? dflt : getByte(name);
    }

    /**
     * @param name  the column name
     * @param dflt  the value which will be returned, if the database value is null
     * @return the value of column name as a long.
     */
    default long getLong(final String name, final long dflt) {
        return isNull(name) ? dflt : getLong(name);
    }
    
    /**
     * @param name   the column name
     * @param dflt   the value which will be returned, if the database value is null
     * @return the value of column name as a double.
     */
    default double getDouble(final String name, final double dflt) {
        return isNull(name) ? dflt : getDouble(name);
    }
    
    /**
     * @param name  the column name 
     * @param dflt  the value which will be returned, if the database value is null 
     * @return the value of column name as a string or null
     */
    default String getString(final String name, final String dflt) {
        Preconditions.checkNotNull(dflt);
        return isNull(name) ? dflt : getString(name);
    }
    
    /**
     * @param name  the column name
     * @param dflt  the value which will be returned, if the database value is null
     * @return the value of column name as a boolean
     */
    default boolean getBool(final String name, final boolean dflt) {
        return isNull(name) ? dflt : getBool(name);
    }
    
    /**
     * @param name  the column name 
     * @param dflt  the value which will be returned, if the database value is null 
     * @return the value of column name as a ByteBuffer 
     */
    default ByteBuffer getBytes(final String name, final ByteBuffer dflt) {
        Preconditions.checkNotNull(dflt);
        return isNull(name) ? dflt : getBytes(name);
    }
     
    /**
     * @param name  the column name 
     * @param dflt  the value which will be returned, if the database value is null 
     * @return the value of column name as a ByteBuffer. This method always return the bytes composing the value, even if the column is not of type BLOB
     */
    default ByteBuffer getBytesUnsafe(final String name, final ByteBuffer dflt) {
        Preconditions.checkNotNull(dflt);
        return isNull(name) ? dflt : getBytesUnsafe(name);
    }
    
    /**
     * @param name  the column name 
     * @param dflt  the value which will be returned, if the database value is null 
     * @return value of column name as a float.
     */
    default float getFloat(final String name, final float dflt) {
        return isNull(name) ? dflt : getFloat(name);
    }

    /**
     * @param name  the column name 
     * @param dflt  the value which will be returned, if the database value is null 
     * @return value of column name as a local date.
     */
    default LocalDate getLocalDate(final String name) {
        return get(name, LocalDateCodec.instance);
    }

    /**
     * @param name  the column name 
     * @param dflt  the value which will be returned, if the database value is null 
     * @return value of column name as a local date.
     */
    default LocalDate getLocalDate(final String name, final LocalDate dflt) {
        final LocalDate value = getLocalDate(name);
        return isNull(name) ? dflt : value;
    }
    
    /**
     * @param name  the column name 
     * @param dflt  the value which will be returned, if the database value is null 
     * @return value of column name as time.
     */
    default long getTime(final String name, final long dflt) {
        return isNull(name) ? dflt : getTime(name);
    }
    
    /**
     * @param name  the column name 
     * @param dflt  the value which will be returned, if the database value is null 
     * @return the value of column name as a decimal. If the value is NULL, null is returned
     */
    default BigDecimal getDecimal(final String name, final BigDecimal dflt) {
        Preconditions.checkNotNull(dflt);
        return isNull(name) ? dflt : getDecimal(name);
    }
    
    
    /**
     * @param name  the column name 
     * @param dflt  the value which will be returned, if the database value is null 
     * @return value of column name as an integer.
     */
    default int getInt(final String name, final int dflt) {
        return isNull(name) ? dflt : getInt(name);
    }
    
    /**
     * @param name  the column name 
     * @param dflt  the value which will be returned, if the database value is null 
     * @return value of column name as an InetAddress. 
     */
    default InetAddress getInet(final String name, final InetAddress dflt) {
        Preconditions.checkNotNull(dflt);
        return isNull(name) ? dflt : getInet(name);
    }
     
    /**
     * @param name  the column name 
     * @param dflt  the value which will be returned, if the database value is null 
     * @return the value of column name as a variable length integer. If the value is NULL, null is returned.
     */
    default BigInteger getVarint(final String name, final BigInteger dflt) {
        Preconditions.checkNotNull(dflt);
        return isNull(name) ? dflt : getVarint(name);
    }
  
    /**
     * @param name  the column name
     * @param dflt  the value which will be returned, if the database value is null   
     * @return the value of column name as a UUID.
     */
    default UUID getUUID(final String name, final UUID dflt) {
        Preconditions.checkNotNull(dflt);
        return isNull(name) ? dflt : getUUID(name);
    }
    
    /**
     * @param name  the name to retrieve.
     * @param dflt  the value which will be returned, if the database value is null 
     * @return the value of {@code name} as a tuple value. 
     */
    default TupleValue getTupleValue(final String name, final TupleValue dflt) {
        Preconditions.checkNotNull(dflt);
        return isNull(name) ? dflt : getTupleValue(name);
    }
   
    /**
     * @param name  the column name 
     * @param dflt  the value which will be returned, if the database value is null 
     * @return the value for column name as a UDT value. 
     */
    default UDTValue getUDTValue(final String name, final UDTValue dflt) {
        Preconditions.checkNotNull(dflt);
        return isNull(name) ? dflt : getUDTValue(name);
    }
     
    /**
     * @param name the column name 
     * @return the value for column name as an Instant value. If the value is NULL, then null will be returned.
     */
    default Instant getInstant(final String name) {
        return get(name, InstantCodec.instance);
    }
    
    /**
     * @param name  the column name 
     * @param dflt  the value which will be returned, if the database value is null
     * @return the value for column name as an Instant value. 
     */
    default Instant getInstant(final String name, final Instant dflt) {
        Preconditions.checkNotNull(dflt);
        return isNull(name) ? dflt : getInstant(name);
    }
    
    /**
     * @param name      the column name 
     * @param enumType  the enum type
     * @param <T> the enum type class
     * @return the value for column name as an enum value
     */
    <T extends Enum<T>> T getEnum(String name, Class<T> enumType);
        
    /**
     * @param name the column name 
     * @param <T> the name type
     * @return the value for column name as an value type according the name definition
     */
    <T> T getValue(ColumnName<T> name);
    
    /**
     * @param name  the column name 
     * @param <T>   the name type
     * @param dflt  the value which will be returned, if the database value is null  
     * @return the value for column name as an value type according the name definition
     */
    default <T> T getValue(final ColumnName<T> name, final T dflt) {
        Preconditions.checkNotNull(dflt);
        return isNull(name.getName()) ? dflt : getValue(name);
    }
   
    /**
     * @param name   the column name 
     * @param type   the class for value to retrieve.
     * @param <T>  the object type
     * @return  the value of column name 
     */
    <T> T getValue(String name, Class<T> type);    

    /**
     * @param name   the column name 
     * @param type   the class for value to retrieve.
     * @param dflt   the value which will be returned, if the database value is null
     * @param <T>    the object type
     * @return  the value of column name 
     */
    default <T> T getValue(final String name, final Class<T> type, final T dflt) {
        Preconditions.checkNotNull(dflt);
        return isNull(name) ? dflt : getValue(name, type);
    }
    
    @Override
    <T> ImmutableSet<T> getSet(String name, Class<T> elementsClass);
    
    /**
     * @param name           the column name 
     * @param elementsClass  the class for the elements of the set to retrieve
     * @param dflt           the value which will be returned, if the database value is null   
     * @param <T>  the element type
     * @return  the value of column name as a set
     */
    default <T> ImmutableSet<T> getSet(final String name, final Class<T> elementsClass, final ImmutableSet<T> dflt) {
        Preconditions.checkNotNull(dflt);
        return isNull(name) ? dflt : getSet(name, elementsClass); 
    }
         
    @Override
    <T> ImmutableList<T> getList(String name, Class<T> elementsClass);
    
    /**
     * @param name          the column name 
     * @param elementsClass the class for the elements of the list to retrieve
     * @param dflt           the value which will be returned, if the database value is null    
     * @param <T> the element type
     * @return  the value of column name as a list
     */
    default <T> ImmutableList<T> getList(final String name, final Class<T> elementsClass, final ImmutableList<T> dflt) {
        Preconditions.checkNotNull(dflt);
        return isNull(name) ? dflt : getList(name, elementsClass);
    }
    
    @Override
    <K, V> ImmutableMap<K, V> getMap(String name, Class<K> keysClass, Class<V> valuesClass);
    
    /**
     * @param name          the column name 
     * @param keysClass     the class for the key element
     * @param valuesClass   the class for the value element
     * @param dflt          the value which will be returned, if the database value is null    
     * @param <K> the member key type
     * @param <V> the member value type
     * @return  the value of column name as a map
     */
    default <K, V> ImmutableMap<K, V> getMap(final String name, final Class<K> keysClass, final Class<V> valuesClass, final ImmutableMap<K, V> dflt) {
        Preconditions.checkNotNull(dflt);
        return isNull(name) ? dflt : getMap(name, keysClass, valuesClass);
    }
}
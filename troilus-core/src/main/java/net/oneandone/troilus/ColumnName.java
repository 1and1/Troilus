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
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.datastax.driver.core.TupleValue;
import com.google.common.collect.ImmutableList;


 
/**
 * A name which defines the class type of the associated value type 
 *
 * @param <T> the value type
 */
public abstract class ColumnName<T> {
 
    private final String name;
    
    private ColumnName(String name) { 
        this.name = name;
    }
    
    
    /**
     * @return the name
     */
    public String getName() { 
        return name;
    }


    abstract T read(Record record);
    
    
    
    
    
    /**
     * defines a new name 
     * 
     * @param name        the name 
     * @param type        the value type
     * @param <E>         the name type
     * @return a new instance
     */
    public static <E> ColumnName<E> define(String name, Class<E> type) {
        return new SkalarName<>(name, type);
    }

    
    /**
     * defines a new list name 
     * 
     * @param name          the name 
     * @param elementType   the list member value type
     * @param <E>           the name type
     * @return a new instance
     */
    public static <E> ColumnName<List<E>> defineList(String name, Class<E> elementType) {
        return new ListName<>(name, elementType);
    }

    

    /**
     * defines a new set name 
     * 
     * @param name          the name 
     * @param elementType   the set member value type
     * @param <E>           the name type
     * @return a new instance
     */
    public static <E> ColumnName<Set<E>> defineSet(String name, Class<E> elementType) {
        return new SetName<>(name, elementType);
    }
        
    
    /**
     * defines a new set name 
     * 
     * @param name      the name 
     * @param keyType   the set member key type
     * @param valueType the set member value type
     * @param <E>       the member key name type
     * @param <F>       the member value type
     * @return a new instance
     */
    public static <E, F> ColumnName<Map<E, F>> defineMap(String name, Class<E> keyType,  Class<F> valueType) {
        return new MapName<>(name, keyType, valueType);
    }

 
    /**
     * defines a new name with Long-typed value
     * 
     * @param name        the name 
     * @return a new instance
     */
    public static ColumnName<Long> defineLong(String name) {
        return define(name, Long.class);
    }

    /**
     * defines a new name with String-typed value
     * 
     * @param name        the name 
     * @return a new instance
     */
    public static ColumnName<String> defineString(String name) {
        return define(name, String.class);
    }
    
    /**
     * defines a new name with Boolean-typed value
     * 
     * @param name        the name 
     * @return a new instance
     */
    public static ColumnName<Boolean> defineBool(String name) {
        return define(name, Boolean.class);
    }
    
    /**
     * defines a new name with ByteBuffer-typed value
     * 
     * @param name        the name 
     * @return a new instance
     */
    public static ColumnName<ByteBuffer> defineByteBuffer(String name) {
        return define(name, ByteBuffer.class);
    }
    
    /**
     * defines a new name with Float-typed value
     * 
     * @param name        the name 
     * @return a new instance
     */
    public static ColumnName<Float> defineFloat(String name) {
        return define(name, Float.class);
    }
   
    /**
     * defines a new name with Date-typed value
     * 
     * @param name        the name 
     * @return a new instance
     */
    public static ColumnName<Date> defineDate(String name) {
        return define(name, Date.class);
    }
    
    /**
     * defines a new name with Decimal-typed value
     * 
     * @param name   the name 
     * @return a new instance
     */
    public static ColumnName<BigDecimal> defineDecimal(String name) {
        return define(name, BigDecimal.class);
    }
    
    /**
     * defines a new name with Integer-typed value
     * 
     * @param name        the name 
     * @return a new instance
     */
    public static ColumnName<Integer> defineInt(String name) {
        return define(name, Integer.class);
    }
    
    /**
     * defines a new name with InetAddress-typed value
     * 
     * @param name        the name 
     * @return a new instance
     */
    public static ColumnName<InetAddress> defineInet(String name) {
        return define(name, InetAddress.class);
    }
    
    /**
     * defines a new name with Varint-typed value
     * 
     * @param name        the name 
     * @return a new instance
     */
    public static ColumnName<BigInteger> defineVarint(String name) {
        return define(name, BigInteger.class);
    }
    
    /**
     * defines a new name with TupleValue-typed value
     * 
     * @param name        the name 
     * @return a new instance
     */
    public static ColumnName<TupleValue> defineTupleValue(String name) {
        return define(name, TupleValue.class);
    }
    
    /**
     * defines a new name with UUID-typed value
     * 
     * @param name        the name 
     * @return a new instance
     */
    public static ColumnName<UUID> defineUUID(String name) {
        return define(name, UUID.class);
    }
    

    /**
     * defines a new name with ByteBuffer-typed value
     * 
     * @param name        the name 
     * @return a new instance
     */
    public static ColumnName<byte[]> defineBytes(String name) {
        return define(name, byte[].class);
    }
    
    
    private static class SkalarName<T> extends ColumnName<T> {
        private final Class<T> type;
        
        
        /**
         * @param name      the name
         * @param type      the value type
         */
        SkalarName(String name, Class<T> type) {
            super(name);
            this.type = type;
        }
        
        @SuppressWarnings("unchecked")
        @Override
        T read(Record record) {
            return (T) record.getValue(getName(), (Class<Object>) type);
        }
    }
    

    
    /**
     * A list name which defines the class type of the associated list member value type 
     *
     * @param <T> the list member value type
     */
    private static class ListName<T> extends ColumnName<List<T>> {
        private final Class<T> elementType;
        
        /**
         * @param name          the name
         * @param elementType   the list member value type
         */
        private ListName(String name, Class<T> elementType) {
            super(name);
            this.elementType = elementType;
        }
        
        @Override
        ImmutableList<T> read(Record record) {
            return record.getList(getName(), (Class<T>) elementType);
        }
    }
    
    

     
    /**
     * A set name which defines the class type of the associated set member value type 
     *
     * @param <T> the set member value type
     */
    private static class SetName<T> extends ColumnName<Set<T>> {
        private final Class<T> elementType;

        /**
         * @param name          the name
         * @param elementType   the set member value type
         */
        private SetName(String name, Class<T> elementType) {
            super(name);
            this.elementType = elementType;
        }
        
        @Override
        Set<T> read(Record record) {
            return record.getSet(getName(), (Class<T>) elementType);
        }
    } 
    
    
    /**
     * A set name which defines the class type of the associated set member value type 
     *
     * @param <T> the set member key type
     * @param <V> the set member value type
     */
    private static class MapName<T, V> extends ColumnName<Map<T, V>> {
        
        private final Class<T> keyType;
        private final Class<V> valueType;
        
        
        /**
         * @param name      the name
         * @param keyType   the set member value type
         * @param valueType the set member value type 
         */
        private MapName(String name, Class<T> keyType, Class<V> valueType) {
            super(name);
            this.keyType = keyType;
            this.valueType = valueType;
        }
        
        @Override
        Map<T,V> read(Record record) {
            return record.getMap(getName(), (Class<T>) keyType, (Class<V>) valueType);
        }
    } 
}
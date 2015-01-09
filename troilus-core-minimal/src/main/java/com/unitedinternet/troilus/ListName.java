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
import java.time.Instant;
import java.util.Date;
import java.util.UUID;

import com.datastax.driver.core.TupleValue;


 
/**
 * A list name which defines the class type of the associated list member value type 
 *
 * @param <T> the list member value type
 */
public class ListName<T> extends Name<T> {
    
     
    /**
     * @param name   the name
     * @param type   the list member value type
     */
    private ListName(String name, Class<T> type) {
        super(name, type);
    }
    
    
    
    /**
     * defines a new list name 
     * 
     * @param name   the name 
     * @param type   the list member value type
     * @return a new instance
     */
    public static <E> ListName<E> define(String name, Class<E> type) {
        return new ListName<>(name, type);
    }


    /**
     * defines a new list name with Long-typed member value
     * 
     * @param name   the list name 
     * @return a new instance
     */
    public static ListName<Long> defineLong(String name) {
        return define(name, Long.class);
    }

    /**
     * defines a new list name with String-typed member value
     * 
     * @param name   the list name 
     * @return a new instance
     */
    public static ListName<String> defineString(String name) {
        return define(name, String.class);
    }
    
    /**
     * defines a new list name with Boolean-typed member value
     * 
     * @param name   the list name 
     * @return a new instance
     */
    public static ListName<Boolean> defineBool(String name) {
        return define(name, Boolean.class);
    }
    
    /**
     * defines a new list name with ByteBuffer-typed member value
     * 
     * @param name   the list name 
     * @return a new instance
     */
    public static ListName<ByteBuffer> defineByte(String name) {
        return define(name, ByteBuffer.class);
    }
   
    /**
     * defines a new list name with Float-typed member value
     * 
     * @param name   the list name 
     * @return a new instance
     */
    public static ListName<Float> defineFloat(String name) {
        return define(name, Float.class);
    }
   
    /**
     * defines a new list name with Date-typed member value
     * 
     * @param name   the list name 
     * @return a new instance
     */
    public static ListName<Date> defineDate(String name) {
        return define(name, Date.class);
    }
    
    /**
     * defines a new list name with Decimal-typed member value
     * 
     * @param name   the list name 
     * @return a new instance
     */
    public static ListName<BigDecimal> defineDecimal(String name) {
        return define(name, BigDecimal.class);
    }
    
    /**
     * defines a new list name with Integer-typed member value
     * 
     * @param name   the list name 
     * @return a new instance
     */
    public static ListName<Integer> defineInt(String name) {
        return define(name, Integer.class);
    }
    
    /**
     * defines a new list name with InetAddress-typed member value
     * 
     * @param name   the list name 
     * @return a new instance
     */
    public static ListName<InetAddress> defineInet(String name) {
        return define(name, InetAddress.class);
    }
    
    /**
     * defines a new list name with Varint-typed member value
     * 
     * @param name   the list name 
     * @return a new instance
     */
    public static ListName<BigInteger> defineVarint(String name) {
        return define(name, BigInteger.class);
    }
    
    /**
     * defines a new list name with TupleValue-typed member value
     * 
     * @param name   the list name 
     * @return a new instance
     */
    public static ListName<TupleValue> defineTupleValue(String name) {
        return define(name, TupleValue.class);
    }
    
    /**
     * defines a new list name with UUID-typed member value
     * 
     * @param name   the list name 
     * @return a new instance
     */
    public static ListName<UUID> defineUUID(String name) {
        return define(name, UUID.class);
    }
    
    /**
     * defines a new list name with Instant-typed member value
     * 
     * @param name   the list name 
     * @return a new instance
     */
    public static ListName<Instant> defineInstant(String name) {
        return define(name, Instant.class);
    }
} 
   
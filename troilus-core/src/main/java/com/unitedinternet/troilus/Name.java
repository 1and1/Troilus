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
import java.time.Instant;
import java.util.Date;
import java.util.UUID;

import com.datastax.driver.core.TupleValue;


 
/**
 * A name which defines the class type of the associated value type 
 *
 * @param <T> the value type
 */
public class Name<T> {
    
    private final String name;
    private final Class<T> type;
    
    
    /**
     * @param name   the name
     * @param type   the value type
     */
    Name(String name, Class<T> type) {
        this.name = name;
        this.type = type;
    }
    
    
    /**
     * defines a new name 
     * 
     * @param name   the name 
     * @param type   the value type
     * @return a new instance
     */
    public static <E> Name<E> define(String name, Class<E> type) {
        return new Name<>(name, type);
    }


    /**
     * defines a new name with Long-typed value
     * 
     * @param name   the name 
     * @return a new instance
     */
    public static Name<Long> defineLong(String name) {
        return define(name, Long.class);
    }

    /**
     * defines a new name with String-typed value
     * 
     * @param name   the name 
     * @return a new instance
     */
    public static Name<String> defineString(String name) {
        return define(name, String.class);
    }
    
    /**
     * defines a new name with Boolean-typed value
     * 
     * @param name   the name 
     * @return a new instance
     */
    public static Name<Boolean> defineBool(String name) {
        return define(name, Boolean.class);
    }
    
    /**
     * defines a new name with ByteBuffer-typed value
     * 
     * @param name   the name 
     * @return a new instance
     */
    public static Name<ByteBuffer> defineByte(String name) {
        return define(name, ByteBuffer.class);
    }
   
    /**
     * defines a new name with Float-typed value
     * 
     * @param name   the name 
     * @return a new instance
     */
    public static Name<Float> defineFloat(String name) {
        return define(name, Float.class);
    }
   
    /**
     * defines a new name with Date-typed value
     * 
     * @param name   the name 
     * @return a new instance
     */
    public static Name<Date> defineDate(String name) {
        return define(name, Date.class);
    }
    
    /**
     * defines a new name with Decimal-typed value
     * 
     * @param name   the name 
     * @return a new instance
     */
    public static Name<BigDecimal> defineDecimal(String name) {
        return define(name, BigDecimal.class);
    }
    
    /**
     * defines a new name with Integer-typed value
     * 
     * @param name   the name 
     * @return a new instance
     */
    public static Name<Integer> defineInt(String name) {
        return define(name, Integer.class);
    }
    
    /**
     * defines a new name with InetAddress-typed value
     * 
     * @param name   the name 
     * @return a new instance
     */
    public static Name<InetAddress> defineInet(String name) {
        return define(name, InetAddress.class);
    }
    
    /**
     * defines a new name with Varint-typed value
     * 
     * @param name   the name 
     * @return a new instance
     */
    public static Name<BigInteger> defineVarint(String name) {
        return define(name, BigInteger.class);
    }
    
    /**
     * defines a new name with TupleValue-typed value
     * 
     * @param name   the name 
     * @return a new instance
     */
    public static Name<TupleValue> defineTupleValue(String name) {
        return define(name, TupleValue.class);
    }
    
    /**
     * defines a new name with UUID-typed value
     * 
     * @param name   the name 
     * @return a new instance
     */
    public static Name<UUID> defineUUID(String name) {
        return define(name, UUID.class);
    }
    
    /**
     * defines a new name with Instant-typed value
     * 
     * @param name   the name 
     * @return a new instance
     */
    public static Name<Instant> defineInstant(String name) {
        return define(name, Instant.class);
    }
    
    /**
     * @return the name
     */
    String getName() {
        return name;
    }
    
    /**
     * @return the value type
     */
    Class<T> getType() {
        return type;
    }
}
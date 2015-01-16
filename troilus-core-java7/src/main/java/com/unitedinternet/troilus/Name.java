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
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.datastax.driver.core.TupleValue;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;


 
/**
 * A name which defines the class type of the associated value type 
 *
 * @param <T> the value type
 */
public abstract class Name<T> {
 
    private final String name;
    private final ImmutableList<ValueValidator> validators;
    
    private Name(String name, ValueValidator... validators) { 
        this.name = name;
        this.validators = ImmutableList.copyOf(validators);
    }
    
    
    /**
     * @return the name
     */
    public String getName() { 
        return name;
    }


    /**
     * @param value the value to convert
     * @return the converted value
     */
    Object convertWrite(T value) {
        return value; 
    }
    
    
    /**
     * @param value the value to validate
     */
    void vaildate(T value) {
        for (ValueValidator validator : validators) {
            if (!validator.isValid(value)) {
                throw new ConstraintException(value + " violates " + validator);
            }
        }
    }
    
    
    abstract Optional<T> read(PropertiesSource propertiesSource);
    
    
    
    
    
    /**
     * defines a new name 
     * 
     * @param name        the name 
     * @param type        the value type
     * @param validators  the validators
     * @return a new instance
     */
    public static <E> Name<E> define(String name, Class<E> type, ValueValidator... validators) {
        return new SkalarName<>(name, type, validators);
    }

    
    /**
     * defines a new list name 
     * 
     * @param name          the name 
     * @param elementType   the list member value type
     * @param validators    the validators 
     * @return a new instance
     */
    public static <E> Name<List<E>> defineList(String name, Class<E> elementType, ValueValidator... validators) {
        return new ListName<>(name, elementType);
    }

    

    /**
     * defines a new set name 
     * 
     * @param name          the name 
     * @param elementType   the set member value type
     * @param validators    the validators 
     * @return a new instance
     */
    public static <E> Name<Set<E>> defineSet(String name, Class<E> elementType, ValueValidator... validators) {
        return new SetName<>(name, elementType);
    }
        
    
    /**
     * defines a new set name 
     * 
     * @param name      the name 
     * @param keyType   the set member key type
     * @param valueType the set member value type
     * @return a new instance
     */
    public static <E, F> Name<Map<E, F>> defineMap(String name, Class<E> keyType,  Class<F> valueType) {
        return new MapName<>(name, keyType, valueType);
    }

 
    /**
     * defines a new name with Long-typed value
     * 
     * @param name        the name 
     * @param validators  the validators 
     * @return a new instance
     */
    public static Name<Long> defineLong(String name, ValueValidator... validators) {
        return define(name, Long.class, validators);
    }

    /**
     * defines a new name with String-typed value
     * 
     * @param name        the name 
     * @param validators  the validators 
     * @return a new instance
     */
    public static Name<String> defineString(String name, ValueValidator... validators) {
        return define(name, String.class, validators);
    }
    
    /**
     * defines a new name with Boolean-typed value
     * 
     * @param name        the name 
     * @param validators  the validators 
     * @return a new instance
     */
    public static Name<Boolean> defineBool(String name, ValueValidator... validators) {
        return define(name, Boolean.class, validators);
    }
    
    /**
     * defines a new name with ByteBuffer-typed value
     * 
     * @param name        the name 
     * @param validators  the validators 
     * @return a new instance
     */
    public static Name<ByteBuffer> defineByteBuffer(String name, ValueValidator... validators) {
        return define(name, ByteBuffer.class, validators);
    }
    
    /**
     * defines a new name with Float-typed value
     * 
     * @param name        the name 
     * @param validators  the validators 
     * @return a new instance
     */
    public static Name<Float> defineFloat(String name, ValueValidator... validators) {
        return define(name, Float.class, validators);
    }
   
    /**
     * defines a new name with Date-typed value
     * 
     * @param name        the name 
     * @param validators  the validators 
     * @return a new instance
     */
    public static Name<Date> defineDate(String name, ValueValidator... validators) {
        return define(name, Date.class, validators);
    }
    
    /**
     * defines a new name with Decimal-typed value
     * 
     * @param name   the name 
     * @param validators  the validators 
     * @return a new instance
     */
    public static Name<BigDecimal> defineDecimal(String name, ValueValidator... validators) {
        return define(name, BigDecimal.class, validators);
    }
    
    /**
     * defines a new name with Integer-typed value
     * 
     * @param name        the name 
     * @param validators  the validators 
     * @return a new instance
     */
    public static Name<Integer> defineInt(String name, ValueValidator... validators) {
        return define(name, Integer.class, validators);
    }
    
    /**
     * defines a new name with InetAddress-typed value
     * 
     * @param name        the name 
     * @param validators  the validators 
     * @return a new instance
     */
    public static Name<InetAddress> defineInet(String name, ValueValidator... validators) {
        return define(name, InetAddress.class, validators);
    }
    
    /**
     * defines a new name with Varint-typed value
     * 
     * @param name        the name 
     * @param validators  the validators 
     * @return a new instance
     */
    public static Name<BigInteger> defineVarint(String name, ValueValidator... validators) {
        return define(name, BigInteger.class, validators);
    }
    
    /**
     * defines a new name with TupleValue-typed value
     * 
     * @param name        the name 
     * @param validators  the validators 
     * @return a new instance
     */
    public static Name<TupleValue> defineTupleValue(String name, ValueValidator... validators) {
        return define(name, TupleValue.class, validators);
    }
    
    /**
     * defines a new name with UUID-typed value
     * 
     * @param name        the name 
     * @param validators  the validators 
     * @return a new instance
     */
    public static Name<UUID> defineUUID(String name, ValueValidator... validators) {
        return define(name, UUID.class, validators);
    }
    
    private static class SkalarName<T> extends Name<T> {
        private final Class<T> type;
        
        
        /**
         * @param name      the name
         * @param converter the converter
         * @param type      the value type
         */
        SkalarName(String name, Class<T> type, ValueValidator... validators) {
            super(name, validators);
            this.type = type;
        }
        
        @Override
        @SuppressWarnings("unchecked")         
        Optional<T> read(PropertiesSource propertiesSource) {
            return (Optional<T>) propertiesSource.read(getName(), (Class<Object>) type);
        }
    }
    

    /**
     * defines a new name with ByteBuffer-typed value
     * 
     * @param name        the name 
     * @param validators  the validators 
     * @return a new instance
     */
    public static Name<byte[]> defineBytes(String name, ValueValidator... validators) {
        return new ByteSkalarName<>(name, validators);
    }
    
  
    private static class ByteSkalarName<T> extends SkalarName<byte[]> {
        
        /**
         * @param name      the name
         * @param converter the converter
         */
        ByteSkalarName(String name, ValueValidator... validators) {
            super(name, byte[].class, validators);
        }
     
        @Override
        Object convertWrite(byte[] value) {
            return ByteBuffer.wrap(value);
        } 
       
        @Override
        Optional<byte[]> read(PropertiesSource propertiesSource) {
            Optional<ByteBuffer> optionalByteBufffer = propertiesSource.read(getName(), ByteBuffer.class);
            
            if (optionalByteBufffer.isPresent()) {
                ByteBuffer bb = optionalByteBufffer.get();
                byte[] bytes = new byte[bb.remaining()];
                bb.get(bytes, 0, bytes.length);
                return Optional.of(bytes);
                
            } else  {
                return Optional.absent();
            }
        }
    }
    
    
    /**
     * A list name which defines the class type of the associated list member value type 
     *
     * @param <T> the list member value type
     */
    private static class ListName<T> extends Name<List<T>> {
        private final Class<T> elementType;
        
        /**
         * @param name          the name
         * @param validators    the validators  
         * @param elementType   the list member value type
         */
        private ListName(String name, Class<T> elementType, ValueValidator... validators) {
            super(name, validators);
            this.elementType = elementType;
        }
        
        @SuppressWarnings("unchecked")
        @Override
        Optional<List<T>> read(PropertiesSource propertiesSource) {
            return propertiesSource.read(getName(), (Class<Object>) elementType);
        }
    }
    
    

     
    /**
     * A set name which defines the class type of the associated set member value type 
     *
     * @param <T> the set member value type
     */
    private static class SetName<T> extends Name<Set<T>> {
        private final Class<T> elementType;

        /**
         * @param name          the name
         * @param validators    the validators 
         * @param elementType   the set member value type
         */
        private SetName(String name, Class<T> elementType, ValueValidator... validators) {
            super(name, validators);
            this.elementType = elementType;
        }
        
        @SuppressWarnings("unchecked")       
        @Override
        Optional<Set<T>> read(PropertiesSource propertiesSource) {
            return propertiesSource.read(getName(), (Class<Object>) elementType);
        }
    } 
    
    
    /**
     * A set name which defines the class type of the associated set member value type 
     *
     * @param <T> the set member key type
     * @param <V> the set member value type
     */
    private static class MapName<T, V> extends Name<Map<T, V>> {
        
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
        
        @SuppressWarnings("unchecked")        
        @Override
        Optional<Map<T, V>> read(PropertiesSource propertiesSource) {
            return propertiesSource.read(getName(), (Class<Object>) (Class<Object>) keyType, (Class<Object>) valueType);
        }
    } 
}
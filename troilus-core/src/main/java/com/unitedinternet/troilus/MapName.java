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



 
/**
 * A set name which defines the class type of the associated set member value type 
 *
 * @param <T> the set member key type
 * @param <V> the set member value type
 */
public class MapName<T, V> extends Name<T> {
    
    private final Class<V> valueType;
    
    
    /**
     * @param name      the name
     * @param keyType   the set member value type
     * @param valueType the set member value type 
     */
    private MapName(String name, Class<T> keyType, Class<V> valueType) {
        super(name, keyType);
        this.valueType = valueType;
    }
    
    
    /**
     * @return the value type
     */
    Class<V> getValueType() {
        return valueType;
    }
    
    
    /**
     * defines a new set name 
     * 
     * @param name      the name 
     * @param keyType   the set member key type
     * @param valueType the set member value type
     * @return a new instance
     */
    public static <E, F> MapName<E, F> define(String name, Class<E> keyType,  Class<F> valueType) {
        return new MapName<>(name, keyType, valueType);
    }
} 
   
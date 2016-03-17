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

import java.util.Optional;

/**
 * Properties source
 */
interface PropertiesSource {
    
    /**
     * @param name   the property name
     * @param clazz  the property type
     * @return the property value
     */
    <T> Optional<T> read(String name, Class<?> clazz);
    
    /**
     * @param name    the property source 
     * @param clazz1  the property type of element 1 (e.g. key type of map)  
     * @param clazz2  the property type of element 2 (e.g. value type of map) 
     * @return the property value
     */
    <T> Optional<T> read(String name, Class<?> clazz1, Class<?> clazz2);
}
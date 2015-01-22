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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;




/**
 * Optional utility class
 *
 */
class Optionals {

    private static final Class<?> JAVA_OPTIONAL_CLASS;
    private static final Method JAVA_OPTIONAL_OR_ELSE_METH;
    
    static {
        Class<?> clazz = null;
        Method meth = null;
        
        try {
            clazz = Class.forName("java.util.Optional");
            meth = clazz.getMethod("orElse", Object.class);
        } catch (ClassNotFoundException | NoSuchMethodException | SecurityException ignore) { }
        
        JAVA_OPTIONAL_CLASS = clazz;
        JAVA_OPTIONAL_OR_ELSE_METH = meth;
    }
    
    
    private Optionals() {  }

    
    private static boolean isJavaOptional(Object obj) {
        if ((obj != null) && (JAVA_OPTIONAL_CLASS != null)) {
            return JAVA_OPTIONAL_CLASS.isAssignableFrom(obj.getClass());
        }
        
        return false;
    }
    
    @SuppressWarnings("unchecked")
    private static <T> T fromJavaOptional(Object obj) {
        if ((obj != null) && (JAVA_OPTIONAL_OR_ELSE_METH != null)) {
            try {
                return (T) JAVA_OPTIONAL_OR_ELSE_METH.invoke(obj, (Object) null);
            } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException ignore) { }
        }
        
        return (T) obj;
    }
    
    private static boolean isGuavaOptional(Object obj) {
        return (obj == null) ? false  : (Optional.class.isAssignableFrom(obj.getClass()));
    }

    /**
     * @param obj  the object (could also be an optional)
     * @return the object wrapped by guava optional
     */
    @SuppressWarnings("unchecked")
    public static <T> Optional<T> toGuavaOptional(T obj) {
        if (obj == null) {
            return Optional.<T>absent();
        }
        
        if (isGuavaOptional(obj)) {
            return (Optional<T>) obj;
            
        } else if(isJavaOptional(obj)) {
            return Optional.fromNullable((T) fromJavaOptional(obj));
            
        } else {
            return Optional.of(obj);
        }
    }
 

    /**
     * @param map   the map (could include optional values)
     * @return the map which optional wrapped values
     */
    public static ImmutableMap<String, Optional<Object>> toGuavaOptional(ImmutableMap<String, Object> map) {
        Map<String, Optional<Object>> result = Maps.newHashMap();
        
        for (Entry<String, Object> entry : map.entrySet()) {
            result.put(entry.getKey(), toGuavaOptional(entry.getValue()));
        }
        
        return ImmutableMap.copyOf(result);
    }
    
}
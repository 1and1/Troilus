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

import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;


/**
 * Optional utility class
 *
 */
class Optionals {
    
    private Optionals() {  }

    /**
     * @param obj  the object (could also be an optional)
     * @return the object wrapped by optional
     */
    @SuppressWarnings("unchecked")
    public static <T> Optional<T> toOptional(T obj) {
        if (obj == null) {
            return Optional.<T>empty();
        } else if(Optional.class.isAssignableFrom(obj.getClass())) {
            return (Optional<T>) obj;
        } else {
            return Optional.of(obj);
        }
    }
    
    /**
     * @param map   the map (could include optional values)
     * @return the map which optional wrapped values
     */
    public static ImmutableMap<String, Optional<Object>> toOptional(ImmutableMap<String, Object> map) {
        Map<String, Optional<Object>> result = Maps.newHashMap();
        
        for (Entry<String, Object> entry : map.entrySet()) {
            result.put(entry.getKey(), toOptional(entry.getValue()));
        }
        
        return ImmutableMap.copyOf(result);
    }
}
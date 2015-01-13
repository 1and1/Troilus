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


import java.util.Map;
import java.util.Optional;







import java.util.Map.Entry;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

 

class GuavaOptionals {

    private GuavaOptionals() {  }

    
    public static ImmutableMap<String, Optional<Object>> fromStringOptionalMap(ImmutableMap<String, com.google.common.base.Optional<Object>> map) {
        Map<String, Optional<Object>> result = Maps.newHashMap();
        for (Entry<String, com.google.common.base.Optional<Object>> entry : map.entrySet()) {
            result.put(entry.getKey(), Optional.ofNullable(entry.getValue().orNull()));
        }
        
        return ImmutableMap.copyOf(result);        
    }

    
    public static ImmutableMap<String, com.google.common.base.Optional<Object>> toStringOptionalMap(ImmutableMap<String, Optional<Object>> map) {
        Map<String, com.google.common.base.Optional<Object>> result = Maps.newHashMap();
        for (Entry<String, Optional<Object>> entry : map.entrySet()) {
            result.put(entry.getKey(), com.google.common.base.Optional.fromNullable(entry.getValue().orElse(null)));
        }
            
        return ImmutableMap.copyOf(result);
    }
     

    public static ImmutableMap<String, ImmutableMap<Object, Optional<Object>>> fromStringObjectOptionalMap(ImmutableMap<String, ImmutableMap<Object, com.google.common.base.Optional<Object>>>map) {
        Map<String, ImmutableMap<Object, Optional<Object>>> result = Maps.newHashMap();
        for (Entry<String, ImmutableMap<Object, com.google.common.base.Optional<Object>>> entry : map.entrySet()) {
            Map<Object, Optional<Object>> iresult = Maps.newHashMap();
            for (Entry<Object, com.google.common.base.Optional<Object>> entry2 : entry.getValue().entrySet()) {
                iresult.put(entry2.getKey(), Optional.ofNullable(entry2.getValue().orNull()));
            }
            result.put(entry.getKey(), ImmutableMap.copyOf(iresult));
        }
        
        return ImmutableMap.copyOf(result);
    }

    
 
    public static ImmutableMap<String, ImmutableMap<Object, com.google.common.base.Optional<Object>>> toStringObjectOptionalMap(Map<String, ImmutableMap<Object, Optional<Object>>> map) {
        Map<String, ImmutableMap<Object, com.google.common.base.Optional<Object>>> result = Maps.newHashMap();
        for (Entry<String, ImmutableMap<Object, Optional<Object>>> entry : map.entrySet()) {
            Map<Object, com.google.common.base.Optional<Object>> iresult = Maps.newHashMap();
            for (Entry<Object, Optional<Object>> entry2 : entry.getValue().entrySet()) {
                iresult.put(entry2.getKey(), com.google.common.base.Optional.fromNullable(entry2.getValue().orElse(null)));
            }
            result.put(entry.getKey(), ImmutableMap.copyOf(iresult));
        }
        
        return ImmutableMap.copyOf(result);
    }
}
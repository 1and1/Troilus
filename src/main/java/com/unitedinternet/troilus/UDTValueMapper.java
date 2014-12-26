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


import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

 

class UDTValueMapper {
    
    private UDTValueMapper() {  }
 
    
    public static <T> T fromUdtValue(Context ctx, DataType datatype, UDTValue udtValue) {
       return null;
    }
    
    
    @SuppressWarnings("unchecked")
    public static Object toUdtValue(Context ctx, DataType datatype, Object value) {
        
        // build-in type (will not be converted)
        if (ctx.isBuildInType(datatype)) {
            return value;
            
        // udt collection
        } else if (datatype.isCollection()) {
           
           if (Set.class.isAssignableFrom(value.getClass())) {
               DataType elementDataType = datatype.getTypeArguments().get(0);
               
               Set<Object> udt = Sets.newHashSet();
               for (Object element : (Set<Object>) value) {
                   udt.add(toUdtValue(ctx, ctx.getUserType(((UserType) elementDataType).getTypeName()), element));
               }
               
               return ImmutableSet.copyOf(udt);
               
           } else if (List.class.isAssignableFrom(value.getClass())) {
               DataType elementDataType = datatype.getTypeArguments().get(0);
               
               List<Object> udt = Lists.newArrayList();
               for (Object element : (List<Object>) value) {
                   udt.add(toUdtValue(ctx, ctx.getUserType(((UserType) elementDataType).getTypeName()), element));
               }
               
               return ImmutableList.copyOf(udt);
               
           } else {
               DataType keyDataType = datatype.getTypeArguments().get(0);
               DataType valueDataType = datatype.getTypeArguments().get(1);
               
               Map<Object, Object> udt = Maps.newHashMap();
               for (Entry<Object, Object> entry : ((Map<Object, Object>) value).entrySet()) {
                   udt.put(toUdtValue(ctx, ctx.getUserType(((UserType) keyDataType).getTypeName()), entry.getKey()), 
                           toUdtValue(ctx, ctx.getUserType(((UserType) valueDataType).getTypeName()), entry.getValue()));
               }
               
               return ImmutableMap.copyOf(udt);  
           }
    
        // udt
        } else {
            return toUdtValue(ctx, ctx.getUserType(((UserType) datatype).getTypeName()), value);
        }
    }
    
    
    
    private static Object toUdtValue(Context ctx, UserType usertype, Object entity) {
        UDTValue udtValue = usertype.newValue();
        
        for (Entry<String, Optional<Object>> entry : ctx.toValues(entity).entrySet()) {
            DataType fieldType = usertype.getFieldType(entry.getKey());
                    
            if (entry.getValue().isPresent()) {
                Object value = entry.getValue().get();
                
                if (!ctx.isBuildInType(usertype.getFieldType(entry.getKey()))) {
                    value = toUdtValue(ctx, fieldType, value);
                }
                
                udtValue.setBytesUnsafe(entry.getKey(), fieldType.serialize(value, ctx.getProtocolVersion()));
            }
        }
        
        return udtValue;
    }
}

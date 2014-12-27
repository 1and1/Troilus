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
 
    
    public static <T> T fromUdtValue(Context ctx, UserType usertype, UDTValue udtValue, Class<T> type) {
         return ctx.fromValues(type, (name, clazz) -> fromUdtValue(ctx, usertype.getFieldType(name), udtValue, clazz, name));
    }
    
    
    
    public static Optional<?> fromUdtValue(Context ctx, DataType datatype, UDTValue udtValue, Class<?> fieldtype, String fieldname) {
        
        // build-in type 
        if (ctx.isBuildInType(datatype)) {
            return Optional.ofNullable(datatype.deserialize(udtValue.getBytesUnsafe(fieldname), ctx.getProtocolVersion()));
        
            
        // udt collection    
        } else if (datatype.isCollection()) {
            Class<?> type = datatype.getName().asJavaClass();
            
            if (Set.class.isAssignableFrom(type)) {
                return Optional.ofNullable(fromUdtValues(ctx, datatype.getTypeArguments().get(0), ImmutableSet.copyOf(udtValue.getSet(fieldname, UDTValue.class)), fieldtype)); 
                
            } else if (List.class.isAssignableFrom(type)) {
                return Optional.ofNullable(fromUdtValues(ctx, datatype.getTypeArguments().get(0), ImmutableList.copyOf(udtValue.getList(fieldname, UDTValue.class)), fieldtype)); 

            } else {
              
                return null;
            }
            
            
        } else {
            return Optional.ofNullable(fromUdtValue(ctx, datatype, udtValue, fieldtype));
        }
    }
    

    
    public static <T> T fromUdtValue(Context ctx, DataType datatype, UDTValue udtValue, Class<T> type) {
        return ctx.fromValues(type, (name, clazz) -> fromUdtValue(ctx, ((UserType) datatype).getFieldType(name), udtValue, clazz, name));
    }

    
    
    public static <T> ImmutableSet<T> fromUdtValues(Context ctx, DataType datatype, ImmutableSet<UDTValue> udtValues, Class<T> type) {
        Set<T> elements = Sets.newHashSet();
        
        for (UDTValue elementUdtValue : udtValues) {
            T element = ctx.fromValues(type, (name, clazz) -> fromUdtValue(ctx, ((UserType) datatype).getFieldType(name), elementUdtValue, clazz, name));
            elements.add(element);
        }
        
        return ImmutableSet.copyOf(elements);
    }

    
    public static <T> ImmutableList<T> fromUdtValues(Context ctx, DataType datatype, ImmutableList<UDTValue> udtValues, Class<T> type) {
        List<T> elements = Lists.newArrayList();
        
        for (UDTValue elementUdtValue : udtValues) {
            T element = ctx.fromValues(type, (name, clazz) -> fromUdtValue(ctx, ((UserType) datatype).getFieldType(name), elementUdtValue, clazz, name));
            elements.add(element);
        }
        
        return ImmutableList.copyOf(elements);
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
                   udt.add(toUdtValue(ctx, elementDataType, element));
               }
               
               return ImmutableSet.copyOf(udt);
               
           } else if (List.class.isAssignableFrom(value.getClass())) {
               DataType elementDataType = datatype.getTypeArguments().get(0);
               
               List<Object> udt = Lists.newArrayList();
               for (Object element : (List<Object>) value) {
                   udt.add(toUdtValue(ctx, elementDataType, element));
               }
               
               return ImmutableList.copyOf(udt);
               
           } else {
               DataType keyDataType = datatype.getTypeArguments().get(0);
               DataType valueDataType = datatype.getTypeArguments().get(1);
               
               Map<Object, Object> udt = Maps.newHashMap();
               for (Entry<Object, Object> entry : ((Map<Object, Object>) value).entrySet()) {
                     udt.put(toUdtValue(ctx, keyDataType, entry.getKey()), 
                             toUdtValue(ctx, valueDataType, entry.getValue()));
               }
               
               return ImmutableMap.copyOf(udt);  
           }
    
           
        // udt
        } else {
            UserType usertype = ctx.getUserType(((UserType) datatype).getTypeName());
            UDTValue udtValue = usertype.newValue();
            
            for (Entry<String, Optional<Object>> entry : ctx.toValues(value).entrySet()) {
                DataType fieldType = usertype.getFieldType(entry.getKey());
                        
                if (entry.getValue().isPresent()) {
                    Object vl = entry.getValue().get();
                    
                    if (!ctx.isBuildInType(usertype.getFieldType(entry.getKey()))) {
                        vl = toUdtValue(ctx, fieldType, vl);
                    }
                    
                    udtValue.setBytesUnsafe(entry.getKey(), fieldType.serialize(vl, ctx.getProtocolVersion()));
                }
            }
            
            return udtValue; 
        }
    }
}

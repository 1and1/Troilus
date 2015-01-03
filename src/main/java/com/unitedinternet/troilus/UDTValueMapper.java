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

 

/**
 * UDTValueMapper
 */
class UDTValueMapper {
    
    private UDTValueMapper() {  }
 
    
    public static <T> T fromUdtValue(Context ctx, UserType usertype, UDTValue udtValue, Class<T> type) {
         return ctx.fromValues(type, (name, clazz1, clazz2) -> fromUdtValue(ctx, 
                                                                            usertype.getFieldType(name), 
                                                                            udtValue, 
                                                                            clazz1, 
                                                                            clazz2, 
                                                                            name));
    }
    
    
    
    public static Optional<?> fromUdtValue(Context ctx, 
                                           DataType datatype, 
                                           UDTValue udtValue, Class<?> fieldtype1, 
                                           Class<?> fieldtype2,
                                           String fieldname) {
        
        // build-in type 
        if (ctx.isBuildInType(datatype)) {
            return Optional.ofNullable(datatype.deserialize(udtValue.getBytesUnsafe(fieldname), ctx.getProtocolVersion()));
        
            
        // udt collection    
        } else if (datatype.isCollection()) {
            Class<?> type = datatype.getName().asJavaClass();
           
            // set
            if (Set.class.isAssignableFrom(type)) {
                return Optional.ofNullable(fromUdtValues(ctx, 
                                                         datatype.getTypeArguments().get(0), 
                                                         ImmutableSet.copyOf(udtValue.getSet(fieldname, UDTValue.class)), 
                                                         fieldtype1)); 
                
            // list
            } else if (List.class.isAssignableFrom(type)) {
                return Optional.ofNullable(fromUdtValues(ctx, 
                                                         datatype.getTypeArguments().get(0), 
                                                         ImmutableList.copyOf(udtValue.getList(fieldname, UDTValue.class)),
                                                         fieldtype1)); 

            // map
            } else {
                if (ctx.isBuildInType(datatype.getTypeArguments().get(0))) {
                    return Optional.ofNullable(fromUdtValues(ctx, 
                                                             datatype.getTypeArguments().get(0), 
                                                             datatype.getTypeArguments().get(1), 
                                                             ImmutableMap.<Object, Object>copyOf(udtValue.getMap(fieldname, fieldtype1, UDTValue.class)), 
                                                             fieldtype1, 
                                                             fieldtype2));

                } else if (ctx.isBuildInType(datatype.getTypeArguments().get(1))) {
                    return Optional.ofNullable(fromUdtValues(ctx, 
                                                             datatype.getTypeArguments().get(0), 
                                                             datatype.getTypeArguments().get(1), 
                                                             ImmutableMap.<Object, Object>copyOf(udtValue.getMap(fieldname, UDTValue.class, fieldtype2)), 
                                                             fieldtype1, 
                                                             fieldtype2));
                    
                } else {
                    return Optional.ofNullable(fromUdtValues(ctx, 
                                                             datatype.getTypeArguments().get(0), 
                                                             datatype.getTypeArguments().get(1), 
                                                             ImmutableMap.<Object, Object>copyOf(udtValue.getMap(fieldname, UDTValue.class, UDTValue.class)),
                                                             fieldtype1, 
                                                             fieldtype2));
                }
            }
                        
        // udt    
        } else {
            return Optional.ofNullable(fromUdtValue(ctx, datatype, udtValue, fieldtype1));
        }
    }
    

    
    public static <T> T fromUdtValue(Context ctx, DataType datatype, UDTValue udtValue, Class<T> type) {
        return ctx.fromValues(type, (name, clazz1, clazz2) -> fromUdtValue(ctx, 
                                                                           ((UserType) datatype).getFieldType(name), 
                                                                           udtValue, 
                                                                           clazz1, 
                                                                           clazz2,
                                                                           name));
    }


    
    static <T> ImmutableSet<T> fromUdtValues(Context ctx, 
                                                    DataType datatype, 
                                                    ImmutableSet<UDTValue> udtValues, 
                                                    Class<T> type) {
        Set<T> elements = Sets.newHashSet();
        
        for (UDTValue elementUdtValue : udtValues) {
            T element = ctx.fromValues(type, (name, clazz1, clazz2) -> fromUdtValue(ctx, 
                                                                                    ((UserType) datatype).getFieldType(name), 
                                                                                    elementUdtValue, 
                                                                                    clazz1, 
                                                                                    clazz2, 
                                                                                    name));
            elements.add(element);
        }
        
        return ImmutableSet.copyOf(elements);
    }


    
    
    public static <T> ImmutableList<T> fromUdtValues(Context ctx, 
                                                     DataType datatype, 
                                                     ImmutableList<UDTValue> udtValues, 
                                                     Class<T> type) {
        List<T> elements = Lists.newArrayList();
        
        for (UDTValue elementUdtValue : udtValues) {
            T element = ctx.fromValues(type, (name, clazz1, clazz2) -> fromUdtValue(ctx, 
                                                                                    ((UserType) datatype).getFieldType(name), 
                                                                                    elementUdtValue, 
                                                                                    clazz1,
                                                                                    clazz2, 
                                                                                    name));
            elements.add(element);
        }
        
        return ImmutableList.copyOf(elements);
    }

    
    
    @SuppressWarnings("unchecked")
    public static <K, V> ImmutableMap<K, V> fromUdtValues(Context ctx, 
                                                          DataType keyDatatype, 
                                                          DataType valueDatatype, 
                                                          ImmutableMap<?, ?> udtValues, 
                                                          Class<K> keystype, 
                                                          Class<V> valuesType) {
        
        Map<K, V> elements = Maps.newHashMap();
        
        for (Entry<?, ?> entry : udtValues.entrySet()) {
            
            K keyElement;
            if (keystype.isAssignableFrom(entry.getKey().getClass())) {
                keyElement = (K) entry.getKey(); 
            } else {
                keyElement = ctx.fromValues(keystype, (name, clazz1, clazz2) -> fromUdtValue(ctx, 
                                                                                             ((UserType) keyDatatype).getFieldType(name), 
                                                                                             (UDTValue) entry.getKey(), 
                                                                                             clazz1, 
                                                                                             clazz2, 
                                                                                             name));
            }
            
            V valueElement;
            if (valuesType.isAssignableFrom(entry.getValue().getClass())) {
                valueElement = (V) entry.getValue(); 
            } else {
                valueElement = ctx.fromValues(valuesType, (name, clazz1, clazz2) -> fromUdtValue(ctx, 
                                                                                                 ((UserType) valueDatatype).getFieldType(name), 
                                                                                                 (UDTValue) entry.getValue(), 
                                                                                                 clazz1, 
                                                                                                 clazz2, 
                                                                                                 name));
            }

            elements.put(keyElement, valueElement);
        }
        
        return ImmutableMap.copyOf(elements);
    }
    
    
    @SuppressWarnings("unchecked")
    public static Object toUdtValue(Context ctx, DataType datatype, Object value) {
        
        // build-in type (will not be converted)
        if (ctx.isBuildInType(datatype)) {
            return value;
            
        // udt collection
        } else if (datatype.isCollection()) {
           
           // set 
           if (Set.class.isAssignableFrom(datatype.getName().asJavaClass())) {
               DataType elementDataType = datatype.getTypeArguments().get(0);
               
               Set<Object> udt = Sets.newHashSet();
               if (value != null) {
                   for (Object element : (Set<Object>) value) {
                       udt.add(toUdtValue(ctx, elementDataType, element));
                   }
               }
               
               return ImmutableSet.copyOf(udt);
               
           // list 
           } else if (List.class.isAssignableFrom(datatype.getName().asJavaClass())) {
               DataType elementDataType = datatype.getTypeArguments().get(0);
               
               List<Object> udt = Lists.newArrayList();
               if (value != null) {
                   for (Object element : (List<Object>) value) {
                       udt.add(toUdtValue(ctx, elementDataType, element));
                   }
               }
               
               return ImmutableList.copyOf(udt);
              
           // map
           } else {
               DataType keyDataType = datatype.getTypeArguments().get(0);
               DataType valueDataType = datatype.getTypeArguments().get(1);
               
               Map<Object, Object> udt = Maps.newHashMap();
               if (value != null) {
                   for (Entry<Object, Object> entry : ((Map<Object, Object>) value).entrySet()) {
                         udt.put(toUdtValue(ctx, keyDataType, entry.getKey()), 
                                 toUdtValue(ctx, valueDataType, entry.getValue()));
                   }
               
               }
               return ImmutableMap.copyOf(udt);  
           }
    
           
        // udt
        } else {
            if (value == null) {
                return value;
                
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
}

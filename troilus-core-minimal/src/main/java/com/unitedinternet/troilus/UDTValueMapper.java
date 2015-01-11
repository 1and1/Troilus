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
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;
import com.google.common.base.Optional;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;



class UDTValueMapper {

    private final ProtocolVersion protocolVersion;
    private final BeanMapper beanMapper;
    
    UDTValueMapper(ProtocolVersion protocolVersion, BeanMapper beanMapper) {
        this.protocolVersion = protocolVersion;
        this.beanMapper = beanMapper;
    }
    
    
    
    private UserType getUserType(LoadingCache<String, UserType> userTypeCache, String usertypeName) {
        try {
            return userTypeCache.get(usertypeName);
        } catch (ExecutionException e) {
            throw new RuntimeException(e.getCause());
        }
    }




    static boolean isBuildInType(DataType dataType) {        
        if (dataType.isCollection()) {
            for (DataType type : dataType.getTypeArguments()) {
                if (!isBuildInType(type)) {
                    return false;
                }
            }
            return true;

        } else {
            return DataType.allPrimitiveTypes().contains(dataType);
        }
    }
    
    
    
    /**
     * @param datatype   the db datatype
     * @param udtValue   the udt value
     * @param fieldtype1 the field 1 type
     * @param fieldtype2 the field 2 type
     * @param fieldname  the fieldname
     * @return the mapped value or <code>null</code>
     */
    public Object fromUdtValue(DataType datatype, 
                               UDTValue udtValue,
                               Class<?> fieldtype1, 
                               Class<?> fieldtype2,
                               String fieldname) {
        
        // build-in type 
        if (isBuildInType(datatype)) {
            return datatype.deserialize(udtValue.getBytesUnsafe(fieldname), protocolVersion);
        
            
        // udt collection    
        } else if (datatype.isCollection()) {
            Class<?> type = datatype.getName().asJavaClass();
           
            // set
            if (Set.class.isAssignableFrom(type)) {
                return fromUdtValues(datatype.getTypeArguments().get(0), 
                                     ImmutableSet.copyOf(udtValue.getSet(fieldname, UDTValue.class)), 
                                     fieldtype1); 
                
            // list
            } else if (List.class.isAssignableFrom(type)) {
                return fromUdtValues(datatype.getTypeArguments().get(0), 
                                     ImmutableList.copyOf(udtValue.getList(fieldname, UDTValue.class)),
                                     fieldtype1); 

            // map
            } else {
                if (isBuildInType(datatype.getTypeArguments().get(0))) {
                    return fromUdtValues(datatype.getTypeArguments().get(0), 
                                         datatype.getTypeArguments().get(1), 
                                         ImmutableMap.<Object, Object>copyOf(udtValue.getMap(fieldname, fieldtype1, UDTValue.class)), 
                                         fieldtype1, 
                                         fieldtype2);

                } else if (isBuildInType(datatype.getTypeArguments().get(1))) {
                    return fromUdtValues(datatype.getTypeArguments().get(0), 
                                         datatype.getTypeArguments().get(1), 
                                         ImmutableMap.<Object, Object>copyOf(udtValue.getMap(fieldname, UDTValue.class, fieldtype2)), 
                                         fieldtype1, 
                                         fieldtype2);
                    
                } else {
                    return fromUdtValues(datatype.getTypeArguments().get(0), 
                                         datatype.getTypeArguments().get(1), 
                                         ImmutableMap.<Object, Object>copyOf(udtValue.getMap(fieldname, UDTValue.class, UDTValue.class)),
                                         fieldtype1, 
                                         fieldtype2);
                }
            }
                        
        // udt    
        } else {
            return fromUdtValue(datatype, udtValue, fieldtype1);
        }
    }
    

    
    public <T> T fromUdtValue(final DataType datatype, final UDTValue udtValue, Class<T> type) {
        
        PropertiesSource propsSource = new PropertiesSource() {
            
            @SuppressWarnings("unchecked")
            @Override
            public <E> Optional<E> read(String name, Class<Object> clazz1, Class<Object> clazz2) {
                return Optional.fromNullable((E) fromUdtValue(((UserType) datatype).getFieldType(name), udtValue, clazz1, clazz2, name));
            }
        };
        
        return beanMapper.fromValues(type, propsSource);
    }

    
    <T> ImmutableSet<T> fromUdtValues(final DataType datatype, ImmutableSet<UDTValue> udtValues, Class<T> type) {
        Set<T> elements = Sets.newHashSet();
        
        for (UDTValue elementUdtValue : udtValues) {

            final UDTValue elementUdtVal = elementUdtValue;
            
            PropertiesSource propsSource = new PropertiesSource() {
                
                @SuppressWarnings("unchecked")
                @Override
                public <E> Optional<E> read(String name, Class<Object> clazz1, Class<Object> clazz2) {
                    return Optional.fromNullable((E) fromUdtValue(((UserType) datatype).getFieldType(name), elementUdtVal, clazz1, clazz2, name));
                }
            };

            T element = beanMapper.fromValues(type, propsSource);
            elements.add(element);
        }
        
        return ImmutableSet.copyOf(elements);
    }


    
    
    public <T> ImmutableList<T> fromUdtValues(final DataType datatype, ImmutableList<UDTValue> udtValues, Class<T> type) {
        List<T> elements = Lists.newArrayList();
        
        for (UDTValue elementUdtValue : udtValues) {
            
            final UDTValue elementUdtVal = elementUdtValue;
            
            PropertiesSource propsSource = new PropertiesSource() {
                
                @SuppressWarnings("unchecked")
                @Override
                public <E> Optional<E>  read(String name, Class<Object> clazz1, Class<Object> clazz2) {
                    return Optional.fromNullable((E) fromUdtValue(((UserType) datatype).getFieldType(name), elementUdtVal, clazz1, clazz2, name));
                }
            };

            
            T element = beanMapper.fromValues(type, propsSource);
            elements.add(element);
        }
        
        return ImmutableList.copyOf(elements);
    }

    
    
    @SuppressWarnings("unchecked")
    public <K, V> ImmutableMap<K, V> fromUdtValues(final DataType keyDatatype, final DataType valueDatatype, ImmutableMap<?, ?> udtValues, Class<K> keystype, Class<V> valuesType) {
        
        Map<K, V> elements = Maps.newHashMap();

        for (Entry<?, ?> entry : udtValues.entrySet()) {
        
            K keyElement;
            if (keystype.isAssignableFrom(entry.getKey().getClass())) {
                keyElement = (K) entry.getKey(); 
                
            } else {
                final UDTValue keyUdtValue = (UDTValue) entry.getKey();
                
                PropertiesSource propsSource = new PropertiesSource() {
                    
                    @Override
                    public <T> Optional<T> read(String name, Class<Object> clazz1, Class<Object> clazz2) {
                        return Optional.fromNullable((T) fromUdtValue(((UserType) keyDatatype).getFieldType(name), keyUdtValue, clazz1, clazz2, name));
                    }
                };

                keyElement = beanMapper.fromValues(keystype, propsSource);
            }
            
            
            
            V valueElement;
            if (valuesType.isAssignableFrom(entry.getValue().getClass())) {
                valueElement = (V) entry.getValue();
                
            } else {
                final UDTValue valueUdtValue = (UDTValue) entry.getValue();

                PropertiesSource propsSource = new PropertiesSource() {
                    
                    @Override
                    public <T> Optional<T> read(String name, Class<Object> clazz1, Class<Object> clazz2) {
                        return Optional.fromNullable((T) fromUdtValue(((UserType) valueDatatype).getFieldType(name), valueUdtValue, clazz1, clazz2, name));
                    }
                };
                
                valueElement = beanMapper.fromValues(valuesType, propsSource);
            }

            elements.put(keyElement, valueElement);
        }
        
        return ImmutableMap.copyOf(elements);
    }
    
    
    @SuppressWarnings("unchecked")
    public Object toUdtValue(LoadingCache<String, UserType> userTypeCache, DataType datatype, Object value) {
        
        // build-in type (will not be converted)
        if (isBuildInType(datatype)) {
            return value;
            
        // udt collection
        } else if (datatype.isCollection()) {
           
           // set 
           if (Set.class.isAssignableFrom(datatype.getName().asJavaClass())) {
               DataType elementDataType = datatype.getTypeArguments().get(0);
               
               Set<Object> udt = Sets.newHashSet();
               if (value != null) {
                   for (Object element : (Set<Object>) value) {
                       udt.add(toUdtValue(userTypeCache, elementDataType, element));
                   }
               }
               
               return ImmutableSet.copyOf(udt);
               
           // list 
           } else if (List.class.isAssignableFrom(datatype.getName().asJavaClass())) {
               DataType elementDataType = datatype.getTypeArguments().get(0);
               
               List<Object> udt = Lists.newArrayList();
               if (value != null) {
                   for (Object element : (List<Object>) value) {
                       udt.add(toUdtValue(userTypeCache, elementDataType, element));
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
                         udt.put(toUdtValue(userTypeCache, keyDataType, entry.getKey()), 
                                 toUdtValue(userTypeCache, valueDataType, entry.getValue()));
                   }
               
               }
               return ImmutableMap.copyOf(udt);  
           }
    
           
        // udt
        } else {
            if (value == null) {
                return value;
                
            } else {
                UserType usertype = getUserType(userTypeCache, ((UserType) datatype).getTypeName());
                UDTValue udtValue = usertype.newValue();
                
                for (Entry<String, Optional<Object>> entry : beanMapper.toValues(value).entrySet()) {
                    if (!entry.getValue().isPresent()) {
                        return null;
                    }

                    DataType fieldType = usertype.getFieldType(entry.getKey());
                    Object vl = entry.getValue().get();
                    
                    if (!isBuildInType(usertype.getFieldType(entry.getKey()))) {
                        vl = toUdtValue(userTypeCache, fieldType, vl);
                    }
                    
                    udtValue.setBytesUnsafe(entry.getKey(), fieldType.serialize(vl, protocolVersion));
                }
                
                return udtValue;
            }
        }
    }
}    
        
        


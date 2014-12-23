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



import java.lang.reflect.Constructor;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.mapping.annotations.Field;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

 

class EntityMapper {
    private final LoadingCache<Class<?>, PropertiesMapper> propertiesMapperCache;
    
    
    
    
    public EntityMapper() {        
        this.propertiesMapperCache = CacheBuilder.newBuilder()
                                                 .maximumSize(200)
                                                 .build(new PropertiesMapperLoader());
    }
     
    
    
    private static final class PropertiesMapper {
        private final Class<?> clazz;
        private final ImmutableSet<BiConsumer<Object, Record>> propertyWriters;
        
        private final ImmutableMap<String, Function<Object, Map.Entry<String, Object>>> valueReaders;
        
    
        public PropertiesMapper(ImmutableMap<String, Function<Object, Map.Entry<String, Object>>> valueReaders, ImmutableSet<BiConsumer<Object, Record>> propertyWriters, Class<?> clazz) {
            this.valueReaders = valueReaders;
            this.propertyWriters = propertyWriters;
            this.clazz = clazz;
        }
     
      
        public ImmutableMap<String, Object> toValues(Object entity) {
            Map<String, Object> values = Maps.newHashMap();
            
            for (Function<Object, Map.Entry<String, Object>> valueReader : valueReaders.values()) {
                Map.Entry<String, Object> pair = valueReader.apply(entity); 
                values.put(pair.getKey(), pair.getValue());
            }

            return ImmutableMap.copyOf(values);
        }

        
        @SuppressWarnings("unchecked")
        public <T> T fromValues(Record record) {
            try {
                T persistenceObject = newInstance((Constructor<T>) clazz.getDeclaredConstructor());
                propertyWriters.forEach(writer -> writer.accept(persistenceObject, record)); 
                
                return persistenceObject;
            } catch (ReflectiveOperationException e) {
                throw new RuntimeException(e);
            }
        }
        
        
        private <T> T newInstance(Constructor<T> constructor) {
            try {
                return (T) constructor.newInstance();
            } catch (ReflectiveOperationException e) {
                constructor.setAccessible(true);
                try {
                    return (T) constructor.newInstance();
                } catch (ReflectiveOperationException e2) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
    
  

    
    public ImmutableMap<String, Object> toValues(Object entity) {
        return getPropertiesMapper(entity.getClass()).toValues(entity);
    }

    
    public <T> T fromValues(Class<?> clazz, Record record) {
        return getPropertiesMapper(clazz).fromValues(record);
    }
    
    
    
    private PropertiesMapper getPropertiesMapper(Class<?> clazz) {
        try {
            return propertiesMapperCache.get(clazz);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
    
    


    public interface RecordValueMapper extends BiFunction<Record, String, Optional<Object>> {
        boolean isSupport(Class<?> type);
        
    }

 
    
    private static final class PropertiesMapperLoader extends CacheLoader<Class<?>, PropertiesMapper> {
        private final ImmutableList<RecordValueMapper> recordValueMappers = ImmutableList.of(new OptionalRecordValueMapper(), 
                                                                                             new ImmutableSetRecordValueMapper(),
                                                                                             new ImmutableListRecordValueMapper(),
                                                                                             new ImmutableMapRecordValueMapper(),
                                                                                             new DefaultRecordValueMapper());
    
        
     
        
        
        @Override
        public PropertiesMapper load(Class<?> clazz) throws Exception {
            Map<String, Function<Object, Map.Entry<String, Object>>> valueReaders = Maps.newHashMap();
            
            // check attributes
            valueReaders.putAll(fetchFieldReaders(ImmutableSet.copyOf(clazz.getFields())));
            valueReaders.putAll(fetchFieldReaders(ImmutableSet.copyOf(clazz.getDeclaredFields())));
     
            
            Set<BiConsumer<Object, Record>> propertyWriters = Sets.newHashSet();
            propertyWriters.addAll(fetchFieldWriters(ImmutableSet.copyOf(clazz.getFields())));
            propertyWriters.addAll(fetchFieldWriters(ImmutableSet.copyOf(clazz.getDeclaredFields())));
                   
            return new PropertiesMapper(ImmutableMap.copyOf(valueReaders), ImmutableSet.copyOf(propertyWriters), clazz);
        }
        
        
        
        
        private static ImmutableMap<String, Function<Object, Map.Entry<String, Object>>> fetchFieldReaders(ImmutableSet<java.lang.reflect.Field> beanFields) {
            Map<String, Function<Object, Map.Entry<String, Object>>> valueReaders = Maps.newHashMap();
            
            for (java.lang.reflect.Field beanField : beanFields) {
                Field annotation = beanField.getAnnotation(com.datastax.driver.mapping.annotations.Field.class);
                String columnName = annotation.name();    
                if (columnName != null) {
                    valueReaders.put(columnName, entity -> Maps.immutableEntry(columnName, readBeanField(beanField, entity)));
                }
            }
            
            return ImmutableMap.copyOf(valueReaders);
        }
        
            
        @SuppressWarnings({ "unchecked", "rawtypes" })
        private static Optional<Object> readBeanField(java.lang.reflect.Field field, Object persistenceObject) {
            Object value = null;
            try {
                field.setAccessible(true);
                value = field.get(persistenceObject);
            } catch (IllegalArgumentException | IllegalAccessException e) { }
            
            if (value instanceof Optional) {
                return (Optional) value;
            } else {
                return Optional.ofNullable(value);
            }
        }
    
        
        
        
        private ImmutableSet<BiConsumer<Object, Record>> fetchFieldWriters(ImmutableSet<java.lang.reflect.Field> beanFields) {
            Set<BiConsumer<Object, Record>> valueWriters = Sets.newHashSet();
            
            for (java.lang.reflect.Field beanField : beanFields) {
                Field annotation = beanField.getAnnotation(com.datastax.driver.mapping.annotations.Field.class);
                String columnName = annotation.name();
                Class<?> beanFieldClass = beanField.getType();
                                  
                for (RecordValueMapper valueMapper : recordValueMappers) {
                    if (valueMapper.isSupport(beanFieldClass)) {
                        valueWriters.add((persistenceObject, record) -> valueMapper.apply(record, columnName).ifPresent(value -> writeBeanField(beanField, persistenceObject, value)));
                        break;
                    }
                }
            }
            
            return ImmutableSet.copyOf(valueWriters);
        }
        
        
        private static void writeBeanField(java.lang.reflect.Field field, Object persistenceObject, Object value) {
            try {
                field.setAccessible(true);
                field.set(persistenceObject, value);
            } catch (IllegalArgumentException | IllegalAccessException e) {
                e.printStackTrace();
            }
        }
    
    
            
      
        
     
        private static final class OptionalRecordValueMapper implements RecordValueMapper {
            
            @Override
            public boolean isSupport(Class<?> type) {
                return Optional.class.isAssignableFrom(type);
            }
         
            
            @Override
            public Optional<Object> apply(Record record, String columnname) {
                for (Definition definition : record.getColumnDefinitions().asList()) {
                    if (definition.getName().equals(columnname)) {
                        Optional<Object> optionalValue =  record.getBytesUnsafe(columnname).map(bytes -> Optional.of(definition.getType().deserialize(bytes, record.getProtocolVersion())));
                        if (optionalValue.isPresent()) {
                            return optionalValue;
                        }
                    }
                }
                
                return Optional.empty();
            }
        }
        
        
        
        private static final class ImmutableSetRecordValueMapper implements RecordValueMapper {
            
            @Override
            public boolean isSupport(Class<?> type) {
                return ImmutableSet.class.isAssignableFrom(type);
            }
         
            @SuppressWarnings({ "unchecked", "rawtypes" })
            @Override
            public Optional<Object> apply(Record record, String columnname) {
                
                for (Definition definition : record.getColumnDefinitions().asList()) {
                    if (definition.getName().equals(columnname)) {
                        Optional<Object> optionalValue =  record.getBytesUnsafe(columnname).map(bytes -> ImmutableSet.copyOf((Collection) definition.getType().deserialize(bytes, record.getProtocolVersion())));
                        if (optionalValue.isPresent()) {
                            return optionalValue;
                        }
                    }
                }

                return Optional.empty();
            }
        }

        
        private static final class ImmutableListRecordValueMapper implements RecordValueMapper {
            
            @Override
            public boolean isSupport(Class<?> type) {
                return ImmutableList.class.isAssignableFrom(type);
            }
         
            @SuppressWarnings({ "unchecked", "rawtypes" })
            @Override
            public Optional<Object> apply(Record record, String columnname) {
                for (Definition definition : record.getColumnDefinitions().asList()) {
                    if (definition.getName().equals(columnname)) {
                        Optional<Object> optionalValue =  record.getBytesUnsafe(columnname).map(bytes -> ImmutableList.copyOf((Collection) definition.getType().deserialize(bytes, record.getProtocolVersion())));
                        if (optionalValue.isPresent()) {
                            return optionalValue;
                        }
                    }
                }
                
                return Optional.empty();
            }
        }


        private static final class ImmutableMapRecordValueMapper implements RecordValueMapper {
            
            @Override
            public boolean isSupport(Class<?> type) {
                return ImmutableList.class.isAssignableFrom(type);
            }
         
            

            @SuppressWarnings({ "unchecked", "rawtypes" })
            @Override
            public Optional<Object> apply(Record record, String columnname) {
                
                for (Definition definition : record.getColumnDefinitions().asList()) {
                    if (definition.getName().equals(columnname)) {
                        if (definition.getName().equals(columnname)) {
                            Optional<Object> optionalValue =  record.getBytesUnsafe(columnname).map(bytes -> ImmutableMap.copyOf((Map) definition.getType().deserialize(bytes, record.getProtocolVersion())));
                            if (optionalValue.isPresent()) {
                                return optionalValue;
                            }
                        }
                    }
                }

                return Optional.empty();
            }
        }



        
        private static final class DefaultRecordValueMapper implements RecordValueMapper {
            
            @Override
            public boolean isSupport(Class<?> type) {
                return true;
            }
         
            
            @Override
            public Optional<Object> apply(Record record, String columnname) {
                for (Definition definition : record.getColumnDefinitions().asList()) {
                    if (definition.getName().equals(columnname)) {
                        Optional<Object> optionalValue =  record.getBytesUnsafe(columnname).map(bytes -> definition.getType().deserialize(bytes, record.getProtocolVersion()));
                        if (optionalValue.isPresent()) {
                            return optionalValue;
                        }
                    }
                }
                
                return Optional.empty();
            }
        }   
    }
}

        
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



import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;
import java.util.function.Function;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

 

class BeanMapper {
    private final LoadingCache<Class<?>, PropertiesMapper> propertiesMapperCache;
    
    
    
    
    public BeanMapper() {        
        this.propertiesMapperCache = CacheBuilder.newBuilder()
                                                 .maximumSize(200)
                                                 .build(new PropertiesMapperLoader());
    }
     
    
    
    private static final class PropertiesMapper {
        private final Class<?> clazz;
        private final ImmutableSet<BiConsumer<Object, Function<String, Optional<?>>>> propertyWriters;
        
        private final ImmutableMap<String, Function<Object, Map.Entry<String, Optional<Object>>>> valueReaders;
        
    
        public PropertiesMapper(ImmutableMap<String, Function<Object, Map.Entry<String, Optional<Object>>>> valueReaders, ImmutableSet<BiConsumer<Object, Function<String, Optional<?>>>> propertyWriters, Class<?> clazz) {
            this.valueReaders = valueReaders;
            this.propertyWriters = propertyWriters;
            this.clazz = clazz;
        }
     
      
        public ImmutableMap<String, Optional<Object>> toValues(Object entity) {
            Map<String, Optional<Object>> values = Maps.newHashMap();
            
            for (Function<Object, Map.Entry<String, Optional<Object>>> valueReader : valueReaders.values()) {
                Map.Entry<String, Optional<Object>> pair = valueReader.apply(entity); 
                values.put(pair.getKey(), pair.getValue());
            }

            return ImmutableMap.copyOf(values);
        }

        
        @SuppressWarnings("unchecked")
        public <T> T fromValues(Function<String, Optional<?>> datasource) {
            try {
                T persistenceObject = newInstance((Constructor<T>) clazz.getDeclaredConstructor());
                propertyWriters.forEach(writer -> writer.accept(persistenceObject, datasource)); 
                
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
    
  

    
    public ImmutableMap<String, Optional<Object>> toValues(Object entity) {
        return getPropertiesMapper(entity.getClass()).toValues(entity);
    }

    
    public <T> T fromValues(Class<?> clazz, Function<String, Optional<?>> datasource) {
        return getPropertiesMapper(clazz).fromValues(datasource);
    }
    
    
    
    private PropertiesMapper getPropertiesMapper(Class<?> clazz) {
        try {
            return propertiesMapperCache.get(clazz);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
    
    

    
    private static final class PropertiesMapperLoader extends CacheLoader<Class<?>, PropertiesMapper> {

        
        @Override
        public PropertiesMapper load(Class<?> clazz) throws Exception {
            Map<String, Function<Object, Map.Entry<String, Optional<Object>>>> valueReaders = Maps.newHashMap();
            
            // check attributes
            valueReaders.putAll(fetchJEEFieldReaders(ImmutableSet.copyOf(clazz.getFields())));
            valueReaders.putAll(fetchJEEFieldReaders(ImmutableSet.copyOf(clazz.getDeclaredFields())));
            valueReaders.putAll(fetchCMapperFieldReaders(ImmutableSet.copyOf(clazz.getFields())));
            valueReaders.putAll(fetchCMapperFieldReaders(ImmutableSet.copyOf(clazz.getDeclaredFields())));
     
            
            Set<BiConsumer<Object, Function<String, Optional<?>>>> propertyWriters = Sets.newHashSet();
            propertyWriters.addAll(fetchJEEFieldWriters(ImmutableSet.copyOf(clazz.getFields())));
            propertyWriters.addAll(fetchJEEFieldWriters(ImmutableSet.copyOf(clazz.getDeclaredFields())));
            propertyWriters.addAll(fetchCMapperFieldWriters(ImmutableSet.copyOf(clazz.getFields())));
            propertyWriters.addAll(fetchCMapperFieldWriters(ImmutableSet.copyOf(clazz.getDeclaredFields())));
                   
            return new PropertiesMapper(ImmutableMap.copyOf(valueReaders), ImmutableSet.copyOf(propertyWriters), clazz);
        }
        
        
        
        private static ImmutableMap<String, Function<Object, Map.Entry<String, Optional<Object>>>> fetchJEEFieldReaders(ImmutableSet<Field> beanFields) {
            Map<String, Function<Object, Map.Entry<String, Optional<Object>>>> valueReaders = Maps.newHashMap();
            
            for (Field beanField : beanFields) {
                for (Annotation annotation : beanField.getAnnotations()) {
                    
                    if (annotation.annotationType().getName().equals("javax.persistence.Column")) {
                        for (Method attributeMethod : annotation.annotationType().getDeclaredMethods()) {
                            if (attributeMethod.getName().equalsIgnoreCase("name")) {
                                try {
                                    String columnName = (String) attributeMethod.invoke(annotation);
                                    if (columnName != null) {
                                        valueReaders.put(columnName, entity -> Maps.immutableEntry(columnName, readBeanField(beanField, entity)));
                                    }
                                    break;

                                } catch (ReflectiveOperationException ignore) { }
                            }
                        }
                    }
                }
            }
            
            return ImmutableMap.copyOf(valueReaders);
        }
        

        private static ImmutableMap<String, Function<Object, Map.Entry<String, Optional<Object>>>> fetchCMapperFieldReaders(ImmutableSet<java.lang.reflect.Field> beanFields) {
            Map<String, Function<Object, Map.Entry<String, Optional<Object>>>> valueReaders = Maps.newHashMap();
            
            for (Field beanField : beanFields) {
                for (Annotation annotation : beanField.getAnnotations()) {
                    
                    if (annotation.annotationType().getName().equals("com.datastax.driver.mapping.annotations.Field")) {
                        for (Method attributeMethod : annotation.annotationType().getDeclaredMethods()) {
                            if (attributeMethod.getName().equalsIgnoreCase("name")) {
                                try {
                                    String columnName = (String) attributeMethod.invoke(annotation);
                                    if (columnName != null) {
                                        valueReaders.put(columnName, entity -> Maps.immutableEntry(columnName, readBeanField(beanField, entity)));
                                    }
                                    break;

                                } catch (ReflectiveOperationException ignore) { }
                            }
                        }
                    }
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
    
        
        private ImmutableSet<BiConsumer<Object, Function<String, Optional<?>>>> fetchJEEFieldWriters(ImmutableSet<Field> beanFields) {
            Set<BiConsumer<Object, Function<String, Optional<?>>>> valueWriters = Sets.newHashSet();
            
            for (Field beanField : beanFields) {
                for (Annotation annotation : beanField.getAnnotations()) {
                    
                    if (annotation.annotationType().getName().equals("javax.persistence.Column")) {
                        for (Method attributeMethod : annotation.annotationType().getDeclaredMethods()) {
                            if (attributeMethod.getName().equalsIgnoreCase("name")) {
                                writeBeanField(valueWriters, beanField, annotation, attributeMethod);
                            }
                        }
                    }
                }
            }
            
            return ImmutableSet.copyOf(valueWriters);
        }

                
        private ImmutableSet<BiConsumer<Object, Function<String, Optional<?>>>> fetchCMapperFieldWriters(ImmutableSet<Field> beanFields) {
            Set<BiConsumer<Object, Function<String, Optional<?>>>> valueWriters = Sets.newHashSet();

            for (Field beanField : beanFields) {
                for (Annotation annotation : beanField.getAnnotations()) {
                    
                    if (annotation.annotationType().getName().equals("com.datastax.driver.mapping.annotations.Field")) {
                        for (Method attributeMethod : annotation.annotationType().getDeclaredMethods()) {
                            if (attributeMethod.getName().equalsIgnoreCase("name")) {
                                writeBeanField(valueWriters, beanField, annotation, attributeMethod);
                            }
                        }
                    }
                }
            }
            
            return ImmutableSet.copyOf(valueWriters);
        }
        
        


        @SuppressWarnings({ "unchecked", "rawtypes" })
        private static void writeBeanField(Set<BiConsumer<Object, Function<String, Optional<?>>>> valueWriters, Field beanField, Annotation annotation, Method attributeMethod) {
            try {
                String columnName = (String) attributeMethod.invoke(annotation);
                Class<?> beanFieldClass = beanField.getType();
              
                if (Optional.class.isAssignableFrom(beanFieldClass)) {
                    valueWriters.add((persistenceObject, dataSource) -> writeBeanField(beanField, persistenceObject, dataSource.apply(columnName)));
                    
                } else if (ImmutableSet.class.isAssignableFrom(beanFieldClass)) {
                    valueWriters.add((persistenceObject, dataSource) -> dataSource.apply(columnName).ifPresent(value -> writeBeanField(beanField, persistenceObject, ImmutableSet.copyOf((Collection) value))));

                } else if (ImmutableList.class.isAssignableFrom(beanFieldClass)) {
                    valueWriters.add((persistenceObject, dataSource) -> dataSource.apply(columnName).ifPresent(value -> writeBeanField(beanField, persistenceObject, ImmutableList.copyOf((Collection) value))));

                } else if (ImmutableMap.class.isAssignableFrom(beanFieldClass)) {
                    valueWriters.add((persistenceObject, dataSource) -> dataSource.apply(columnName).ifPresent(value -> writeBeanField(beanField, persistenceObject, ImmutableMap.copyOf((Map) value))));

                } else {
                    valueWriters.add((persistenceObject, dataSource) -> dataSource.apply(columnName).ifPresent(value -> writeBeanField(beanField, persistenceObject, value)));
                }

            } catch (ReflectiveOperationException ignore) { }
        }
        
        
        
        private static void writeBeanField(java.lang.reflect.Field field, Object persistenceObject, Object value) {
            try {
                field.setAccessible(true);
                field.set(persistenceObject, value);
            } catch (IllegalArgumentException | IllegalAccessException e) {
                e.printStackTrace();
            }
        }
    }    
}

        
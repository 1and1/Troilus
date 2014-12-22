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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

import com.datastax.driver.core.ColumnDefinitions.Definition;
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
        
        private final ImmutableMap<String ,BiFunction<WriteWithValues, Object, WriteWithValues>> valueReaders;
        
    
        public PropertiesMapper(ImmutableMap<String, BiFunction<WriteWithValues, Object, WriteWithValues>> valueReaders, ImmutableSet<BiConsumer<Object, Record>> propertyWriters, Class<?> clazz) {
            this.valueReaders = valueReaders;
            this.propertyWriters = propertyWriters;
            this.clazz = clazz;
        }
     
      
        public WriteWithValues write(WriteWithValues writer, Object entity) {
            
            for (BiFunction<WriteWithValues, Object, WriteWithValues> valueReader : valueReaders.values()) {
                writer = valueReader.apply(writer, entity);
            }

            return writer;
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
    
  

    
    
    public WriteWithValues readPropertiesAndEnhanceWrite(WriteWithValues writer, Object entity) {
        return getPropertiesMapper(entity.getClass()).write(writer, entity);
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
            Map<String, BiFunction<WriteWithValues, Object, WriteWithValues>> valueReaders = Maps.newHashMap();
            
            // check attributes
            valueReaders.putAll(fetchFieldReaders(ImmutableSet.copyOf(clazz.getFields())));
            valueReaders.putAll(fetchMethodReaders(ImmutableSet.copyOf(clazz.getMethods())));
            valueReaders.putAll(fetchFieldReaders(ImmutableSet.copyOf(clazz.getDeclaredFields())));
            valueReaders.putAll(fetchMethodReaders(ImmutableSet.copyOf(clazz.getDeclaredMethods())));
     
            
            Set<BiConsumer<Object, Record>> propertyWriters = Sets.newHashSet();
            propertyWriters.addAll(fetchFieldWriters(ImmutableSet.copyOf(clazz.getFields())));
            propertyWriters.addAll(fetchMethodWriters(ImmutableSet.copyOf(clazz.getMethods())));
            propertyWriters.addAll(fetchFieldWriters(ImmutableSet.copyOf(clazz.getDeclaredFields())));
            propertyWriters.addAll(fetchMethodWriters(ImmutableSet.copyOf(clazz.getDeclaredMethods())));
                   
            return new PropertiesMapper(ImmutableMap.copyOf(valueReaders), ImmutableSet.copyOf(propertyWriters), clazz);
        }
        
        
        
        
        private static ImmutableMap<String, BiFunction<WriteWithValues, Object, WriteWithValues>> fetchFieldReaders(ImmutableSet<Field> beanFields) {
            Map<String, BiFunction<WriteWithValues, Object, WriteWithValues>> valueReaders = Maps.newHashMap();
            
            for (Field beanField : beanFields) {
                for (Annotation annotation : beanField.getAnnotations()) {
                    
                    if (annotation.annotationType().getName().equals("javax.persistence.Column")) {
                        for (Method attributeMethod : annotation.annotationType().getDeclaredMethods()) {
                            if (attributeMethod.getName().equalsIgnoreCase("name")) {
                                try {
                                    String columnName = (String) attributeMethod.invoke(annotation);
                                    valueReaders.put(columnName, (WriteWithValues writer, Object entity) -> writer.value(columnName, readBeanField(beanField, entity)));
                                    
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
        private static Optional<Object> readBeanField(Field field, Object persistenceObject) {
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
            
    
     
        private static ImmutableMap<String, BiFunction<WriteWithValues, Object, WriteWithValues>> fetchMethodReaders(ImmutableSet<Method> beanMethods) {
            Map<String, BiFunction<WriteWithValues, Object, WriteWithValues>> valueReaders = Maps.newHashMap();
            
            for (Method beanMethod : beanMethods) {
                for (Annotation annotation : beanMethod.getAnnotations()) {
                    
                    if (annotation.annotationType().getName().equals("javax.persistence.Column")) {
                        for (Method attributeMethod : annotation.annotationType().getDeclaredMethods()) {
                            if (attributeMethod.getName().equalsIgnoreCase("name")) {
                                if ((beanMethod.getParameterTypes().length == 0) && (beanMethod.getReturnType() != null)) {
                                    try {
                                        String columnName = (String) attributeMethod.invoke(annotation);
                                        valueReaders.put(columnName, (WriteWithValues writer, Object entity) -> writer.value(columnName, readBeanMethod(beanMethod, entity)));

                                        break;
    
                                    } catch (ReflectiveOperationException ignore) { }
                                }
                            }
                        }
                    }
                }
            }
            
            return ImmutableMap.copyOf(valueReaders);
        }

         
     
        
        @SuppressWarnings({ "unchecked", "rawtypes" })
        private static Optional<Object> readBeanMethod(Method meth, Object persistenceObject) {
            Object value = null;
            try {
                value = meth.invoke(persistenceObject);
            } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) { 
                System.out.println("errro occured by executing getter-method " + meth + " for object " + persistenceObject + " "  + e.toString());
            }
            
            if (value instanceof Optional) {
                return (Optional) value;
            } else {
                return Optional.ofNullable(value);
            }
        }
            
   
        
        
        
        private ImmutableSet<BiConsumer<Object, Record>> fetchFieldWriters(ImmutableSet<Field> beanFields) {
            Set<BiConsumer<Object, Record>> valueWriters = Sets.newHashSet();
            
            for (Field beanField : beanFields) {
                for (Annotation annotation : beanField.getAnnotations()) {
                    
                    if (annotation.annotationType().getName().equals("javax.persistence.Column")) {
                        for (Method attributeMethod : annotation.annotationType().getDeclaredMethods()) {
                            if (attributeMethod.getName().equalsIgnoreCase("name")) {
                                try {
                                    String columnName = (String) attributeMethod.invoke(annotation);
                                    Class<?> beanFieldClass = beanField.getType();
                                  
                                    for (RecordValueMapper valueMapper : recordValueMappers) {
                                        if (valueMapper.isSupport(beanFieldClass)) {
                                            valueWriters.add((persistenceObject, record) -> valueMapper.apply(record, columnName).ifPresent(value -> writeBeanField(beanField, persistenceObject, value)));
                                            break;
                                        }
                                    }
                                    
                                    break;
                                } catch (ReflectiveOperationException ignore) { }
                            }
                        }
                    }
                }
            }
            
            return ImmutableSet.copyOf(valueWriters);
        }
        
        
        private static void writeBeanField(Field field, Object persistenceObject, Object value) {
            try {
                field.setAccessible(true);
                field.set(persistenceObject, value);
            } catch (IllegalArgumentException | IllegalAccessException e) {
                e.printStackTrace();
            }
        }
    
    
        
        
        private ImmutableSet<BiConsumer<Object, Record>> fetchMethodWriters(ImmutableSet<Method> beanMethods) {
            Set<BiConsumer<Object, Record>> valueWriters = Sets.newHashSet();
            
            for (Method beanMethod : beanMethods) {
                for (Annotation annotation : beanMethod.getAnnotations()) {
                    
                    if (annotation.annotationType().getName().equals("javax.persistence.Column")) {
                        for (Method attributeMethod : annotation.annotationType().getDeclaredMethods()) {
                            if (attributeMethod.getName().equalsIgnoreCase("name")) {
                                try {
                                    String columnName = (String) attributeMethod.invoke(annotation);
                                    if (beanMethod.getParameterTypes().length == 1) {
                                        Class<?> beanFieldClass = beanMethod.getParameterTypes()[0];
                                      
                                        for (RecordValueMapper valueMapper : recordValueMappers) {
                                            if (valueMapper.isSupport(beanFieldClass)) {
                                                valueWriters.add((persistenceObject, record) -> valueMapper.apply(record, columnName).ifPresent(value -> writeBeanMethod(beanMethod, persistenceObject, value)));
                                                break;
                                            }
                                        }
                                    }
                                    
                                    break;
                                } catch (ReflectiveOperationException ignore) { }
                            }
                        }
                    }
                }
            }
            
            return ImmutableSet.copyOf(valueWriters);
        }
        
            
        private static void writeBeanMethod(Method meth, Object persistenceObject, Object value) {
            try {
                meth.setAccessible(true);
                meth.invoke(persistenceObject, value);
            } catch (IllegalArgumentException | IllegalAccessException | InvocationTargetException e) {
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

        
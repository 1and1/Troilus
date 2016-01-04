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



import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import com.google.common.base.Optional;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;


/**
 * bean mapper
 *
 * 
 * @author Jason Westra - edited
 * 12-12-2015: PropertyWriter.readValue(Class<?> clazz, PropertiesSource datasource)
 * 12-12-2015: PropertiesMapperLoader - loads fields more efficiently
 * 12-12-2015: PropertiesMapperLoader - includes super class annotations
 * 12-12-2015: PropertiesMapper - constructor does proactive check on ability to instantiate the field
 * 
 */
class BeanMapper {
    
    private final LoadingCache<Class<?>, PropertiesMapper> propertiesMapperCache = CacheBuilder.newBuilder()
                                                                                               .build(new PropertiesMapperLoader());
    
    
    private static final class PropertiesMapper {
        private final Class<?> clazz;
        private final ImmutableMap<String, PropertyWriter> propertyWriters;
        private final ImmutableMap<String, PropertyReader> propertyReaders;
           
        public PropertiesMapper(ImmutableMap<String, PropertyReader> propertyReaders,  ImmutableMap<String, PropertyWriter> propertyWriters, Class<?> clazz) {
            // Safety check up front for invalid classes that cannot be instantiated
        	// during property mapping
        	try {
				newInstance((Constructor<?>) clazz.getDeclaredConstructor());
			} catch (NoSuchMethodException | SecurityException e) {
				throw new IllegalArgumentException("Invalid class for properties mapping.  It cannot be "
						+ "instantiated through Java Reflection: "+clazz.getName(), e);
			}
        	
        	this.propertyReaders = propertyReaders;
            this.propertyWriters = propertyWriters;
        	this.clazz = clazz;
            
        }
     
        private Class<?> getConcreteClass() {
        	return clazz;
        }
      
        public ImmutableMap<String, Optional<Object>> toValues(Object entity, ImmutableSet<String> namesToMap) {
            Map<String, Optional<Object>> values = Maps.newHashMap();
            
            for (Entry<String, PropertyReader> entry : propertyReaders.entrySet()) {
                if (namesToMap.isEmpty() || namesToMap.contains(entry.getKey())) {
                    Map.Entry<String, Optional<Object>> pair = entry.getValue().readProperty(entity);
                    values.put(pair.getKey(), pair.getValue());
                }
            }

            return ImmutableMap.copyOf(values);
        }

        
        @SuppressWarnings("unchecked")
        public <T> T fromValues(PropertiesSource datasource, ImmutableSet<String> namesToMap) {
            try {
            	T bean = newInstance((Constructor<T>) getConcreteClass().getDeclaredConstructor());
            	
            	// This blows on java.util.List, ImmutableList, etc.
            	// Basically, anything that is not a concrete class with an empty constructor
                //T bean = newInstance((Constructor<T>) clazz.getDeclaredConstructor());
                
                for (Entry<String, PropertyWriter> entry : propertyWriters.entrySet()) {
                    if (namesToMap.isEmpty() || namesToMap.contains(entry.getKey())) {
                        entry.getValue().writeProperty(bean, datasource);
                    }
                }
                    
                return bean;
            } catch (ReflectiveOperationException e) {
                throw new RuntimeException(e);
            }
        }
        
        
        private <T> T newInstance(final Constructor<T> constructor) {
            try {
                return (T) constructor.newInstance();
            } catch (ReflectiveOperationException e) {
                AccessController.doPrivileged(new SetConstructorAccessible<>(constructor));

                try {
                    return (T) constructor.newInstance();
                } catch (ReflectiveOperationException e2) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
    
    
    private static final class SetConstructorAccessible<T> implements PrivilegedAction<Object> {
        private final Constructor<T> constructor;
        
        public SetConstructorAccessible(Constructor<T> constructor) {
            this.constructor = constructor;
        }
        
        @Override
        public Object run() {
            constructor.setAccessible(true);
            return null;
        }
    }
    
    private static final class SetFieldAccessible implements PrivilegedAction<Object> {
        private final Field field;
        
        public SetFieldAccessible(Field field) {
            this.field = field;
        }
        
        @Override
        public Object run() {
            field.setAccessible(true);
            return null;
        }
    }  

    /**
     * @param entity       the entity to map
     * @param namesToMap   the properties names to consider 
     * @return the extracted name-value pairs
     */
    public ImmutableMap<String, Optional<Object>> toValues(Object entity, ImmutableSet<String> namesToMap) {
        return getPropertiesMapper(entity.getClass()).toValues(entity, namesToMap);
    }

    /**
     * @param clazz         the object type
     * @param datasource    the data source to fetch the property values
     * @param propertyNames the property names to be considered 
     * @return the object instance
     */
    public <T> T fromValues(Class<?> clazz, PropertiesSource datasource, ImmutableSet<String> propertyNames) {
        return getPropertiesMapper(clazz).fromValues(datasource, propertyNames);
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

        	// readers
            Map<String, PropertyReader> propertyReaders = Maps.newHashMap();
            propertyReaders.putAll(  loadPropertyReaders(clazz) );
            
            // writers
            Map<String, PropertyWriter> propertyWriters = Maps.newHashMap();
            
            propertyWriters.putAll( loadPropertyWriters(clazz));
        	
        // OLD APIS DID NOT LOAD SUPER CLASS FIELDS	
//            // readers
//            Map<String, PropertyReader> propertyReaders = Maps.newHashMap();
//            propertyReaders.putAll(fetchJEEFieldReaders(ImmutableSet.copyOf(clazz.getFields())));
//            propertyReaders.putAll(fetchJEEFieldReaders(ImmutableSet.copyOf(clazz.getDeclaredFields())));
//            propertyReaders.putAll(fetchCassandraMapperFieldReaders(ImmutableSet.copyOf(clazz.getFields())));
//            propertyReaders.putAll(fetchCassandraMapperFieldReaders(ImmutableSet.copyOf(clazz.getDeclaredFields())));
//            propertyReaders.putAll(fetchFieldReaders(ImmutableSet.copyOf(clazz.getFields())));
//            propertyReaders.putAll(fetchFieldReaders(ImmutableSet.copyOf(clazz.getDeclaredFields())));
//     
//            // writers
//            Map<String, PropertyWriter> propertyWriters = Maps.newHashMap();
//            propertyWriters.putAll(fetchJEEFieldWriters(ImmutableSet.copyOf(clazz.getFields())));
//            propertyWriters.putAll(fetchJEEFieldWriters(ImmutableSet.copyOf(clazz.getDeclaredFields())));
//            propertyWriters.putAll(fetchCassandraMapperFieldWriters(ImmutableSet.copyOf(clazz.getFields())));
//            propertyWriters.putAll(fetchCassandraMapperFieldWriters(ImmutableSet.copyOf(clazz.getDeclaredFields())));
//            propertyWriters.putAll(fetchFieldWriters(ImmutableSet.copyOf(clazz.getFields())));
//            propertyWriters.putAll(fetchFieldWriters(ImmutableSet.copyOf(clazz.getDeclaredFields())));
//                   
            
            return new PropertiesMapper(ImmutableMap.copyOf(propertyReaders), ImmutableMap.copyOf(propertyWriters), clazz);
        }
     
        private static Map<String, PropertyReader> loadPropertyReaders(Class<?> clazz) {
        	Map<String, PropertyReader> fieldMap = new HashMap<String, PropertyReader>();
        	Class<?> fieldsFromClass = clazz;
    		while(fieldsFromClass != null) {
    			ImmutableSet<Field> fields = ImmutableSet.copyOf(fieldsFromClass.getDeclaredFields());
    			try {
    				fieldMap.putAll(fetchCassandraMapperFieldReaders(fields));
    				fieldMap.putAll(fetchFieldReaders(fields));
    				fieldMap.putAll(fetchJEEFieldReaders(fields));
    				
					//Method loaderMethod = PropertiesMapperLoader.class.getDeclaredMethod(methodName, ImmutableSet.class);
					//fieldMap.putAll((Map<? extends String, ? extends PropertyReader>)loaderMethod.invoke(PropertiesMapperLoader.class, ImmutableSet.copyOf(fieldsFromClass.getFields())));
				} catch (Exception e) {
					throw new RuntimeException("Failed to load property reader for class: "+fieldsFromClass.getName(), e);
				}
    			
    			// Search base classes for annotations as well
    			fieldsFromClass = fieldsFromClass.getSuperclass();
    		}
    		return fieldMap;
        }
        
		private static Map<String, PropertyWriter> loadPropertyWriters(Class<?> clazz) {
        	Map<String, PropertyWriter> fieldMap = new HashMap<String, PropertyWriter>();
        	Class<?> fieldsFromClass = clazz;
        	
    		while(fieldsFromClass != null) {
    			ImmutableSet<Field> fields = ImmutableSet.copyOf(fieldsFromClass.getDeclaredFields());
    			
    			try {
    				fieldMap.putAll(fetchCassandraMapperFieldWriters(fields));
    				fieldMap.putAll(fetchFieldWriters(fields));
    				fieldMap.putAll(fetchJEEFieldWriters(fields));
    			} catch (Exception e) {
					throw new RuntimeException("Failed to load property writer "
							+ "for class: "+fieldsFromClass, e);
				}
    			
    			// Search base classes for annotations as well
    			fieldsFromClass = fieldsFromClass.getSuperclass();
    		}
    		return fieldMap;
        }
		
        
        private static ImmutableMap<String, PropertyReader> fetchFieldReaders(ImmutableSet<Field> beanFields) {
            Map<String, PropertyReader> propertyReaders = Maps.newHashMap();
            
            for (Field beanField : beanFields) {
                final net.oneandone.troilus.Field field = beanField.getAnnotation(net.oneandone.troilus.Field.class);
                if (field != null) {
                    propertyReaders.put(field.name(), new PropertyReader(field.name(), beanField));
                }
            }
            
            return ImmutableMap.copyOf(propertyReaders);
        }
        
        
        private static ImmutableMap<String, PropertyReader> fetchJEEFieldReaders(ImmutableSet<Field> beanFields) {
            Map<String, PropertyReader> propertyReaders = Maps.newHashMap();
            
            for (Field beanField : beanFields) {
                for (Annotation annotation : beanField.getAnnotations()) {
                    
                    if (annotation.annotationType().getName().equals("javax.persistence.Column")) {
                        for (Method attributeMethod : annotation.annotationType().getDeclaredMethods()) {
                            if (attributeMethod.getName().equalsIgnoreCase("name")) {
                                try {
                                    final String columnName = (String) attributeMethod.invoke(annotation);
                                    if (columnName != null) {
                                        propertyReaders.put(columnName, new PropertyReader(columnName, beanField));
                                    }
                                    break;

                                } catch (ReflectiveOperationException ignore) { }
                            }
                        }
                    }
                }
            }
            
            return ImmutableMap.copyOf(propertyReaders);
        }
        

        private static ImmutableMap<String, PropertyReader> fetchCassandraMapperFieldReaders(ImmutableSet<java.lang.reflect.Field> beanFields) {
            Map<String, PropertyReader> propertyReaders = Maps.newHashMap();
            
            for (Field beanField : beanFields) {
                for (Annotation annotation : beanField.getAnnotations()) {
                    
                    if (annotation.annotationType().getName().equals("com.datastax.driver.mapping.annotations.Field")) {
                        for (Method attributeMethod : annotation.annotationType().getDeclaredMethods()) {
                            if (attributeMethod.getName().equalsIgnoreCase("name")) {
                                try {
                                    final String columnName = (String) attributeMethod.invoke(annotation);
                                    if (columnName != null) {
                                        propertyReaders.put(columnName, new PropertyReader(columnName, beanField));
                                    }
                                    break;

                                } catch (ReflectiveOperationException ignore) { }
                            }
                        }
                    }
                }
            }
            
            return ImmutableMap.copyOf(propertyReaders);

        }
        
   
        private static Map<String, PropertyWriter> fetchFieldWriters(ImmutableSet<Field> beanFields) {
            Map<String, PropertyWriter> propertyWriters = Maps.newHashMap();
            
            for (Field beanField : beanFields) {
                
                final net.oneandone.troilus.Field field = beanField.getAnnotation(net.oneandone.troilus.Field.class);
                if (field != null) {
                    propertyWriters.put(field.name(), new PropertyWriter(field.name(), beanField));
                }
            }
            
            return ImmutableMap.copyOf(propertyWriters);
        }
        
        
        private static Map<String, PropertyWriter> fetchJEEFieldWriters(ImmutableSet<Field> beanFields) {
            Map<String, PropertyWriter> propertyWriters = Maps.newHashMap();
            
            for (Field beanField : beanFields) {
                for (Annotation annotation : beanField.getAnnotations()) {
                    
                    if (annotation.annotationType().getName().equals("javax.persistence.Column")) {
                        for (Method attributeMethod : annotation.annotationType().getDeclaredMethods()) {
                            if (attributeMethod.getName().equalsIgnoreCase("name")) {
                                try {
                                    String columnName = (String) attributeMethod.invoke(annotation);
                                    propertyWriters.put(columnName, new PropertyWriter(columnName, beanField));
                                } catch (ReflectiveOperationException ignore) { }
                            }
                        }
                    }
                }
            }
            
            return ImmutableMap.copyOf(propertyWriters);
        }

        
                
        private static Map<String, PropertyWriter> fetchCassandraMapperFieldWriters(ImmutableSet<Field> beanFields) {
            Map<String, PropertyWriter> propertyWriters = Maps.newHashMap();

            for (Field beanField : beanFields) {
                for (Annotation annotation : beanField.getAnnotations()) {
                    
                    if (annotation.annotationType().getName().equals("com.datastax.driver.mapping.annotations.Field")) {
                        for (Method attributeMethod : annotation.annotationType().getDeclaredMethods()) {
                            if (attributeMethod.getName().equalsIgnoreCase("name")) {
                                try {
                                    String columnName = (String) attributeMethod.invoke(annotation);
                                    propertyWriters.put(columnName, new PropertyWriter(columnName, beanField));
                                } catch (ReflectiveOperationException ignore) { }
                            }
                        }
                    }
                }
            }
            
            return ImmutableMap.copyOf(propertyWriters);
        }    
    }    
    

    private static class PropertyReader {
        
        private final String fieldName;
        private final java.lang.reflect.Field field;
        private final OptionalWrapper optionalWrapper;
        
        public PropertyReader(String fieldName, java.lang.reflect.Field field) {
            this.fieldName = fieldName;

            this.field = field;
            AccessController.doPrivileged(new SetFieldAccessible(field));
            
            if (Optional.class.isAssignableFrom(field.getType())) {
                this.optionalWrapper = new GuavaOptionalWrapper();
                
            } else if (field.getType().getName().equals("java.util.Optional")) {
                getActualTypeArgument(field.getType(), 0);

                this.optionalWrapper = new JavaOptionalWrapper();
                
            } else {
                this.optionalWrapper = new NonOptionalWrapper();
            }
        }
        
        
        public Entry<String, Optional<Object>> readProperty(Object bean) {
            
            Object value = null;
            try {
                value = field.get(bean);
            } catch (IllegalArgumentException | IllegalAccessException e) { }
            
            return  Maps.immutableEntry(fieldName, optionalWrapper.wrap(value));
        }

        
    
        
        private static interface OptionalWrapper {
            
            Optional<Object> wrap(Object obj);
        }
        private static final class NonOptionalWrapper implements OptionalWrapper {
            
            public Optional<Object> wrap(Object obj) {
                return Optional.fromNullable(obj);
            }
        }
        
        
        private static final class GuavaOptionalWrapper implements OptionalWrapper {
            
            @SuppressWarnings("unchecked")
            public Optional<Object> wrap(Object obj) {
                if (obj == null) {
                    return Optional.absent();
                } else {
                    return (Optional<Object>) obj;
                }
            }
        }

        
        
        private static final class JavaOptionalWrapper implements OptionalWrapper {
            
            private final Method meth;
            
            public JavaOptionalWrapper() {
                try {
                    meth = Class.forName("java.util.Optional").getMethod("get");
                } catch (NoSuchMethodException | SecurityException | ClassNotFoundException e) {
                    throw new RuntimeException(e);
                }
            }
            
            public Optional<Object> wrap(Object obj) {
                try {
                    Object o = meth.invoke(obj);
                    return Optional.fromNullable(o);
                } catch (InvocationTargetException | IllegalAccessException |  SecurityException e) {
                    return Optional.absent();
                }
            }
        }
    }
    

    
    private static Type getActualTypeArgument(Type type, int argIndex) {
        if (type instanceof ParameterizedType) {
            ParameterizedType paramizedType = (ParameterizedType) type;
            Type[] types = paramizedType.getActualTypeArguments();
            if ((types != null) && (types.length > argIndex)) {
                return types[argIndex];
            }
        }
        
        return Object.class;
    }
    
    
    private static class PropertyWriter {
        
        private final String fieldName;
        private final java.lang.reflect.Field field;
        private final OptionalWrapper optionalWrapper;
        
        private Class<?> javaOptionalClass;
        
        public PropertyWriter(String fieldName, java.lang.reflect.Field field) {
            this.fieldName = fieldName;

            this.field = field;
            AccessController.doPrivileged(new SetFieldAccessible(field));

            
            if (Optional.class.isAssignableFrom(field.getType())) {
                this.optionalWrapper = new GuavaOptionalWrapper();
                
           } else if (field.getType().getName().equals("java.util.Optional")) {
                this.optionalWrapper = new JavaOptionalWrapper();

            } else {
                this.optionalWrapper = new NonOptionalWrapper();
            }

            
            Class<?> cl = null;
            try {
                cl = Class.forName("java.util.Optional");
            } catch (ClassNotFoundException | RuntimeException e) { }
            
            javaOptionalClass = cl;
        }

        
        void writeProperty(Object bean, PropertiesSource datasource) {
            
            Optional<Object> optionalValue = readValue(field.getType(), datasource);

            if (optionalValue == null) {
                return;
            }
            
            try {
                field.set(bean, optionalWrapper.unwrap(optionalValue));
            } catch (IllegalArgumentException | IllegalAccessException e) {
                e.printStackTrace();
            }
        }
      
        
        @SuppressWarnings({ "unchecked", "rawtypes" })
        private Optional<Object> readValue(Class<?> clazz, PropertiesSource datasource) {
            Optional<Object> value = Optional.absent();
            
            Type type = field.getGenericType();
            
            if (isOptional(clazz)) {
                type = getActualTypeArgument(type, 0);
            }
                
            if (Set.class.isAssignableFrom(clazz)) {
            //if (ImmutableSet.class.isAssignableFrom(clazz)) {
                value = datasource.read(fieldName, clazz, (Class<Object>) getActualTypeArgument(type, 0));
                if (value.isPresent()) {
                    return Optional.<Object>of(ImmutableSet.copyOf((Collection) value.get()));
                }

            } else if (List.class.isAssignableFrom(clazz)) {
            //else if (ImmutableList.class.isAssignableFrom(clazz)) {
                value =  datasource.read(fieldName, clazz, (Class<Object>) getActualTypeArgument(type, 0));
                if (value.isPresent()) {
                    return Optional.<Object>of(ImmutableList.copyOf((Collection) value.get()));
                }
            } else if (Map.class.isAssignableFrom(clazz)) {
            //else if (ImmutableMap.class.isAssignableFrom(clazz)) {
                value = datasource.read(fieldName, (Class<Object>) getActualTypeArgument(type, 0), (Class<Object>) getActualTypeArgument(field.getGenericType(), 1));
                if (value.isPresent()) {
                    return Optional.<Object>of(ImmutableMap.copyOf((Map) value.get()));
                }

            } else {
                value = datasource.read(fieldName, (Class<Object>) type);
            }
            
            return value;
        }
        
        
        private boolean isOptional(Class<?> clazz) {
            return Optional.class.isAssignableFrom(clazz) || ((javaOptionalClass != null) && (javaOptionalClass.isAssignableFrom(clazz)));
        }
        
        
        private static interface OptionalWrapper {
            
            Object unwrap(Optional<Object> obj);
        }
        
        private static final class NonOptionalWrapper implements OptionalWrapper {
            
            public Object unwrap(Optional<Object> obj) {
                return obj.orNull();
            }
        }

        
        private static final class GuavaOptionalWrapper implements OptionalWrapper {
            
            public Object unwrap(Optional<Object> obj) {
                return Optional.fromNullable(emptyToNull(obj.orNull()));
            }
        }

        private static Object emptyToNull(Object obj) {
            if (obj == null) {
                return null;
            }
            
            if (List.class.isAssignableFrom(obj.getClass())) {
                if (((List<?>) obj).isEmpty()) {
                    obj = null;
                }
            } else if (Set.class.isAssignableFrom(obj.getClass())) {
                if (((Set<?>) obj).isEmpty()) {
                    obj = null;
                }
            } else if (Map.class.isAssignableFrom(obj.getClass())) {
                if (((Map<?, ?>) obj).isEmpty()) {
                    obj = null;
                }
            } else if (byte[].class.isAssignableFrom(obj.getClass())) {
                if (((byte[]) obj).length == 0) {
                    obj = null;
                }
            }
            
            return obj;
        }


        private static final class JavaOptionalWrapper implements OptionalWrapper {
            
            private final Method meth;
            
            public JavaOptionalWrapper() {
                try {
                    meth = Class.forName("java.util.Optional").getDeclaredMethod("ofNullable", Object.class);
                } catch (NoSuchMethodException | SecurityException | ClassNotFoundException e) {
                    throw new RuntimeException(e);
                }
            }
            
            @Override
            public Object unwrap(Optional<Object> obj) { 
                try {
                    Object o = emptyToNull(obj.orNull());
                    return meth.invoke(null, o);
                } catch (InvocationTargetException | IllegalAccessException |  SecurityException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}

        
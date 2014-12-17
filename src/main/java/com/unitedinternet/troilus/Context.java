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
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.BuiltStatement;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

 

public class Context  {
    private final Cache<String, PreparedStatement> statementCache = CacheBuilder.newBuilder().maximumSize(100).build();
    
    private final String table;
    private final Session session;
    private final PropertiesMapperRegistry propertiesMapperRegistry;
    private final ExecutionSpec executionSpec;

    
    public Context(Session session, String table) {
        this(session, new PropertiesMapperRegistry(), table, new ExecutionSpec());
    }

    
    Context(Session session, PropertiesMapperRegistry propertiesMapperRegistry, String table, ExecutionSpec executionSpec) {
        this.table = table;
        this.session = session;
        this.executionSpec = executionSpec;
        this.propertiesMapperRegistry = propertiesMapperRegistry;
    }
 
    
    protected String getTable() {
        return table;
    }
  
    protected ProtocolVersion getProtocolVersion() {
        return session.getCluster().getConfiguration().getProtocolOptions().getProtocolVersionEnum();
    }

    protected PropertiesMapper getPropertiesMapper(Class<?> clazz) {
        return propertiesMapperRegistry.getPropertiesMapper(clazz);
    }
   

    protected interface PropertiesMapper {
        ImmutableList<ValueToInsert> toValues(Object persistenceObject);
        
        <T> T fromValues(Record record);
    }   
    
    public Context withConsistency(ConsistencyLevel consistencyLevel) {
        return new Context(session, propertiesMapperRegistry, table, executionSpec.withConsistency(consistencyLevel));
    }

    public Context withSerialConsistency(ConsistencyLevel consistencyLevel) {
        return new Context(session, propertiesMapperRegistry, table, executionSpec.withSerialConsistency(consistencyLevel));
    }

    public Context withTtl(Duration ttl) {
        return new Context(session, propertiesMapperRegistry, table, executionSpec.withTtl(ttl));        
    }

    public Context withWritetime(long microsSinceEpoch) {
        return new Context(session, propertiesMapperRegistry, table, executionSpec.withWritetime(microsSinceEpoch));        
    }

    public Context ifNotExits() {
        return new Context(session, propertiesMapperRegistry, table, executionSpec.ifNotExits());        
    }
    
    public Optional<ConsistencyLevel> getConsistencyLevel() {
        return executionSpec.getConsistencyLevel();
    }

    public Optional<ConsistencyLevel> getSerialConsistencyLevel() {
        return executionSpec.getSerialConsistencyLevel();
    }


    public Optional<Duration> getTtl() {
        return executionSpec.getTtl();
    }

    public boolean getIfNotExits() {
        return executionSpec.getIfNotExits();
    }
    
  
    
    protected PreparedStatement prepare(BuiltStatement statement) {
        try {
            return statementCache.get(statement.getQueryString(), () -> session.prepare(statement));
        } catch (ExecutionException e) {
            throw Exceptions.unwrapIfNecessary(e);
        }
    }
    
        
    
    protected CompletableFuture<ResultSet> performAsync(Statement statement) {
        
        executionSpec.getConsistencyLevel().ifPresent(cl -> statement.setConsistencyLevel(cl));
        executionSpec.getWritetime().ifPresent(writetimeMicrosSinceEpoch -> statement.setDefaultTimestamp(writetimeMicrosSinceEpoch));
        
        
        ResultSetFuture rsFuture = session.executeAsync(statement);
        return new CompletableDbFuture(rsFuture);
    }
    
    
    
    private static class CompletableDbFuture extends CompletableFuture<ResultSet> {
        
        public CompletableDbFuture(ResultSetFuture rsFuture) {
            
            Runnable resultHandler = () -> { 
                try {
                    complete(rsFuture.get());
                    
                } catch (ExecutionException ee) {
                    completeExceptionally(ee.getCause());
                    
                } catch (InterruptedException | RuntimeException e) {
                    completeExceptionally(e);
                }
            };
            rsFuture.addListener(resultHandler, ForkJoinPool.commonPool());
        }
    }   


    
    private static class ExecutionSpec {
            
        private final Optional<ConsistencyLevel> consistencyLevel;
        private final Optional<ConsistencyLevel> serialConsistencyLevel;
        private final Optional<Duration> ttl;
        private final Optional<Long> writetimeMicrosSinceEpoch;
        private final boolean ifNotExists;
    
        public ExecutionSpec() {
            this(Optional.empty(), 
                 Optional.empty(),
                 Optional.empty(),
                 Optional.empty(), 
                 false);
        }
    
        
        public ExecutionSpec(Optional<ConsistencyLevel> consistencyLevel, 
                             Optional<ConsistencyLevel> serialConsistencyLevel,
                             Optional<Duration> ttl,
                             Optional<Long> writetimeMicrosSinceEpoch,
                             boolean ifNotExists) {
            this.consistencyLevel = consistencyLevel;
            this.serialConsistencyLevel = serialConsistencyLevel;
            this.ttl = ttl;
            this.writetimeMicrosSinceEpoch = writetimeMicrosSinceEpoch;
            this.ifNotExists = ifNotExists;
        }
        
    
        ExecutionSpec withConsistency(ConsistencyLevel consistencyLevel) {
            return new ExecutionSpec(Optional.of(consistencyLevel),
                                     this.serialConsistencyLevel,
                                     this.ttl,
                                     this.writetimeMicrosSinceEpoch,
                                     this.ifNotExists);
        }
    
        

        ExecutionSpec withSerialConsistency(ConsistencyLevel consistencyLevel) {
            return new ExecutionSpec(this.consistencyLevel,
                                     Optional.of(consistencyLevel),
                                     this.ttl,
                                     this.writetimeMicrosSinceEpoch,
                                     this.ifNotExists);
        }
    
        
        ExecutionSpec withTtl(Duration ttl) {
            return new ExecutionSpec(this.consistencyLevel,
                                     this.serialConsistencyLevel,
                                     Optional.of(ttl),
                                     this.writetimeMicrosSinceEpoch,
                                     this.ifNotExists);
        }
    
        
        ExecutionSpec withWritetime(long microsSinceEpoch) {
            return new ExecutionSpec(this.consistencyLevel,
                                     this.serialConsistencyLevel,
                                     this.ttl,
                                     Optional.of(microsSinceEpoch),
                                     this.ifNotExists);
        }
    
        ExecutionSpec ifNotExits() {
            return new ExecutionSpec(this.consistencyLevel,
                                     this.serialConsistencyLevel,                    
                                     this.ttl,
                                     this.writetimeMicrosSinceEpoch,
                                     true);
        }
    
        
        public Optional<ConsistencyLevel> getConsistencyLevel() {
            return consistencyLevel;
        }
    
        
        public Optional<ConsistencyLevel> getSerialConsistencyLevel() {
            return serialConsistencyLevel;
        }
    
    
        public Optional<Duration> getTtl() {
            return ttl;
        }
    
    
        public Optional<Long> getWritetime() {
            return writetimeMicrosSinceEpoch;
        }
        
        public boolean getIfNotExits() {
            return ifNotExists;
        }
    }
    
    
    
    static final class ValueToInsert {
        private final String name;
        private final Optional<Object> value;
        
        @SuppressWarnings("unchecked")
        public ValueToInsert(String name, Object value) {
            this.name = name;
            if (value instanceof Optional) {
                this.value = (Optional) value;
            } else {
                this.value = Optional.ofNullable(value);
            }
        }
        
        public String getName() {
            return name;
        }
        
        public Optional<Object> getValue() {
            return value;
        }
    }


         
    private static class PropertiesMapperRegistry {
        private final LoadingCache<Class<?>, PropertiesMapper> propertiesMapperCache;
        
        public PropertiesMapperRegistry() {        
            this.propertiesMapperCache = CacheBuilder.newBuilder()
                                                     .maximumSize(200)
                                                     .build(new PropertiesMapperLoader());
        }
        
        
        public PropertiesMapper getPropertiesMapper(Class<?> clazz) {
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
        
            
            private static final class PropertiesMapperImpl implements PropertiesMapper {
                private final Class<?> clazz;
                private final ImmutableSet<BiConsumer<Object, Record>> propertyWriters;
                
                private final ImmutableMap<String, Function<Object, Optional<Object>>> valueReaderMap;
                
            
                public PropertiesMapperImpl(ImmutableMap<String, Function<Object, Optional<Object>>> valueReaderMap, ImmutableSet<BiConsumer<Object, Record>> propertyWriters, Class<?> clazz) {
                    this.valueReaderMap = valueReaderMap;
                    this.propertyWriters = propertyWriters;
                    this.clazz = clazz;
                }
             
              
                @Override
                public ImmutableList<ValueToInsert> toValues( Object persistenceObject) {
                    List<ValueToInsert> valuesToInsert = Lists.newArrayList();
                    valueReaderMap.forEach((name, reader) -> reader.apply(persistenceObject).ifPresent(value -> valuesToInsert.add(new ValueToInsert(name, value))));
                    return ImmutableList.copyOf(valuesToInsert);
                }
                
                @SuppressWarnings("unchecked")
                @Override
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
            
            
            
            @Override
            public PropertiesMapper load(Class<?> clazz) throws Exception {
                Map<String, Function<Object, Optional<Object>>> valueReaders = Maps.newHashMap();
                
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
                       
                return new PropertiesMapperImpl(ImmutableMap.copyOf(valueReaders), ImmutableSet.copyOf(propertyWriters), clazz);
            }
            
            
            
            
            private static ImmutableMap<String, Function<Object, Optional<Object>>> fetchFieldReaders(ImmutableSet<Field> beanFields) {
                Map<String, Function<Object, Optional<Object>>> valueReaders = Maps.newHashMap();
                
                for (Field beanField : beanFields) {
                    for (Annotation annotation : beanField.getAnnotations()) {
                        
                        if (annotation.annotationType().getName().equals("javax.persistence.Column")) {
                            for (Method attributeMethod : annotation.annotationType().getDeclaredMethods()) {
                                if (attributeMethod.getName().equalsIgnoreCase("name")) {
                                    try {
                                        String columnName = (String) attributeMethod.invoke(annotation);
                                        valueReaders.put(columnName, persistenceObject -> readBeanField(beanField, persistenceObject));
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
                
        
         
            private static ImmutableMap<String, Function<Object, Optional<Object>>> fetchMethodReaders(ImmutableSet<Method> beanMethods) {
                Map<String, Function<Object, Optional<Object>>> valueReaders = Maps.newHashMap();
                
                for (Method beanMethod : beanMethods) {
                    for (Annotation annotation : beanMethod.getAnnotations()) {
                        
                        if (annotation.annotationType().getName().equals("javax.persistence.Column")) {
                            for (Method attributeMethod : annotation.annotationType().getDeclaredMethods()) {
                                if (attributeMethod.getName().equalsIgnoreCase("name")) {
                                    if ((beanMethod.getParameterTypes().length == 0) && (beanMethod.getReturnType() != null)) {
                                        try {
                                            String columnName = (String) attributeMethod.invoke(annotation);
                                            valueReaders.put(columnName, persistenceObject -> readBeanMethod(beanMethod, persistenceObject));
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
}



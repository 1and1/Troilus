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



import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;

import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;
import com.datastax.driver.core.policies.RetryPolicy;
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
import com.unitedinternet.troilus.interceptor.ListReadQueryPostInterceptor;
import com.unitedinternet.troilus.interceptor.ListReadQueryPreInterceptor;
import com.unitedinternet.troilus.interceptor.QueryInterceptor;
import com.unitedinternet.troilus.interceptor.SingleReadQueryPostInterceptor;
import com.unitedinternet.troilus.interceptor.SingleReadQueryPreInterceptor;
import com.unitedinternet.troilus.interceptor.WriteQueryPreInterceptor;
import com.unitedinternet.troilus.utils.Immutables;
import com.unitedinternet.troilus.utils.TriFunction;




class Context  {
    private final Cache<String, PreparedStatement> preparedStatementsCache = CacheBuilder.newBuilder().maximumSize(100).build();
    private final LoadingCache<String, ColumnMetadata> columnMetadataCache = CacheBuilder.newBuilder().maximumSize(300).build(new ColumnMetadataCacheLoader());
    private final LoadingCache<String, UserType> userTypeCache = CacheBuilder.newBuilder().maximumSize(100).build(new UserTypeCacheLoader());

    
    
    private final UDTValueMapper udtValueMapper = new UDTValueMapper();

    private final String table;
    private final Session session;
    private final BeanMapper entityMapper;
    private final ExecutionSpec executionSpec;
    private final Interceptors interceptors;
    
    
    public Context(Session session, String table) {
        this(session, 
             new BeanMapper(), 
             table,
             new ExecutionSpec(), 
             new Interceptors());
    }
        
    Context(Session session, 
            BeanMapper entityMapper,
            String table, 
            ExecutionSpec executionSpec,
            Interceptors interceptors) {
        this.table = table;
        this.session = session;
        this.executionSpec = executionSpec;
        this.entityMapper = entityMapper;
        this.interceptors = interceptors;
    }
 
    
    public Context withSerialConsistency(ConsistencyLevel consistencyLevel) {
        return new Context(session, 
                           entityMapper,
                           table, 
                           executionSpec.withSerialConsistency(consistencyLevel),
                           interceptors);
    }

    Context withTtl(Duration ttl) {
        return new Context(session, 
                           entityMapper, 
                           table, 
                           executionSpec.withTtl(ttl),
                           interceptors);        
    }

    Context withWritetime(long microsSinceEpoch) {
        return new Context(session, 
                           entityMapper, 
                           table, 
                           executionSpec.withWritetime(microsSinceEpoch),
                           interceptors);        
    }
    
    public Context withEnableTracking() {
        return new Context(session, 
                           entityMapper, 
                           table, 
                           executionSpec.withEnableTracking(),
                           interceptors);        
    }
    
    public Context withDisableTracking() {
        return new Context(session, 
                           entityMapper, 
                           table, 
                           executionSpec.withDisableTracking(),
                           interceptors);        
    }
    
    public Context withRetryPolicy(RetryPolicy policy) {
        return new Context(session, 
                           entityMapper, 
                           table,
                           executionSpec.withRetryPolicy(policy),
                           interceptors);        
    }
    

    public Optional<ConsistencyLevel> getConsistencyLevel() {
        return executionSpec.getConsistencyLevel();
    }

    public Optional<ConsistencyLevel> getSerialConsistencyLevel() {
        return executionSpec.getSerialConsistencyLevel();
    }

    Optional<Duration> getTtl() {
        return executionSpec.getTtl();
    }

    Optional<Long> getWritetime() {
        return executionSpec.getWritetime();
    }

    Optional<Boolean> getEnableTracing() {
        return executionSpec.getEnableTracing();
    }
    
    Optional<RetryPolicy> getRetryPolicy() {
        return executionSpec.getRetryPolicy();
    }

    
    Session getSession() {
        return session;
    }
    
    UDTValueMapper getUDTValueMapper() {
        return udtValueMapper;
    }
  
    String getTable() {
        return table;
    }
  
  
    ColumnMetadata getColumnMetadata(String columnName) {
        try {
            return columnMetadataCache.get(columnName);
        } catch (ExecutionException e) {
            throw new RuntimeException(e.getCause());
        }
    }

    
    UserType getUserType(String usertypeName) {
        try {
            return userTypeCache.get(usertypeName);
        } catch (ExecutionException e) {
            throw new RuntimeException(e.getCause());
        }
    }


    
    ImmutableMap<String, Optional<Object>> toValues(Object entity) {
        return entityMapper.toValues(entity);
    }

 
    
    Object toStatementValue(String name, Object value) {
        if (isNullOrEmpty(value)) {
            return null;
        } 
        
        DataType dataType = getColumnMetadata(name).getType();
        return (isBuildInType(dataType)) ? value : toUdtValue(getColumnMetadata(name).getType(), value);
    }
    

    boolean isBuildInType(DataType dataType) {        
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
    

    
    Object toUdtValue(DataType datatype, Object value) {
        return udtValueMapper.toUdtValue(datatype, value);
    }
    
    
    private boolean isNullOrEmpty(Object value) {
        return (value == null) || 
               (Collection.class.isAssignableFrom(value.getClass()) && ((Collection<?>) value).isEmpty()) || 
               (Map.class.isAssignableFrom(value.getClass()) && ((Map<?, ?>) value).isEmpty());
    }
       

    <T> T fromValues(Class<?> clazz, TriFunction<String, Class<?>, Class<?>, Optional<?>> datasource) {
        return entityMapper.fromValues(clazz, datasource);
    }
    
    
    Context interceptor(QueryInterceptor interceptor) {
        return new Context(session, 
                           entityMapper, 
                           table,      
                           executionSpec,  
                           interceptors.add(interceptor));

    }
    

    
    <T extends QueryInterceptor> ImmutableList<T> getInterceptors(Class<T> clazz) {
        return interceptors.getInterceptors(clazz);
    }
    
    
    
    public Context withConsistency(ConsistencyLevel consistencyLevel) {
        return new Context(session, 
                           entityMapper, 
                           table, 
                           executionSpec.withConsistency(consistencyLevel),
                           interceptors);
    }

       
    Cache<String, PreparedStatement> getPreparedStatementsCache() {
        return preparedStatementsCache;
    }
    
       
    static class Interceptors {
        
        private final ImmutableMap<Class<? extends QueryInterceptor>, ImmutableList<QueryInterceptor>> typeInterceptorMap;
        
        public Interceptors() {
            this(ImmutableMap.of());
        }
        
        private Interceptors(ImmutableMap<Class<? extends QueryInterceptor>, ImmutableList<QueryInterceptor>> interceptors) {
            this.typeInterceptorMap = interceptors;
        }

        Interceptors add(QueryInterceptor interceptor) {
            Interceptors interceptors = this;
            interceptors =  interceptors.addIfMatch(WriteQueryPreInterceptor.class, interceptor);
            interceptors =  interceptors.addIfMatch(SingleReadQueryPreInterceptor.class, interceptor);
            interceptors =  interceptors.addIfMatch(SingleReadQueryPostInterceptor.class, interceptor);
            interceptors =  interceptors.addIfMatch(ListReadQueryPreInterceptor.class, interceptor);
            interceptors =  interceptors.addIfMatch(ListReadQueryPostInterceptor.class, interceptor);

            return interceptors;
        }
        
        private Interceptors addIfMatch(Class<? extends QueryInterceptor> clazz, QueryInterceptor interceptor) {
            if (clazz.isAssignableFrom(interceptor.getClass())) {
                ImmutableList<QueryInterceptor> interceptorList = typeInterceptorMap.get(clazz);
                if (interceptorList == null) {
                    interceptorList = ImmutableList.of(interceptor);
                } else {
                    interceptorList = Immutables.merge(interceptorList, interceptor);
                }
                
                return new Interceptors(Immutables.merge(typeInterceptorMap, clazz, interceptorList));    
                
            } else {
                return this;
            }
        }
        
        
        @SuppressWarnings("unchecked")
        public <T extends QueryInterceptor> ImmutableList<T> getInterceptors(Class<T> clazz) {
            ImmutableList<T> list = (ImmutableList<T>) typeInterceptorMap.get(clazz);
            if (list == null) {
                return ImmutableList.of();
            } else {
                return list;
            }
        }
    }

    
    private static class ExecutionSpec {
            
        private final Optional<ConsistencyLevel> consistencyLevel;
        private final Optional<ConsistencyLevel> serialConsistencyLevel;
        private final Optional<Duration> ttl;
        private final Optional<Long> writetimeMicrosSinceEpoch;
        private final Optional<Boolean> enableTracing;
        private final Optional<RetryPolicy> retryPolicy;
        
    
        public ExecutionSpec() {
            this(Optional.empty(), 
                 Optional.empty(),
                 Optional.empty(),
                 Optional.empty(),
                 Optional.empty(),
                 Optional.empty());
        }
    
        
        public ExecutionSpec(Optional<ConsistencyLevel> consistencyLevel, 
                             Optional<ConsistencyLevel> serialConsistencyLevel,
                             Optional<Duration> ttl,
                             Optional<Long> writetimeMicrosSinceEpoch,
                             Optional<Boolean> enableTracking,
                             Optional<RetryPolicy> retryPolicy) {
            this.consistencyLevel = consistencyLevel;
            this.serialConsistencyLevel = serialConsistencyLevel;
            this.ttl = ttl;
            this.writetimeMicrosSinceEpoch = writetimeMicrosSinceEpoch;
            this.enableTracing = enableTracking;
            this.retryPolicy = retryPolicy;
        }
        
    
        ExecutionSpec withConsistency(ConsistencyLevel consistencyLevel) {
            return new ExecutionSpec(Optional.of(consistencyLevel),
                                     this.serialConsistencyLevel,
                                     this.ttl,
                                     this.writetimeMicrosSinceEpoch,
                                     this.enableTracing,
                                     this.retryPolicy);
        }
    
        

        ExecutionSpec withSerialConsistency(ConsistencyLevel consistencyLevel) {
            return new ExecutionSpec(this.consistencyLevel,
                                     Optional.of(consistencyLevel),
                                     this.ttl,
                                     this.writetimeMicrosSinceEpoch,
                                     this.enableTracing,
                                     this.retryPolicy);
        }
    
        
        ExecutionSpec withTtl(Duration ttl) {
            return new ExecutionSpec(this.consistencyLevel,
                                     this.serialConsistencyLevel,
                                     Optional.of(ttl),
                                     this.writetimeMicrosSinceEpoch,
                                     this.enableTracing,
                                     this.retryPolicy);
        }
    
        
        ExecutionSpec withWritetime(long microsSinceEpoch) {
            return new ExecutionSpec(this.consistencyLevel,
                                     this.serialConsistencyLevel,
                                     this.ttl,
                                     Optional.of(microsSinceEpoch),
                                     this.enableTracing,
                                     this.retryPolicy);
        }

        
        ExecutionSpec withEnableTracking() {
            return new ExecutionSpec(this.consistencyLevel,
                                     this.serialConsistencyLevel,
                                     this.ttl,
                                     this.writetimeMicrosSinceEpoch,
                                     Optional.of(true),
                                     this.retryPolicy);
        }

        ExecutionSpec withDisableTracking() {
            return new ExecutionSpec(this.consistencyLevel,
                                     this.serialConsistencyLevel,
                                     this.ttl,
                                     this.writetimeMicrosSinceEpoch,
                                     Optional.of(false),
                                     this.retryPolicy);
        }

        
        ExecutionSpec withRetryPolicy(RetryPolicy policy) {
            return new ExecutionSpec(this.consistencyLevel,
                                     this.serialConsistencyLevel,
                                     this.ttl,
                                     this.writetimeMicrosSinceEpoch,
                                     this.enableTracing,
                                     Optional.of(policy));
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


        public Optional<Boolean> getEnableTracing() {
            return enableTracing;
        }
        
        public Optional<RetryPolicy> getRetryPolicy() {
            return retryPolicy;
        }
    }
    
    
    


    private final class ColumnMetadataCacheLoader extends CacheLoader<String, ColumnMetadata> {
        
        @Override
        public ColumnMetadata load(String columnName) throws Exception {
            return  session.getCluster().getMetadata().getKeyspace(session.getLoggedKeyspace()).getTable(table).getColumn(columnName);
        }
    }
    
    

    private final class UserTypeCacheLoader extends CacheLoader<String, UserType> {
        
        @Override
        public UserType load(String usertypeName) throws Exception {
            return session.getCluster().getMetadata().getKeyspace(session.getLoggedKeyspace()).getUserType(usertypeName);
        }
    }    
    
    
    /**
     * UDTValueMapper
     */
    class UDTValueMapper {
        
        private UDTValueMapper() {  }
        
        
        private ProtocolVersion getProtocolVersion() {
            return session.getCluster().getConfiguration().getProtocolOptions().getProtocolVersionEnum();
        }
        
        
        
        public Optional<?> fromUdtValue(DataType datatype, 
                                        UDTValue udtValue, Class<?> fieldtype1, 
                                        Class<?> fieldtype2,
                                        String fieldname) {
            
            // build-in type 
            if (isBuildInType(datatype)) {
                return Optional.ofNullable(datatype.deserialize(udtValue.getBytesUnsafe(fieldname), getProtocolVersion()));
            
                
            // udt collection    
            } else if (datatype.isCollection()) {
                Class<?> type = datatype.getName().asJavaClass();
               
                // set
                if (Set.class.isAssignableFrom(type)) {
                    return Optional.ofNullable(fromUdtValues(datatype.getTypeArguments().get(0), 
                                                             ImmutableSet.copyOf(udtValue.getSet(fieldname, UDTValue.class)), 
                                                             fieldtype1)); 
                    
                // list
                } else if (List.class.isAssignableFrom(type)) {
                    return Optional.ofNullable(fromUdtValues(datatype.getTypeArguments().get(0), 
                                                             ImmutableList.copyOf(udtValue.getList(fieldname, UDTValue.class)),
                                                             fieldtype1)); 

                // map
                } else {
                    if (isBuildInType(datatype.getTypeArguments().get(0))) {
                        return Optional.ofNullable(fromUdtValues(datatype.getTypeArguments().get(0), 
                                                                 datatype.getTypeArguments().get(1), 
                                                                 ImmutableMap.<Object, Object>copyOf(udtValue.getMap(fieldname, fieldtype1, UDTValue.class)), 
                                                                 fieldtype1, 
                                                                 fieldtype2));

                    } else if (isBuildInType(datatype.getTypeArguments().get(1))) {
                        return Optional.ofNullable(fromUdtValues(datatype.getTypeArguments().get(0), 
                                                                 datatype.getTypeArguments().get(1), 
                                                                 ImmutableMap.<Object, Object>copyOf(udtValue.getMap(fieldname, UDTValue.class, fieldtype2)), 
                                                                 fieldtype1, 
                                                                 fieldtype2));
                        
                    } else {
                        return Optional.ofNullable(fromUdtValues(datatype.getTypeArguments().get(0), 
                                                                 datatype.getTypeArguments().get(1), 
                                                                 ImmutableMap.<Object, Object>copyOf(udtValue.getMap(fieldname, UDTValue.class, UDTValue.class)),
                                                                 fieldtype1, 
                                                                 fieldtype2));
                    }
                }
                            
            // udt    
            } else {
                return Optional.ofNullable(fromUdtValue(datatype, 
                                                        udtValue, 
                                                        fieldtype1));
            }
        }
        

        
        public <T> T fromUdtValue(DataType datatype, UDTValue udtValue, Class<T> type) {
            return fromValues(type, (name, clazz1, clazz2) -> fromUdtValue(((UserType) datatype).getFieldType(name), 
                                                                           udtValue, 
                                                                           clazz1, 
                                                                           clazz2,
                                                                           name));
        }


        
        <T> ImmutableSet<T> fromUdtValues(DataType datatype, 
                                          ImmutableSet<UDTValue> udtValues, 
                                          Class<T> type) {
            Set<T> elements = Sets.newHashSet();
            
            for (UDTValue elementUdtValue : udtValues) {
                T element = fromValues(type, (name, clazz1, clazz2) -> fromUdtValue(((UserType) datatype).getFieldType(name), 
                                                                                    elementUdtValue, 
                                                                                    clazz1, 
                                                                                    clazz2, 
                                                                                    name));
                elements.add(element);
            }
            
            return ImmutableSet.copyOf(elements);
        }


        
        
        public <T> ImmutableList<T> fromUdtValues(DataType datatype, 
                                                  ImmutableList<UDTValue> udtValues, 
                                                  Class<T> type) {
            List<T> elements = Lists.newArrayList();
            
            for (UDTValue elementUdtValue : udtValues) {
                T element = fromValues(type, (name, clazz1, clazz2) -> fromUdtValue(((UserType) datatype).getFieldType(name), 
                                                                                     elementUdtValue, 
                                                                                     clazz1,
                                                                                     clazz2, 
                                                                                     name));
                elements.add(element);
            }
            
            return ImmutableList.copyOf(elements);
        }

        
        
        @SuppressWarnings("unchecked")
        public <K, V> ImmutableMap<K, V> fromUdtValues(DataType keyDatatype, 
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
                    keyElement = fromValues(keystype, (name, clazz1, clazz2) -> fromUdtValue(((UserType) keyDatatype).getFieldType(name), 
                                                                                             (UDTValue) entry.getKey(), 
                                                                                             clazz1, 
                                                                                             clazz2, 
                                                                                             name));
                }
                
                V valueElement;
                if (valuesType.isAssignableFrom(entry.getValue().getClass())) {
                    valueElement = (V) entry.getValue(); 
                } else {
                    valueElement = fromValues(valuesType, (name, clazz1, clazz2) -> fromUdtValue(((UserType) valueDatatype).getFieldType(name), 
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
        public Object toUdtValue(DataType datatype, Object value) {
            
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
                           udt.add(toUdtValue(elementDataType, element));
                       }
                   }
                   
                   return ImmutableSet.copyOf(udt);
                   
               // list 
               } else if (List.class.isAssignableFrom(datatype.getName().asJavaClass())) {
                   DataType elementDataType = datatype.getTypeArguments().get(0);
                   
                   List<Object> udt = Lists.newArrayList();
                   if (value != null) {
                       for (Object element : (List<Object>) value) {
                           udt.add(toUdtValue(elementDataType, element));
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
                             udt.put(toUdtValue(keyDataType, entry.getKey()), 
                                     toUdtValue(valueDataType, entry.getValue()));
                       }
                   
                   }
                   return ImmutableMap.copyOf(udt);  
               }
        
               
            // udt
            } else {
                if (value == null) {
                    return value;
                    
                } else {
                    UserType usertype = getUserType(((UserType) datatype).getTypeName());
                    UDTValue udtValue = usertype.newValue();
                    
                    for (Entry<String, Optional<Object>> entry : toValues(value).entrySet()) {
                        DataType fieldType = usertype.getFieldType(entry.getKey());
                                
                        if (entry.getValue().isPresent()) {
                            Object vl = entry.getValue().get();
                            
                            if (!isBuildInType(usertype.getFieldType(entry.getKey()))) {
                                vl = toUdtValue(fieldType, vl);
                            }
                            
                            udtValue.setBytesUnsafe(entry.getKey(), fieldType.serialize(vl, getProtocolVersion()));
                        }
                    }
                    
                    return udtValue;
                }
            }
        }
    }    
}



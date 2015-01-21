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
package net.oneandone.troilus;



import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;

import net.oneandone.troilus.interceptor.QueryInterceptor;

import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.UserType;
import com.datastax.driver.core.policies.RetryPolicy;
import com.datastax.driver.core.querybuilder.BuiltStatement;
import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;




class Context {
    
    private final String table;
    private final Session session;
    private final ExecutionSpec executionSpec;
    private final InterceptorRegistry interceptorRegistry;
    private final BeanMapper beanMapper;
    private final UDTValueMapper udtValueMapper;

    private final Executor executors;
    private final Cache<String, PreparedStatement> preparedStatementsCache;
    private final Cache<String, ColumnMetadata> columnMetadataCache;
    private final LoadingCache<String, UserType> userTypeCache;

    
    Context(Session session, String table) {
        this(session, 
             table,
             new BeanMapper());
    }

    private Context(Session session, String table, BeanMapper beanMapper) {
        this(session, 
             table,
             new ExecutionSpec(), 
             new InterceptorRegistry(),
             beanMapper,
             new UDTValueMapper(session.getCluster().getConfiguration().getProtocolOptions().getProtocolVersionEnum(), beanMapper),
             CacheBuilder.newBuilder().maximumSize(150).<String, PreparedStatement>build(),
             CacheBuilder.newBuilder().maximumSize(300).<String, ColumnMetadata>build(),
             CacheBuilder.newBuilder().maximumSize(100).<String, UserType>build(new UserTypeCacheLoader(session)),
             newTaskExecutor());
    }
    
    
    private static Executor newTaskExecutor() {

        try {
            Method commonPoolMeth = ForkJoinPool.class.getMethod("commonPool");  // Java8 method
            return (Executor) commonPoolMeth.invoke(ForkJoinPool.class);
        } catch (NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
            return Executors.newCachedThreadPool();
        }
    }
    
    private Context(Session session, 
                    String table, 
                    ExecutionSpec executionSpec,
                    InterceptorRegistry interceptorRegistry,
                    BeanMapper beanMapper,
                    UDTValueMapper udtValueMapper,
                    Cache<String, PreparedStatement> preparedStatementsCache, 
                    Cache<String, ColumnMetadata> columnMetadataCache,
                    LoadingCache<String, UserType> userTypeCache,
                    Executor executors) {
        this.table = table;
        this.session = session;
        this.executionSpec = executionSpec;
        this.interceptorRegistry = interceptorRegistry;
        this.beanMapper = beanMapper;
        this.udtValueMapper = udtValueMapper;
        this.preparedStatementsCache = preparedStatementsCache;
        this.columnMetadataCache = columnMetadataCache;
        this.userTypeCache = userTypeCache;
        this.executors = executors;
    }
 
    
    Context withInterceptor(QueryInterceptor interceptor) {
        return new Context(session, 
                           table,      
                           executionSpec,  
                           interceptorRegistry.withInterceptor(interceptor),
                           beanMapper,
                           udtValueMapper,
                           preparedStatementsCache,
                           columnMetadataCache,
                           userTypeCache,
                           executors);

    }
    
    Context withSerialConsistency(ConsistencyLevel consistencyLevel) {
        return new Context(session, 
                           table, 
                           executionSpec.withSerialConsistency(consistencyLevel),
                           interceptorRegistry,
                           beanMapper,
                           udtValueMapper,
                           preparedStatementsCache,
                           columnMetadataCache,
                           userTypeCache,
                           executors);
    }

    Context withTtl(int ttlSec) {
        return new Context(session, 
                           table, 
                           executionSpec.withTtl(ttlSec),
                           interceptorRegistry,
                           beanMapper,
                           udtValueMapper,
                           preparedStatementsCache,
                           columnMetadataCache,
                           userTypeCache,
                           executors);        
    }

    Context withWritetime(long microsSinceEpoch) {
        return new Context(session, 
                           table, 
                           executionSpec.withWritetime(microsSinceEpoch),
                           interceptorRegistry,
                           beanMapper,
                           udtValueMapper,
                           preparedStatementsCache,
                           columnMetadataCache,
                           userTypeCache,
                           executors);        
    }
    
    Context withEnableTracking() {
        return new Context(session, 
                           table, 
                           executionSpec.withEnableTracking(),
                           interceptorRegistry,
                           beanMapper,
                           udtValueMapper,
                           preparedStatementsCache,
                           columnMetadataCache,
                           userTypeCache,
                           executors);        
    }
    
    Context withDisableTracking() {
        return new Context(session, 
                           table, 
                           executionSpec.withDisableTracking(),
                           interceptorRegistry,
                           beanMapper,
                           udtValueMapper,
                           preparedStatementsCache,
                           columnMetadataCache,
                           userTypeCache,
                           executors);        
    }
    
    Context withRetryPolicy(RetryPolicy policy) {
        return new Context(session, 
                           table,
                           executionSpec.withRetryPolicy(policy),
                           interceptorRegistry,
                           beanMapper,
                           udtValueMapper,
                           preparedStatementsCache,
                           columnMetadataCache,
                           userTypeCache,
                           executors);        
    }
    
    Context withConsistency(ConsistencyLevel consistencyLevel) {
        return new Context(session, 
                           table, 
                           executionSpec.withConsistency(consistencyLevel),
                           interceptorRegistry,
                           beanMapper,
                           udtValueMapper,
                           preparedStatementsCache,
                           columnMetadataCache,
                           userTypeCache,
                           executors);
    }
    
    

    ConsistencyLevel getConsistencyLevel() {
        return executionSpec.getConsistencyLevel();
    }

    ConsistencyLevel getSerialConsistencyLevel() {
        return executionSpec.getSerialConsistencyLevel();
    }

    Integer getTtlSec() {
        return executionSpec.getTtl();
    }

    Long getWritetime() {
        return executionSpec.getWritetime();
    }

    Boolean getEnableTracing() {
        return executionSpec.getEnableTracing();
    }
    
    RetryPolicy getRetryPolicy() {
        return executionSpec.getRetryPolicy();
    }   
    
    
    
    
        
    Session getSession() {
        return session;
    }
    
    String getTable() {
        return table;
    }

    Executor getTaskExecutor() {
        return executors;
    }
    
    BeanMapper getBeanMapper() {
        return beanMapper;
    }
     
    UDTValueMapper getUDTValueMapper() {
        return udtValueMapper;
    }

    InterceptorRegistry getInterceptorRegistry() {
        return interceptorRegistry;
    }
    
    ProtocolVersion getProtocolVersion() {
        return getSession().getCluster().getConfiguration().getProtocolOptions().getProtocolVersionEnum();
    }


    ColumnMetadata getColumnMetadata(String columnName) {
        ColumnMetadata metadata = columnMetadataCache.getIfPresent(columnName);
        if (metadata == null) {
            metadata = session.getCluster().getMetadata().getKeyspace(session.getLoggedKeyspace()).getTable(table).getColumn(columnName);
            if (metadata == null) {
                throw new RuntimeException("table " + session.getLoggedKeyspace() + "." + table + " does not support column '" + columnName + "'");
            }
            columnMetadataCache.put(columnName, metadata);
        }

        return metadata;
    }
  
    
    ImmutableList<Object> toStatementValues(String name, ImmutableList<Object> values) {
        List<Object> result = Lists.newArrayList(); 

        for (Object value : values) {
            result.add(toStatementValue(name, value));
        }
        
        return ImmutableList.copyOf(result);
    }

    Object toStatementValue(String name, Object value) {
        if (isNullOrEmpty(value)) {
            return null;
        } 
        
        DataType dataType = getColumnMetadata(name).getType();
        
        // build in
        if (UDTValueMapper.isBuildInType(dataType)) {
            
            // enum
            if (isTextDataType(dataType) && Enum.class.isAssignableFrom(value.getClass())) {
                return value.toString();
            }
            
            return value;
         
        // udt    
        } else {
            return udtValueMapper.toUdtValue(userTypeCache, getColumnMetadata(name).getType(), value);
        }
    }
    
    
    boolean isTextDataType(DataType dataType) {
        return dataType.equals(DataType.text()) ||
               dataType.equals(DataType.ascii()) ||
               dataType.equals(DataType.varchar());
    }
 
    
    private boolean isNullOrEmpty(Object value) {
        return (value == null) || 
               (Collection.class.isAssignableFrom(value.getClass()) && ((Collection<?>) value).isEmpty()) || 
               (Map.class.isAssignableFrom(value.getClass()) && ((Map<?, ?>) value).isEmpty());
    }
    
    

    PreparedStatement prepare(BuiltStatement statement) {
        PreparedStatement preparedStatment = preparedStatementsCache.getIfPresent(statement.getQueryString());
        if (preparedStatment == null) {
            preparedStatment = session.prepare(statement);
            preparedStatementsCache.put(statement.getQueryString(), preparedStatment);
        }

        return preparedStatment;
    }
    
 
    
    
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                          .add("table", table)
                          .add("execution-spec", executionSpec)
                          .add("interceptorRegistry", interceptorRegistry)
                          .add("preparedStatementsCache", printCache(preparedStatementsCache))
                          .toString();
    }
    
    
    private String printCache(Cache<String, ?> cache) {
        return Joiner.on(", ").withKeyValueSeparator("=").join(cache.asMap());
    }
    
    
    protected static class ExecutionSpec {
        
        private final ConsistencyLevel consistencyLevel;
        private final ConsistencyLevel serialConsistencyLevel;
        private final Integer ttlSec;
        private final Long writetimeMicrosSinceEpoch;
        private final Boolean enableTracing;
        private final RetryPolicy retryPolicy;
        
    
        public ExecutionSpec() {
            this(null, 
                 null,
                 null,
                 null,
                 null,
                 null);
        }
    
        
        public ExecutionSpec(ConsistencyLevel consistencyLevel, 
                             ConsistencyLevel serialConsistencyLevel,
                             Integer ttlSec,
                             Long writetimeMicrosSinceEpoch,
                             Boolean enableTracking,
                             RetryPolicy retryPolicy) {
            this.consistencyLevel = consistencyLevel;
            this.serialConsistencyLevel = serialConsistencyLevel;
            this.ttlSec = ttlSec;
            this.writetimeMicrosSinceEpoch = writetimeMicrosSinceEpoch;
            this.enableTracing = enableTracking;
            this.retryPolicy = retryPolicy;
        }
        
    
        ExecutionSpec withConsistency(ConsistencyLevel consistencyLevel) {
            return new ExecutionSpec(consistencyLevel,
                                     this.serialConsistencyLevel,
                                     this.ttlSec,
                                     this.writetimeMicrosSinceEpoch,
                                     this.enableTracing,
                                     this.retryPolicy);
        }
    
        

        ExecutionSpec withSerialConsistency(ConsistencyLevel consistencyLevel) {
            return new ExecutionSpec(this.consistencyLevel,
                                     consistencyLevel,
                                     this.ttlSec,
                                     this.writetimeMicrosSinceEpoch,
                                     this.enableTracing,
                                     this.retryPolicy);
        }
    
        
        ExecutionSpec withTtl(int ttlSec) {
            return new ExecutionSpec(this.consistencyLevel,
                                     this.serialConsistencyLevel,
                                     ttlSec,
                                     this.writetimeMicrosSinceEpoch,
                                     this.enableTracing,
                                     this.retryPolicy);
        }
    
        
        ExecutionSpec withWritetime(long microsSinceEpoch) {
            return new ExecutionSpec(this.consistencyLevel,
                                     this.serialConsistencyLevel,
                                     this.ttlSec,
                                     microsSinceEpoch,
                                     this.enableTracing,
                                     this.retryPolicy);
        }

        
        ExecutionSpec withEnableTracking() {
            return new ExecutionSpec(this.consistencyLevel,
                                     this.serialConsistencyLevel,
                                     this.ttlSec,
                                     this.writetimeMicrosSinceEpoch,
                                     true,
                                     this.retryPolicy);
        }

        ExecutionSpec withDisableTracking() {
            return new ExecutionSpec(this.consistencyLevel,
                                     this.serialConsistencyLevel,
                                     this.ttlSec,
                                     this.writetimeMicrosSinceEpoch,
                                     false,
                                     this.retryPolicy);
        }

        
        ExecutionSpec withRetryPolicy(RetryPolicy policy) {
            return new ExecutionSpec(this.consistencyLevel,
                                     this.serialConsistencyLevel,
                                     this.ttlSec,
                                     this.writetimeMicrosSinceEpoch,
                                     this.enableTracing,
                                     policy);
        }

        public ConsistencyLevel getConsistencyLevel() {
            return consistencyLevel;
        }
    
        
        public ConsistencyLevel getSerialConsistencyLevel() {
            return serialConsistencyLevel;
        }
    
    
        public Integer getTtl() {
            return ttlSec;
        }
    
    
        public Long getWritetime() {
            return writetimeMicrosSinceEpoch;
        }


        public Boolean getEnableTracing() {
            return enableTracing;
        }
        
        public RetryPolicy getRetryPolicy() {
            return retryPolicy;
        }
        
        @Override
        public String toString() {
            return MoreObjects.toStringHelper("spec")
                              .add("consistencyLevel", consistencyLevel)
                              .add("serialConsistencyLevel", serialConsistencyLevel)
                              .add("ttlSec", ttlSec)
                              .add("writetimeMicrosSinceEpoch", writetimeMicrosSinceEpoch)
                              .add("enableTracing", enableTracing)
                              .add("retryPolicy", retryPolicy)
                              .toString();
        }
    }
    
    

    private static final class UserTypeCacheLoader extends CacheLoader<String, UserType> {
        private final Session session;
        
        public UserTypeCacheLoader(Session session) {
            this.session = session;
        }

        @Override
        public UserType load(String usertypeName) throws Exception {
            return session.getCluster().getMetadata().getKeyspace(session.getLoggedKeyspace()).getUserType(usertypeName);
        }
    }    
}



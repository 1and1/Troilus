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



import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import com.datastax.driver.core.TableMetadata;
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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;



/**
 * the context
 * 
 */
class Context {
    
    private final ExecutionSpec executionSpec;
    private final InterceptorRegistry interceptorRegistry;
    private final BeanMapper beanMapper;
    private final Executor executors;
    private final DBSession dbSession;
    
    /**
     * @param session    the underlying session
     * @param tablename  the tablename
     */
    Context(Session session, String tablename) {
        this(session, tablename, new BeanMapper());
    }
    
    private Context(Session session, String tablename, BeanMapper beanMapper) {
        this(new DBSession(session, tablename, beanMapper), 
             new ExecutionSpec(), 
             new InterceptorRegistry(),
             beanMapper,
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
    
    private Context(DBSession dbSession, 
                    ExecutionSpec executionSpec,
                    InterceptorRegistry interceptorRegistry,
                    BeanMapper beanMapper,
                    Executor executors) {
        this.dbSession = dbSession;
        this.executionSpec = executionSpec;
        this.interceptorRegistry = interceptorRegistry;
        this.executors = executors;
        this.beanMapper = beanMapper;
    }
 
    
    Context withInterceptor(QueryInterceptor interceptor) {
        return new Context(dbSession,
                           executionSpec,  
                           interceptorRegistry.withInterceptor(interceptor),
                           beanMapper,
                           executors);

    }
    
    Context withSerialConsistency(ConsistencyLevel consistencyLevel) {
        return new Context(dbSession,
                           executionSpec.withSerialConsistency(consistencyLevel),
                           interceptorRegistry,
                           beanMapper,
                           executors);
    }

    Context withTtl(int ttlSec) {
        return new Context(dbSession,
                           executionSpec.withTtl(ttlSec),
                           interceptorRegistry,
                           beanMapper,
                           executors);        
    }

    Context withWritetime(long microsSinceEpoch) {
        return new Context(dbSession,
                           executionSpec.withWritetime(microsSinceEpoch),
                           interceptorRegistry,
                           beanMapper,
                           executors);        
    }
    
    Context withEnableTracking() {
        return new Context(dbSession,
                           executionSpec.withEnableTracking(),
                           interceptorRegistry,
                           beanMapper,
                           executors);        
    }
    
    Context withDisableTracking() {
        return new Context(dbSession,
                           executionSpec.withDisableTracking(),
                           interceptorRegistry,
                           beanMapper,
                           executors);        
    }
    
    Context withRetryPolicy(RetryPolicy policy) {
        return new Context(dbSession,
                           executionSpec.withRetryPolicy(policy),
                           interceptorRegistry,
                           beanMapper,
                           executors);        
    }
    
    Context withConsistency(ConsistencyLevel consistencyLevel) {
        return new Context(dbSession,
                           executionSpec.withConsistency(consistencyLevel),
                           interceptorRegistry,
                           beanMapper,
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
    
    DBSession getDbSession() {
        return dbSession;
    }
    
    RetryPolicy getRetryPolicy() {
        return executionSpec.getRetryPolicy();
    }   
    
    Executor getTaskExecutor() {
        return executors;
    }
    
    BeanMapper getBeanMapper() {
        return beanMapper;
    }
     
    InterceptorRegistry getInterceptorRegistry() {
        return interceptorRegistry;
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
        
        DataType dataType = dbSession.getColumnMetadata(name).getType();
        
        // build in
        if (UDTValueMapper.isBuildInType(dataType)) {
            
            // enum
            if (isTextDataType(dataType) && Enum.class.isAssignableFrom(value.getClass())) {
                return value.toString();
            }
            
            // byte buffer (byte[])
            if (dataType.equals(DataType.blob()) && byte[].class.isAssignableFrom(value.getClass())) {
                return ByteBuffer.wrap((byte[]) value);
            }

            
            return value;
         
        // udt    
        } else {
            return dbSession.getUDTValueMapper().toUdtValue(dbSession.getUserTypeCache(), dbSession.getColumnMetadata(name).getType(), value);
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
        
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                          .add("dsession", dbSession)
                          .add("execution-spec", executionSpec)
                          .add("interceptorRegistry", interceptorRegistry)
                          .toString();
    }
   
    
     
    static class ExecutionSpec {
        
        private final ConsistencyLevel consistencyLevel;
        private final ConsistencyLevel serialConsistencyLevel;
        private final Integer ttlSec;
        private final Long writetimeMicrosSinceEpoch;
        private final Boolean enableTracing;
        private final RetryPolicy retryPolicy;
        
        ExecutionSpec() {
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
    
    

    
    
    static class DBSession  {
        private final Session session;
        private final String tablename;
        private final TableMetadata tableMetadata;
        private final ImmutableSet<String> columnNames; 
        private final UDTValueMapper udtValueMapper;
        private final Cache<String, PreparedStatement> preparedStatementsCache;
        private final LoadingCache<String, UserType> userTypeCache;

        public DBSession(Session session, String tablename, BeanMapper beanMapper) {
            this.session = session;
            this.tablename = tablename;
            this.tableMetadata = loadTableMetadata(session, tablename);
            this.columnNames = loadColumnNames(tableMetadata);
            
            this.udtValueMapper = new UDTValueMapper(session.getCluster().getConfiguration().getProtocolOptions().getProtocolVersionEnum(), beanMapper);
            this.preparedStatementsCache = CacheBuilder.newBuilder().maximumSize(150).<String, PreparedStatement>build();
            this.userTypeCache = CacheBuilder.newBuilder().maximumSize(100).<String, UserType>build(new UserTypeCacheLoader(session));
        }

        private static TableMetadata loadTableMetadata(Session session, String tablename) {
            TableMetadata tableMetadata = session.getCluster().getMetadata().getKeyspace(session.getLoggedKeyspace()).getTable(tablename);
            if (tableMetadata == null) {
                throw new RuntimeException("table " + session.getLoggedKeyspace() + "." + tablename + " is not defined in keyspace '" + session.getLoggedKeyspace() + "'");
            }

            return tableMetadata;
        }

        private static ImmutableSet<String> loadColumnNames(TableMetadata tableMetadata) {
            Set<String> columnNames = Sets.newHashSet();
            for (ColumnMetadata columnMetadata : tableMetadata.getColumns()) {
                columnNames.add(columnMetadata.getName());
            }
            
            return ImmutableSet.copyOf(columnNames);
        }

        Session getSession() {
            return session;
        }
        
        String getTablename() {
            return tablename;
        }
        
        ProtocolVersion getProtocolVersion() {
            return getSession().getCluster().getConfiguration().getProtocolOptions().getProtocolVersionEnum();
        }
        
        LoadingCache<String, UserType> getUserTypeCache() {
            return userTypeCache; 
        }
                
        UDTValueMapper getUDTValueMapper() {
            return udtValueMapper;
        }
        
        ImmutableSet<String> getColumnNames() {
            return columnNames;
        }
        
        ColumnMetadata getColumnMetadata(String columnName) {
            ColumnMetadata metadata = tableMetadata.getColumn(columnName);
            if (metadata == null) {
                throw new RuntimeException("table " + session.getLoggedKeyspace() + "." + tablename + " does not support column '" + columnName + "'");
            }
            return metadata;
        }
        
        PreparedStatement prepare(BuiltStatement statement) {
            PreparedStatement preparedStatment = preparedStatementsCache.getIfPresent(statement.getQueryString());
            if (preparedStatment == null) {
//                preparedStatment = session.prepareAsync(statement);
                preparedStatment = session.prepare(statement);
                preparedStatementsCache.put(statement.getQueryString(), preparedStatment);
            }

            return preparedStatment;
        }
        
        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                              .add("tablename", tablename)
                              .add("preparedStatementsCache", printCache(preparedStatementsCache))
                              .toString();
        }
        
        private String printCache(Cache<String, ?> cache) {
            return Joiner.on(", ").withKeyValueSeparator("=").join(cache.asMap());
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
}
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
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;

import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.UserType;
import com.datastax.driver.core.querybuilder.BuiltStatement;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;

 

public class Context  {
    private final Cache<String, PreparedStatement> statementCache = CacheBuilder.newBuilder().maximumSize(100).build();
    private final LoadingCache<String, ColumnMetadata> columnMetadataCache = CacheBuilder.newBuilder().maximumSize(300).build(new ColumnMetadataCacheLoader());
    private final LoadingCache<String, UserType> userTypeCache = CacheBuilder.newBuilder().maximumSize(100).build(new UserTypeCacheLoader());
    
    
    private final String table;
    private final Session session;
    private final BeanMapper entityMapper;
    private final ExecutionSpec executionSpec;

    
    public Context(Session session, String table) {
        this(session, new BeanMapper(), table, new ExecutionSpec());
    }

    
    Context(Session session, BeanMapper entityMapper, String table, ExecutionSpec executionSpec) {
        this.table = table;
        this.session = session;
        this.executionSpec = executionSpec;
        this.entityMapper = entityMapper;
    }
 
  
    protected String getTable() {
        return table;
    }
  
    protected ProtocolVersion getProtocolVersion() {
        return session.getCluster().getConfiguration().getProtocolOptions().getProtocolVersionEnum();
    }
    
    protected ColumnMetadata getColumnMetadata(String columnName) {
        try {
            return columnMetadataCache.get(columnName);
        } catch (ExecutionException e) {
            throw new RuntimeException(e.getCause());
        }
    }

    
    protected UserType getUserType(String usertypeName) {
        try {
            return userTypeCache.get(usertypeName);
        } catch (ExecutionException e) {
            throw new RuntimeException(e.getCause());
        }
    }

 
    

    public boolean isBuildInType(DataType dataType) {
        
        if (dataType.isCollection()) {
            for (DataType type : dataType.getTypeArguments()) {
                if (!isBuildinType(type)) {
                    return false;
                }
            }
            return true;

        } else {
            return isBuildinType(dataType);
        }
    }
  
    private boolean isBuildinType(DataType type) {
        return DataType.allPrimitiveTypes().contains(type);
    }
    
    
    protected ImmutableMap<String, Optional<Object>> toValues(Object entity) {
        return entityMapper.toValues(entity);
    }

    
  
    
    
    protected <T> T fromValues(Class<?> clazz, TriFunction<String, Class<?>, Class<?>, Optional<?>> datasource) {
        return entityMapper.fromValues(clazz, datasource);
    }
    
    
    public Context withConsistency(ConsistencyLevel consistencyLevel) {
        return new Context(session, entityMapper, table, executionSpec.withConsistency(consistencyLevel));
    }

    public Context withSerialConsistency(ConsistencyLevel consistencyLevel) {
        return new Context(session, entityMapper, table, executionSpec.withSerialConsistency(consistencyLevel));
    }

    public Context withTtl(Duration ttl) {
        return new Context(session, entityMapper, table, executionSpec.withTtl(ttl));        
    }

    public Context withWritetime(long microsSinceEpoch) {
        return new Context(session, entityMapper, table, executionSpec.withWritetime(microsSinceEpoch));        
    }
    
    public Context withEnableTracking() {
        return new Context(session, entityMapper, table, executionSpec.withEnableTracking());        
    }
    
    public Context withDisableTracking() {
        return new Context(session, entityMapper, table, executionSpec.withDisableTracking());        
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
        executionSpec.getEnableTracing().ifPresent(enable -> {
                                                                if (enable) {
                                                                    statement.enableTracing();
                                                                } else {
                                                                    statement.disableTracing(); 
                                                                }
                                                             });
        
        
        
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
        private final Optional<Boolean> enableTracing;
        
    
        public ExecutionSpec() {
            this(Optional.empty(), 
                 Optional.empty(),
                 Optional.empty(),
                 Optional.empty(),
                 Optional.empty());
        }
    
        
        public ExecutionSpec(Optional<ConsistencyLevel> consistencyLevel, 
                             Optional<ConsistencyLevel> serialConsistencyLevel,
                             Optional<Duration> ttl,
                             Optional<Long> writetimeMicrosSinceEpoch,
                             Optional<Boolean> enableTracking) {
            this.consistencyLevel = consistencyLevel;
            this.serialConsistencyLevel = serialConsistencyLevel;
            this.ttl = ttl;
            this.writetimeMicrosSinceEpoch = writetimeMicrosSinceEpoch;
            this.enableTracing = enableTracking;
        }
        
    
        ExecutionSpec withConsistency(ConsistencyLevel consistencyLevel) {
            return new ExecutionSpec(Optional.of(consistencyLevel),
                                     this.serialConsistencyLevel,
                                     this.ttl,
                                     this.writetimeMicrosSinceEpoch,
                                     this.enableTracing);
        }
    
        

        ExecutionSpec withSerialConsistency(ConsistencyLevel consistencyLevel) {
            return new ExecutionSpec(this.consistencyLevel,
                                     Optional.of(consistencyLevel),
                                     this.ttl,
                                     this.writetimeMicrosSinceEpoch,
                                     this.enableTracing);
        }
    
        
        ExecutionSpec withTtl(Duration ttl) {
            return new ExecutionSpec(this.consistencyLevel,
                                     this.serialConsistencyLevel,
                                     Optional.of(ttl),
                                     this.writetimeMicrosSinceEpoch,
                                     this.enableTracing);
        }
    
        
        ExecutionSpec withWritetime(long microsSinceEpoch) {
            return new ExecutionSpec(this.consistencyLevel,
                                     this.serialConsistencyLevel,
                                     this.ttl,
                                     Optional.of(microsSinceEpoch),
                                     this.enableTracing);
        }

        
        ExecutionSpec withEnableTracking() {
            return new ExecutionSpec(this.consistencyLevel,
                                     this.serialConsistencyLevel,
                                     this.ttl,
                                     this.writetimeMicrosSinceEpoch,
                                     Optional.of(true));
        }

        ExecutionSpec withDisableTracking() {
            return new ExecutionSpec(this.consistencyLevel,
                                     this.serialConsistencyLevel,
                                     this.ttl,
                                     this.writetimeMicrosSinceEpoch,
                                     Optional.of(false));
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
}



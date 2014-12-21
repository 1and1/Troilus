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

 

public class Context  {
    private final Cache<String, PreparedStatement> statementCache = CacheBuilder.newBuilder().maximumSize(100).build();
    
    private final String table;
    private final Session session;
    private final EntityMapper entityMapper;
    private final ExecutionSpec executionSpec;

    
    public Context(Session session, String table) {
        this(session, new EntityMapper(), table, new ExecutionSpec());
    }

    
    Context(Session session, EntityMapper entityMapper, String table, ExecutionSpec executionSpec) {
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


    protected WriteWithValues readPropertiesAndEnhanceWrite(WriteWithValues write, Object entity) {
        return entityMapper.readPropertiesAndEnhanceWrite(write, entity);
    }

    
    protected <T> T fromValues(Class<?> clazz, Record record) {
        return entityMapper.fromValues(clazz, record);
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

    public Context ifNotExits() {
        return new Context(session, entityMapper, table, executionSpec.ifNotExits());        
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
    
    
    
  
}



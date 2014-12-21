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
import com.unitedinternet.troilus.PropertiesMapperRegistry.PropertiesMapper;

 

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
}



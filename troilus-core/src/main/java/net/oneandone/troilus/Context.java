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
import java.time.Duration;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;

import net.oneandone.troilus.interceptor.QueryInterceptor;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.RetryPolicy;
import com.google.common.base.MoreObjects;




/**
 * the context
 * 
 */
public class Context  {
    private final ExecutionSpec executionSpec;
    private final InterceptorRegistry interceptorRegistry;
    private final UDTValueMapper udtValueMapper;
    private final BeanMapper beanMapper;
    private final Executor executor;
    private final MetadataCatalog catalog;
    private final DBSession dbSession;

    
    /**
     * @param session    the underlying session
     */
    Context(final Session session) {
        this(session, new BeanMapper());
    }
    
    private Context(final Session session, final BeanMapper beanMapper) {
        this(session, beanMapper, newTaskExecutor());
    }
    
    private Context(final Session session, final BeanMapper beanMapper, final Executor executor) {
        this(session, beanMapper, executor, new MetadataCatalog(session));
    }
    
    private Context(final Session session, 
                    final BeanMapper beanMapper,
                    final Executor executor,
                    final MetadataCatalog catalog) {
        this(session, beanMapper, executor, catalog, new DBSession(session, catalog, beanMapper));
    }
    
    private Context(final Session session, 
                    final BeanMapper beanMapper, 
                    final Executor executor,
                    final MetadataCatalog catalog,
                    final DBSession dbSession) {
        this(dbSession, 
             catalog,
             new ExecutionSpecImpl(), 
             new InterceptorRegistry(),
             beanMapper,
             new UDTValueMapper(dbSession.getProtocolVersion(), catalog, beanMapper),
             executor);
    }
    
    private Context(final DBSession dbSession, 
                    final MetadataCatalog catalog,
                    final ExecutionSpec executionSpec,
                    final InterceptorRegistry interceptorRegistry,
                    final BeanMapper beanMapper,
                    final UDTValueMapper udtValueMapper,
                    final Executor executors) {
        this.dbSession = dbSession;
        this.catalog = catalog;
        this.executionSpec = executionSpec;
        this.interceptorRegistry = interceptorRegistry;
        this.executor = executors;
        this.beanMapper = beanMapper;
        this.udtValueMapper = udtValueMapper;
    }
 
  
    
    private static Executor newTaskExecutor() {
        try {
            Method commonPoolMeth = ForkJoinPool.class.getMethod("commonPool");  // Java8 method
            return (Executor) commonPoolMeth.invoke(ForkJoinPool.class);
        } catch (NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
            return Executors.newCachedThreadPool();
        }
    }
    
    
    Context withInterceptor(final QueryInterceptor interceptor) {
        return new Context(dbSession,
                           catalog,
                           executionSpec,  
                           interceptorRegistry.withInterceptor(interceptor),
                           beanMapper,
                           udtValueMapper,
                           executor);

    }
    
    Context withSerialConsistency(final ConsistencyLevel consistencyLevel) {
        return new Context(dbSession,
                           catalog,
                           executionSpec.withSerialConsistency(consistencyLevel),
                           interceptorRegistry,
                           beanMapper,
                           udtValueMapper,
                           executor);
    }

    Context withTtl(final Duration ttlSec) {
        return new Context(dbSession,
                           catalog,
                           executionSpec.withTtl(ttlSec),
                           interceptorRegistry,
                           beanMapper,
                           udtValueMapper,
                           executor);        
    }

    Context withWritetime(final long microsSinceEpoch) {
        return new Context(dbSession,
                           catalog,
                           executionSpec.withWritetime(microsSinceEpoch),
                           interceptorRegistry,
                           beanMapper,
                           udtValueMapper,
                           executor);        
    }
    
    Context withTracking() {
        return new Context(dbSession,
                           catalog,
                           executionSpec.withTracking(),
                           interceptorRegistry,
                           beanMapper,
                           udtValueMapper,
                           executor);        
    }
    
    Context withoutTracking() {
        return new Context(dbSession,
                           catalog,
                           executionSpec.withoutTracking(),
                           interceptorRegistry,
                           beanMapper,
                           udtValueMapper,
                           executor);        
    }
    
    Context withRetryPolicy(final RetryPolicy policy) {
        return new Context(dbSession,
                           catalog,
                           executionSpec.withRetryPolicy(policy),
                           interceptorRegistry,
                           beanMapper,
                           udtValueMapper,
                           executor);        
    }
    
    Context withConsistency(final ConsistencyLevel consistencyLevel) {
        return new Context(dbSession,
                           catalog,
                           executionSpec.withConsistency(consistencyLevel),
                           interceptorRegistry,
                           beanMapper,
                           udtValueMapper,
                           executor);
    }
    
    DBSession getDefaultDbSession() {
        return dbSession;
    }
    
    MetadataCatalog getCatalog() {
        return catalog;
    }
    
    UDTValueMapper getUDTValueMapper() {
        return udtValueMapper;
    }
    
    ExecutionSpec getExecutionSpec() {
        return executionSpec;
    }
    
    Executor getTaskExecutor() {
        return executor;
    }
    
    BeanMapper getBeanMapper() {
        return beanMapper;
    }
     
    InterceptorRegistry getInterceptorRegistry() {
        return interceptorRegistry;
    }
  
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                          .add("dsession", dbSession)
                          .add("execution-spec", executionSpec)
                          .add("interceptorRegistry", interceptorRegistry)
                          .toString();
    }
   
    
     
    private static class ExecutionSpecImpl implements ExecutionSpec {
        private final ConsistencyLevel consistencyLevel;
        private final ConsistencyLevel serialConsistencyLevel;
        private final Duration ttl;
        private final Long writetimeMicrosSinceEpoch;
        private final Boolean enableTracing;
        private final RetryPolicy retryPolicy;
        
        ExecutionSpecImpl() {
            this(null, 
                 null,
                 null,
                 null,
                 null,
                 null);
        }
    
        public ExecutionSpecImpl(final ConsistencyLevel consistencyLevel, 
                                 final ConsistencyLevel serialConsistencyLevel,
                                 final Duration ttl,
                                 final Long writetimeMicrosSinceEpoch,
                                 final Boolean enableTracking,
                                 final RetryPolicy retryPolicy) {
            this.consistencyLevel = consistencyLevel;
            this.serialConsistencyLevel = serialConsistencyLevel;
            this.ttl = ttl;
            this.writetimeMicrosSinceEpoch = writetimeMicrosSinceEpoch;
            this.enableTracing = enableTracking;
            this.retryPolicy = retryPolicy;
        }
        
        public ExecutionSpec withConsistency(final ConsistencyLevel consistencyLevel) {
            return new ExecutionSpecImpl(consistencyLevel,
                                         this.serialConsistencyLevel,
                                         this.ttl,
                                         this.writetimeMicrosSinceEpoch,
                                         this.enableTracing,
                                         this.retryPolicy);
        }
    
        public ExecutionSpec withSerialConsistency(final ConsistencyLevel consistencyLevel) {
            return new ExecutionSpecImpl(this.consistencyLevel,
                                         consistencyLevel,
                                         this.ttl,
                                         this.writetimeMicrosSinceEpoch,
                                         this.enableTracing,
                                         this.retryPolicy);
        }
        
        public ExecutionSpec withTtl(final Duration ttlSec) {
            return new ExecutionSpecImpl(this.consistencyLevel,
                                         this.serialConsistencyLevel,
                                         ttlSec,
                                         this.writetimeMicrosSinceEpoch,
                                         this.enableTracing,
                                         this.retryPolicy);
        }
        
        public ExecutionSpec withWritetime(final long microsSinceEpoch) {
            return new ExecutionSpecImpl(this.consistencyLevel,
                                         this.serialConsistencyLevel,
                                         this.ttl,
                                         microsSinceEpoch,
                                         this.enableTracing,
                                         this.retryPolicy);
        }

        public ExecutionSpec withTracking() {
            return new ExecutionSpecImpl(this.consistencyLevel,
                                         this.serialConsistencyLevel,
                                         this.ttl,
                                         this.writetimeMicrosSinceEpoch,
                                         true,
                                         this.retryPolicy);
        }

        public ExecutionSpec withoutTracking() {
            return new ExecutionSpecImpl(this.consistencyLevel,
                                         this.serialConsistencyLevel,
                                         this.ttl,
                                         this.writetimeMicrosSinceEpoch,
                                         false,
                                         this.retryPolicy);
        }
        
        public ExecutionSpec withRetryPolicy(final RetryPolicy policy) {
            return new ExecutionSpecImpl(this.consistencyLevel,
                                         this.serialConsistencyLevel,
                                         this.ttl,
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
    
        public Duration getTtl() {
            return ttl;
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
                              .add("ttlSec", ttl)
                              .add("writetimeMicrosSinceEpoch", writetimeMicrosSinceEpoch)
                              .add("enableTracing", enableTracing)
                              .add("retryPolicy", retryPolicy)
                              .toString();
        }
    }
}
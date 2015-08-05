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
    Context(Session session) {
        this(session, new BeanMapper());
    }
    
    private Context(Session session, BeanMapper beanMapper) {
        this(session, beanMapper, newTaskExecutor());
    }
    
    private Context(Session session, BeanMapper beanMapper, Executor executor) {
        this(session, beanMapper, executor, new MetadataCatalog(session));
    }
    
    private Context(Session session, BeanMapper beanMapper, Executor executor, MetadataCatalog catalog) {
        this(session, beanMapper, executor, catalog, new DBSession(session, catalog, beanMapper));
    }
    
    private Context(Session session, BeanMapper beanMapper, Executor executor, MetadataCatalog catalog, DBSession dbSession) {
        this(dbSession, 
             catalog,
             new ExecutionSpecImpl(), 
             new InterceptorRegistry(),
             beanMapper,
             new UDTValueMapper(dbSession.getProtocolVersion(), catalog, beanMapper),
             executor);
    }
    
    private Context(DBSession dbSession, 
                    MetadataCatalog catalog,
                    ExecutionSpec executionSpec,
                    InterceptorRegistry interceptorRegistry,
                    BeanMapper beanMapper,
                    UDTValueMapper udtValueMapper,
                    Executor executors) {
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
    
    
    Context withInterceptor(QueryInterceptor interceptor) {
        return new Context(dbSession,
                           catalog,
                           executionSpec,  
                           interceptorRegistry.withInterceptor(interceptor),
                           beanMapper,
                           udtValueMapper,
                           executor);

    }
    
    Context withSerialConsistency(ConsistencyLevel consistencyLevel) {
        return new Context(dbSession,
                           catalog,
                           executionSpec.withSerialConsistency(consistencyLevel),
                           interceptorRegistry,
                           beanMapper,
                           udtValueMapper,
                           executor);
    }

    Context withTtl(int ttlSec) {
        return new Context(dbSession,
                           catalog,
                           executionSpec.withTtl(ttlSec),
                           interceptorRegistry,
                           beanMapper,
                           udtValueMapper,
                           executor);        
    }

    Context withWritetime(long microsSinceEpoch) {
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
    
    Context withRetryPolicy(RetryPolicy policy) {
        return new Context(dbSession,
                           catalog,
                           executionSpec.withRetryPolicy(policy),
                           interceptorRegistry,
                           beanMapper,
                           udtValueMapper,
                           executor);        
    }
    
    Context withConsistency(ConsistencyLevel consistencyLevel) {
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
        private final Integer ttlSec;
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
    
        public ExecutionSpecImpl(ConsistencyLevel consistencyLevel, 
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
        
        public ExecutionSpec withConsistency(ConsistencyLevel consistencyLevel) {
            return new ExecutionSpecImpl(consistencyLevel,
                                         this.serialConsistencyLevel,
                                         this.ttlSec,
                                         this.writetimeMicrosSinceEpoch,
                                         this.enableTracing,
                                         this.retryPolicy);
        }
    
        public ExecutionSpec withSerialConsistency(ConsistencyLevel consistencyLevel) {
            return new ExecutionSpecImpl(this.consistencyLevel,
                                         consistencyLevel,
                                         this.ttlSec,
                                         this.writetimeMicrosSinceEpoch,
                                         this.enableTracing,
                                         this.retryPolicy);
        }
        
        public ExecutionSpec withTtl(int ttlSec) {
            return new ExecutionSpecImpl(this.consistencyLevel,
                                         this.serialConsistencyLevel,
                                         ttlSec,
                                         this.writetimeMicrosSinceEpoch,
                                         this.enableTracing,
                                         this.retryPolicy);
        }
        
        public ExecutionSpec withWritetime(long microsSinceEpoch) {
            return new ExecutionSpecImpl(this.consistencyLevel,
                                         this.serialConsistencyLevel,
                                         this.ttlSec,
                                         microsSinceEpoch,
                                         this.enableTracing,
                                         this.retryPolicy);
        }

        public ExecutionSpec withTracking() {
            return new ExecutionSpecImpl(this.consistencyLevel,
                                         this.serialConsistencyLevel,
                                         this.ttlSec,
                                         this.writetimeMicrosSinceEpoch,
                                         true,
                                         this.retryPolicy);
        }

        public ExecutionSpec withoutTracking() {
            return new ExecutionSpecImpl(this.consistencyLevel,
                                         this.serialConsistencyLevel,
                                         this.ttlSec,
                                         this.writetimeMicrosSinceEpoch,
                                         false,
                                         this.retryPolicy);
        }
        
        public ExecutionSpec withRetryPolicy(RetryPolicy policy) {
            return new ExecutionSpecImpl(this.consistencyLevel,
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
}
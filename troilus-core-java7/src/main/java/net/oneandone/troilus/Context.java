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
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;

import net.oneandone.troilus.interceptor.QueryInterceptor;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.RetryPolicy;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;




/**
 * the context
 * 
 */
public class Context  {
 
    private final ExecutionSpec executionSpec;
    private final InterceptorRegistry interceptorRegistry;
    private final BeanMapper beanMapper;
    private final Executor executor;
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
        this(new DBSession(session, beanMapper, executor), 
             new ExecutionSpecImpl(), 
             new InterceptorRegistry(),
             beanMapper,
             executor);
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
        this.executor = executors;
        this.beanMapper = beanMapper;
    }
 
    
    Context withInterceptor(QueryInterceptor interceptor) {
        return new Context(dbSession,
                           executionSpec,  
                           interceptorRegistry.withInterceptor(interceptor),
                           beanMapper,
                           executor);

    }
    
    Context withSerialConsistency(ConsistencyLevel consistencyLevel) {
        return new Context(dbSession,
                           executionSpec.withSerialConsistency(consistencyLevel),
                           interceptorRegistry,
                           beanMapper,
                           executor);
    }

    Context withTtl(int ttlSec) {
        return new Context(dbSession,
                           executionSpec.withTtl(ttlSec),
                           interceptorRegistry,
                           beanMapper,
                           executor);        
    }

    Context withWritetime(long microsSinceEpoch) {
        return new Context(dbSession,
                           executionSpec.withWritetime(microsSinceEpoch),
                           interceptorRegistry,
                           beanMapper,
                           executor);        
    }
    
    Context withTracking() {
        return new Context(dbSession,
                           executionSpec.withTracking(),
                           interceptorRegistry,
                           beanMapper,
                           executor);        
    }
    
    Context withoutTracking() {
        return new Context(dbSession,
                           executionSpec.withoutTracking(),
                           interceptorRegistry,
                           beanMapper,
                           executor);        
    }
    
    Context withRetryPolicy(RetryPolicy policy) {
        return new Context(dbSession,
                           executionSpec.withRetryPolicy(policy),
                           interceptorRegistry,
                           beanMapper,
                           executor);        
    }
    
    Context withConsistency(ConsistencyLevel consistencyLevel) {
        return new Context(dbSession,
                           executionSpec.withConsistency(consistencyLevel),
                           interceptorRegistry,
                           beanMapper,
                           executor);
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
    
    
    DBSession getDefaultDbSession() {
        return dbSession;
    }
    
    ExecutionSpec getExecutionSpec() {
        return executionSpec;
    }
    
    RetryPolicy getRetryPolicy() {
        return executionSpec.getRetryPolicy();
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
        
    ImmutableList<Object> toStatementValues(Tablename tablename, String name, ImmutableList<Object> values) {
        List<Object> result = Lists.newArrayList(); 

        for (Object value : values) {
            result.add(getDefaultDbSession().toStatementValue(tablename, name, value));
        }
        
        return ImmutableList.copyOf(result);
    }

  
  
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                          .add("dsession", dbSession)
                          .add("execution-spec", executionSpec)
                          .add("interceptorRegistry", interceptorRegistry)
                          .toString();
    }
   
    
     
    static class ExecutionSpecImpl implements ExecutionSpec {
        
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
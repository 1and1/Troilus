/*
 * Copyright (c) 2015 1&1 Internet AG, Germany, http://www.1und1.de
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


import java.util.List;
import java.util.concurrent.ExecutionException;

import com.google.common.base.Joiner;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.unitedinternet.troilus.interceptor.QueryInterceptor;



/**
 * InterceptorRegistry
 */
class InterceptorRegistry {

    private final ImmutableList<QueryInterceptor> interceptors;
    
    
    private final LoadingCache<Class<? extends QueryInterceptor>, ImmutableList<QueryInterceptor>> interceptorsByTypeCache = CacheBuilder.newBuilder().build(new InterceptorsByTypeLoader());

    private final class InterceptorsByTypeLoader extends CacheLoader<Class<?>, ImmutableList<QueryInterceptor>> {

        @Override
        public ImmutableList<QueryInterceptor> load(Class<?> clazz) throws Exception {
            List<QueryInterceptor> result = Lists.newArrayList();
            
            for (QueryInterceptor interceptor : interceptors) {
                if (clazz.isAssignableFrom(interceptor.getClass())) {   
                    result.add(interceptor);
                }
            }
            
            return ImmutableList.copyOf(result);
        }
    }
    

    /**
     * empty interceptor registry
     */
    public InterceptorRegistry() {
        this(ImmutableList.<QueryInterceptor>of());
    }

    
    /**
     * interceptor registry
     * @param interceptors the interceptors to register
     */
    private InterceptorRegistry(ImmutableList<QueryInterceptor> interceptors) {
        this.interceptors = interceptors;
    }

    
    /**
     * @param interceptor the interceptor to register
     * @return a new instance of the interceptor registry which also includes the new interceptor
     */
    <T extends QueryInterceptor> InterceptorRegistry withInterceptor(T interceptor) {
        return new InterceptorRegistry(Immutables.merge(interceptors, interceptor));
    }


    /**
     * @param clazz interceptor type
     * @return all registered interceptors which implements the requested type
     */
    @SuppressWarnings("unchecked")
    public <T extends QueryInterceptor> ImmutableList<T> getInterceptors(Class<T> clazz) {
        try {
            return (ImmutableList<T>) interceptorsByTypeCache.get(clazz);
        } catch (ExecutionException e) {
            throw new RuntimeException(e.getCause());
        }            
    }
    
    
    @Override
    public String toString() {
        return Joiner.on("\n").join(interceptors);
    }
}
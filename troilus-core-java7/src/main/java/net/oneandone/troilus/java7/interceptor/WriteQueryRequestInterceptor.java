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
package net.oneandone.troilus.java7.interceptor;

import com.google.common.util.concurrent.ListenableFuture;

import net.oneandone.troilus.interceptor.QueryInterceptor;









/**
 * Interceptor which will be executed before performing a write (update, insert) query  
 */   
public interface WriteQueryRequestInterceptor extends QueryInterceptor {
    
    /**
     * @param data the request data
     * @return the (modified) request data
     */
    ListenableFuture<WriteQueryData> onWriteRequestAsync(WriteQueryData data); 
}

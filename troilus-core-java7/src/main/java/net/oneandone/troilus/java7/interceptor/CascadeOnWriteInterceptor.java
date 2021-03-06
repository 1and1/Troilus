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


import net.oneandone.troilus.interceptor.QueryInterceptor;
import net.oneandone.troilus.java7.Batchable;

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;



/**
 * Interceptor which adds cascading statements to the write statement by using the cql batch command with write ahead.
 * Please consider that the interceptor will fail, if a non-batchable write operation e.g. ifNotExist(...), onlyIf(...)
 * is performed   
 */
public interface CascadeOnWriteInterceptor extends QueryInterceptor {
       
    /**
     * @param queryData  the write query data
     * @return the additional, cascading statements to execute
     */
    ListenableFuture<ImmutableSet<? extends Batchable<?>>> onWriteAsync(WriteQueryData queryData); 
}


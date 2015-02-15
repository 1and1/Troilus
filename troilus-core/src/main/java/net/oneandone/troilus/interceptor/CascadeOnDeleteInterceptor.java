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
package net.oneandone.troilus.interceptor;

import java.util.concurrent.CompletableFuture;



import com.google.common.collect.ImmutableSet;

import net.oneandone.troilus.Batchable;
import net.oneandone.troilus.interceptor.DeleteQueryData;



/**
 * Interceptor which adds cascading statements to the delete statement by using the cql batch command with write ahead.
 * Please consider that the interceptor will fail, if a non-batchable delete operation e.g. ifExist(...)
 * is performed   
 */
public interface CascadeOnDeleteInterceptor extends QueryInterceptor {
       
    /**
     * @param queryData  the delete query data
     * @return the additional, cascading statements to execute
     */
    CompletableFuture<ImmutableSet<? extends Batchable>> onDelete(DeleteQueryData queryData);
}
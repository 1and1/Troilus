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

import net.oneandone.troilus.ResultList;
import net.oneandone.troilus.interceptor.QueryInterceptor;
import net.oneandone.troilus.Record;







/**
 * Interceptor which will be executed after performing a list read query   
 */ 
public interface ReadQueryResponseInterceptor extends QueryInterceptor {
    
    /**
     * @param queryData    the request data
     * @param recordList   the response
     * @return the (modified) response
     */
    CompletableFuture<ResultList<Record>> onReadResponseAsync(ReadQueryData queryData, ResultList<Record> recordList);
}
 
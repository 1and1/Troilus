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
package net.oneandone.troilus.java7.interceptor;

import net.oneandone.troilus.interceptor.QueryInterceptor;
import net.oneandone.troilus.interceptor.SingleReadQueryData;
import net.oneandone.troilus.java7.Record;




/**
 * Interceptor which will be executed after performing a read query  
 */  
public interface SingleReadQueryResponseInterceptor extends QueryInterceptor {
    
    /**
     * @param queryData     the request data
     * @param record        the requested record 
     * @return the (modified) requested record
     */
    Record onSingleReadResponse(SingleReadQueryData queryData, Record record);
}
 
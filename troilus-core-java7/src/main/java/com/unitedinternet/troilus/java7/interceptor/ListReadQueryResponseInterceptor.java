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
package com.unitedinternet.troilus.java7.interceptor;


import com.unitedinternet.troilus.interceptor.QueryInterceptor;
import com.unitedinternet.troilus.java7.Dao.RecordList;





/**
 * Interceptor which will be executed after performing a list read query  
 */  
public interface ListReadQueryResponseInterceptor extends QueryInterceptor {
    
    /**
     * @param data        the request data
     * @param recordList  the requested record list
     * @return the (modified) requested record list
     */
    RecordList onListReadResponse(ListReadQueryData data, RecordList recordList);
}
 
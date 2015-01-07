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
package com.unitedinternet.troilus.interceptor;

import com.datastax.driver.core.querybuilder.Clause;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;



 
public class DeleteQueryData {
    
    private final ImmutableMap<String, Object> keyNameValuePairs;
    private final ImmutableList<Clause> whereConditions;
    private final ImmutableList<Clause> onlyIfConditions;
     

    public DeleteQueryData() {
        this(ImmutableMap.of(), ImmutableList.of(), ImmutableList.of());
    }
    
    
    private DeleteQueryData(ImmutableMap<String, Object> keyNameValuePairs, 
                    ImmutableList<Clause> whereConditions, 
                    ImmutableList<Clause> onlyIfConditions) {
        this.keyNameValuePairs = keyNameValuePairs;
        this.whereConditions = whereConditions;
        this.onlyIfConditions = onlyIfConditions;
    }

    

    public DeleteQueryData withKeys(ImmutableMap<String, Object> keyNameValuePairs) {
        return new DeleteQueryData(keyNameValuePairs, 
                                   this.whereConditions, 
                                   this.onlyIfConditions);  
    }
    
    public DeleteQueryData withWhereConditions(ImmutableList<Clause> whereConditions) {
        return new DeleteQueryData(this.keyNameValuePairs, 
                                   whereConditions, 
                                   this.onlyIfConditions);  
    }
    
    
    public DeleteQueryData withOnlyIfConditions(ImmutableList<Clause> onlyIfConditions) {
        return new DeleteQueryData(this.keyNameValuePairs, 
                                   this.whereConditions, 
                                   onlyIfConditions);  
    }
    
    
    public ImmutableMap<String, Object> getKeyNameValuePairs() {
        return keyNameValuePairs;
    }

    public ImmutableList<Clause> getWhereConditions() {
        return whereConditions;
    }

    public ImmutableList<Clause> getOnlyIfConditions() {
        return onlyIfConditions;
    }
}
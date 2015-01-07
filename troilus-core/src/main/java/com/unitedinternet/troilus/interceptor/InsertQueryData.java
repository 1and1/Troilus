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


import java.util.Optional;
import com.google.common.collect.ImmutableMap;


 
public class InsertQueryData {
    
    private final ImmutableMap<String, Optional<Object>> valuesToMutate;
    private final boolean ifNotExists;
  

    public InsertQueryData() {
        this(ImmutableMap.of(), false);
    }


    private InsertQueryData(ImmutableMap<String, Optional<Object>> valuesToMutate, 
                           boolean ifNotExists) {
        this.valuesToMutate = valuesToMutate;
        this.ifNotExists = ifNotExists;
    }
    
    

    public InsertQueryData withValuesToMutate(ImmutableMap<String, Optional<Object>> valuesToMutate) {
        return new InsertQueryData(valuesToMutate, 
                                   this.ifNotExists);  
    }
    
    public InsertQueryData withIfNotExits(boolean ifNotExists) {
        return new InsertQueryData(this.valuesToMutate, 
                                   ifNotExists);  
    }
 
    
    public ImmutableMap<String, Optional<Object>> getValuesToMutate() {
        return valuesToMutate;
    }

    public boolean isIfNotExists() {
        return ifNotExists;
    }
}
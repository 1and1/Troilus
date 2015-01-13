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


import com.datastax.driver.core.querybuilder.Clause;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;


 
public interface WriteQueryData {
    
    WriteQueryData keys(ImmutableMap<String, Object> keys);
    
    WriteQueryData whereConditions(ImmutableList<Clause> whereConditions);
    
    WriteQueryData valuesToMutate(ImmutableMap<String, Optional<Object>> valuesToMutate);
 
    WriteQueryData setValuesToAdd(ImmutableMap<String, ImmutableSet<Object>> setValuesToAdd);
    
    WriteQueryData setValuesToRemove(ImmutableMap<String, ImmutableSet<Object>> setValuesToRemove);
    
    WriteQueryData listValuesToAppend(ImmutableMap<String, ImmutableList<Object>> listValuesToAppend);
    
    WriteQueryData listValuesToPrepend(ImmutableMap<String, ImmutableList<Object>> listValuesToPrepend);
    
    WriteQueryData listValuesToRemove(ImmutableMap<String, ImmutableList<Object>> listValuesToRemove);
    
    WriteQueryData mapValuesToMutate(ImmutableMap<String, ImmutableMap<Object, Optional<Object>>> mapValuesToMutate);

    WriteQueryData onlyIfConditions(ImmutableList<Clause> onlyIfConditions);

    WriteQueryData ifNotExists(Optional<Boolean> ifNotExists);

    ImmutableMap<String, Object> getKeys();

    ImmutableList<Clause> getWhereConditions();
    
    ImmutableMap<String, Optional<Object>> getValuesToMutate();

    ImmutableMap<String, ImmutableSet<Object>> getSetValuesToAdd();

    ImmutableMap<String, ImmutableSet<Object>> getSetValuesToRemove();

    ImmutableMap<String, ImmutableList<Object>> getListValuesToAppend();

    ImmutableMap<String, ImmutableList<Object>> getListValuesToPrepend();

    ImmutableMap<String, ImmutableList<Object>> getListValuesToRemove();
    
    ImmutableMap<String, ImmutableMap<Object, Optional<Object>>> getMapValuesToMutate();
    
    ImmutableList<Clause> getOnlyIfConditions();
    
    Optional<Boolean> getIfNotExits();
}
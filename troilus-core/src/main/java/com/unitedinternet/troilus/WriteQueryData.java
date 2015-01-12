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
package com.unitedinternet.troilus;


import java.util.Map;
import java.util.Optional;
import java.util.Map.Entry;

import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Clause;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;


 
public class WriteQueryData {

    private final MinimalWriteQueryData data;
        
    

    WriteQueryData() {
        this(new MinimalWriteQueryData());
    }

    
    private WriteQueryData(MinimalWriteQueryData data) {
        this.data = data;
    }
    
    

    public WriteQueryData keys(ImmutableMap<String, Object> keys) {
        return new WriteQueryData(data.keys(keys));
    }
    
  
    public WriteQueryData whereConditions(ImmutableList<Clause> whereConditions) {
        return new WriteQueryData(data.whereConditions(whereConditions));
    }
    
    public WriteQueryData valuesToMutate(ImmutableMap<String, Optional<Object>> valuesToMutate) {
        // convert java optional to guava optional
        Map<String, com.google.common.base.Optional<Object>> result = Maps.newHashMap();
        for (Entry<String, Optional<Object>> entry : valuesToMutate.entrySet()) {
            result.put(entry.getKey(), com.google.common.base.Optional.fromNullable(entry.getValue().orElse(null)));
        }
        
        return new WriteQueryData(data.valuesToMutate(ImmutableMap.copyOf(result)));
    }
 
    public WriteQueryData setValuesToAdd(ImmutableMap<String, ImmutableSet<Object>> setValuesToAdd) {
        return new WriteQueryData(data.setValuesToAdd(setValuesToAdd));
    }
    
    
    public WriteQueryData setValuesToRemove(ImmutableMap<String, ImmutableSet<Object>> setValuesToRemove) {
        return new WriteQueryData(data.setValuesToRemove(setValuesToRemove));
    }
 
    
    public WriteQueryData listValuesToAppend(ImmutableMap<String, ImmutableList<Object>> listValuesToAppend) {
        return new WriteQueryData(data.listValuesToAppend(listValuesToAppend));
    }
   
    
    public WriteQueryData listValuesToPrepend(ImmutableMap<String, ImmutableList<Object>> listValuesToPrepend) {
        return new WriteQueryData(data.listValuesToPrepend(listValuesToPrepend));
    }
 
    
    public WriteQueryData listValuesToRemove(ImmutableMap<String, ImmutableList<Object>> listValuesToRemove) {
        return new WriteQueryData(data.listValuesToRemove(listValuesToRemove));
    }
 

    public WriteQueryData mapValuesToMutate(ImmutableMap<String, ImmutableMap<Object, Optional<Object>>> mapValuesToMutate) {
        // convert java optional to guava optional
       Map<String, ImmutableMap<Object, com.google.common.base.Optional<Object>>> result = Maps.newHashMap();
        
        for (Entry<String, ImmutableMap<Object, Optional<Object>>> entry : mapValuesToMutate.entrySet()) {
            Map<Object, com.google.common.base.Optional<Object>> iresult = Maps.newHashMap();
            for (Entry<Object, Optional<Object>> entry2 : entry.getValue().entrySet()) {
                iresult.put(entry2.getKey(), com.google.common.base.Optional.fromNullable(entry2.getValue().orElse(null)));
            }
            result.put(entry.getKey(), ImmutableMap.copyOf(iresult));
        }
        
        return new WriteQueryData(data.mapValuesToMutate(ImmutableMap.copyOf(result)));
    }

    
    public WriteQueryData onlyIfConditions(ImmutableList<Clause> onlyIfConditions) {
        return new WriteQueryData(data.onlyIfConditions(onlyIfConditions));
    }

    public WriteQueryData ifNotExists(Optional<Boolean> ifNotExists) {
        return new WriteQueryData(data.ifNotExists(ifNotExists.orElse(null)));
    }
    

    
    public ImmutableMap<String, Object> getKeyNameValuePairs() {
        return data.getKeyNameValuePairs();
    }

    public ImmutableList<Clause> getWhereConditions() {
        return data.getWhereConditions();
    }

    public ImmutableMap<String, Optional<Object>> getValuesToMutate() {
        Map<String, Optional<Object>> result = Maps.newHashMap();
        for (Entry<String, com.google.common.base.Optional<Object>> entry : data.getValuesToMutate().entrySet()) {
            result.put(entry.getKey(), Optional.ofNullable(entry.getValue().orNull()));
        }
        
        return ImmutableMap.copyOf(result);
    }

    public ImmutableMap<String, ImmutableSet<Object>> getSetValuesToAdd() {
        return data.getSetValuesToAdd();
    }

    public ImmutableMap<String, ImmutableSet<Object>> getSetValuesToRemove() {
        return data.getSetValuesToRemove();
    }

    public ImmutableMap<String, ImmutableList<Object>> getListValuesToAppend() {
        return data.getListValuesToAppend();
    }

    public ImmutableMap<String, ImmutableList<Object>> getListValuesToPrepend() {
        return data.getListValuesToPrepend();
    }

    public ImmutableMap<String, ImmutableList<Object>> getListValuesToRemove() {
        return data.getListValuesToRemove();
    }

    public ImmutableMap<String, ImmutableMap<Object, Optional<Object>>> getMapValuesToMutate() {
        Map<String, ImmutableMap<Object, Optional<Object>>> result = Maps.newHashMap();
        for (Entry<String, ImmutableMap<Object, com.google.common.base.Optional<Object>>> entry : data.getMapValuesToMutate().entrySet()) {
            Map<Object, Optional<Object>> iresult = Maps.newHashMap();
            for (Entry<Object, com.google.common.base.Optional<Object>> entry2 : entry.getValue().entrySet()) {
                iresult.put(entry2.getKey(), Optional.ofNullable(entry2.getValue().orNull()));
            }
            result.put(entry.getKey(), ImmutableMap.copyOf(iresult));
        }
        
        return ImmutableMap.copyOf(result);
    }

    public ImmutableList<Clause> getOnlyIfConditions() {
        return data.getOnlyIfConditions();
    }
    
    public Optional<Boolean> getIfNotExits() {
        return Optional.ofNullable(data.getIfNotExits());
    }
    
    Statement toStatement(Context ctx) {
        return data.toStatement(ctx);
    }
}
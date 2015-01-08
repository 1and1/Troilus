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
import java.util.function.Supplier;

import com.datastax.driver.core.querybuilder.Clause;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;


 
public class WriteQueryData {

    private final ImmutableMap<String, Object> keys;
    private final ImmutableList<Clause> whereConditions;
    
    private final ImmutableMap<String, Optional<Object>> valuesToMutate;
    private final ImmutableMap<String, ImmutableSet<Object>> setValuesToAdd;
    private final ImmutableMap<String, ImmutableSet<Object>> setValuesToRemove;
    private final ImmutableMap<String, ImmutableList<Object>> listValuesToAppend;
    private final ImmutableMap<String, ImmutableList<Object>> listValuesToPrepend;
    private final ImmutableMap<String, ImmutableList<Object>> listValuesToRemove;
    private final ImmutableMap<String, ImmutableMap<Object, Optional<Object>>> mapValuesToMutate;
    
    private final ImmutableList<Clause> onlyIfConditions;
    private final Optional<Boolean> ifNotExists;
    
    

    public WriteQueryData() {
        this(ImmutableMap.of(),
             ImmutableList.of(),
             ImmutableMap.of(),
             ImmutableMap.of(),
             ImmutableMap.of(),
             ImmutableMap.of(),
             ImmutableMap.of(),
             ImmutableMap.of(),
             ImmutableMap.of(),
             ImmutableList.of(),
             Optional.empty());
    }

    
    private WriteQueryData(ImmutableMap<String, Object> keys, 
                            ImmutableList<Clause> whereConditions, 
                            ImmutableMap<String, Optional<Object>> valuesToMutate, 
                            ImmutableMap<String, ImmutableSet<Object>> setValuesToAdd,
                            ImmutableMap<String, ImmutableSet<Object>> setValuesToRemove,
                            ImmutableMap<String, ImmutableList<Object>> listValuesToAppend, 
                            ImmutableMap<String, ImmutableList<Object>> listValuesToPrepend,
                            ImmutableMap<String, ImmutableList<Object>> listValuesToRemove,
                            ImmutableMap<String, ImmutableMap<Object, Optional<Object>>> mapValuesToMutate,
                            ImmutableList<Clause> onlyIfConditions,
                            Optional<Boolean> ifNotExists) {
        this.keys = keys;
        this.whereConditions = whereConditions;
        this.valuesToMutate = valuesToMutate;
        this.setValuesToAdd = setValuesToAdd;
        this.setValuesToRemove = setValuesToRemove;
        this.listValuesToAppend = listValuesToAppend;
        this.listValuesToPrepend = listValuesToPrepend;
        this.listValuesToRemove = listValuesToRemove;
        this.mapValuesToMutate = mapValuesToMutate;
        this.onlyIfConditions = onlyIfConditions;
        this.ifNotExists = ifNotExists;
    }
    
    

    public WriteQueryData keys(ImmutableMap<String, Object> keys) {
        return new WriteQueryData(keys, 
                                   this.whereConditions,
                                   this.valuesToMutate, 
                                   this.setValuesToAdd,
                                   this.setValuesToRemove,
                                   this.listValuesToAppend,
                                   this.listValuesToPrepend,
                                   this.listValuesToRemove,
                                   this.mapValuesToMutate,
                                   this.onlyIfConditions,
                                   this.ifNotExists);
    }
    
  
    public WriteQueryData whereConditions(ImmutableList<Clause> whereConditions) {
        preCondition(!ifNotExists.isPresent(), IllegalStateException::new);
        
        return new WriteQueryData(this.keys, 
                                   whereConditions,
                                   this.valuesToMutate, 
                                   this.setValuesToAdd,
                                   this.setValuesToRemove,
                                   this.listValuesToAppend,
                                   this.listValuesToPrepend,
                                   this.listValuesToRemove,
                                   this.mapValuesToMutate,
                                   this.onlyIfConditions,
                                   this.ifNotExists);
    }
    
    public WriteQueryData valuesToMutate(ImmutableMap<String, Optional<Object>> valuesToMutate) {
        return new WriteQueryData(this.keys, 
                                   this.whereConditions,
                                   valuesToMutate, 
                                   this.setValuesToAdd,
                                   this.setValuesToRemove,
                                   this.listValuesToAppend,
                                   this.listValuesToPrepend,
                                   this.listValuesToRemove,
                                   this.mapValuesToMutate,
                                   this.onlyIfConditions,
                                   this.ifNotExists);
    }
 
    
    public WriteQueryData setValuesToAdd(ImmutableMap<String, ImmutableSet<Object>> setValuesToAdd) {
        preCondition(!ifNotExists.isPresent(), IllegalStateException::new);
        
        return new WriteQueryData(this.keys, 
                                   this.whereConditions,
                                   this.valuesToMutate, 
                                   setValuesToAdd,
                                   this.setValuesToRemove,
                                   this.listValuesToAppend,
                                   this.listValuesToPrepend,
                                   this.listValuesToRemove,
                                   this.mapValuesToMutate,
                                   this.onlyIfConditions,
                                   this.ifNotExists);
    }
    
    
    public WriteQueryData setValuesToRemove(ImmutableMap<String, ImmutableSet<Object>> setValuesToRemove) {
        preCondition(!ifNotExists.isPresent(), IllegalStateException::new);
        
        return new WriteQueryData(this.keys, 
                                   this.whereConditions,
                                   this.valuesToMutate, 
                                   this.setValuesToAdd,
                                   setValuesToRemove,
                                   this.listValuesToAppend,
                                   this.listValuesToPrepend,
                                   this.listValuesToRemove,
                                   this.mapValuesToMutate,
                                   this.onlyIfConditions,
                                   this.ifNotExists);
    }
 
    
    public WriteQueryData listValuesToAppend(ImmutableMap<String, ImmutableList<Object>> listValuesToAppend) {
        preCondition(!ifNotExists.isPresent(), IllegalStateException::new);

        return new WriteQueryData(this.keys, 
                                   this.whereConditions,
                                   this.valuesToMutate, 
                                   this.setValuesToAdd,
                                   this.setValuesToRemove,
                                   listValuesToAppend,
                                   this.listValuesToPrepend,
                                   this.listValuesToRemove,
                                   this.mapValuesToMutate,
                                   this.onlyIfConditions,
                                   this.ifNotExists);
    }
   
    
    public WriteQueryData listValuesToPrepend(ImmutableMap<String, ImmutableList<Object>> listValuesToPrepend) {
        preCondition(!ifNotExists.isPresent(), IllegalStateException::new);

        return new WriteQueryData(this.keys, 
                                   this.whereConditions,
                                   this.valuesToMutate, 
                                   this.setValuesToAdd,
                                   this.setValuesToRemove,
                                   this.listValuesToAppend,
                                   listValuesToPrepend,
                                   this.listValuesToRemove,
                                   this.mapValuesToMutate,
                                   this.onlyIfConditions,
                                   this.ifNotExists);
    }
 
    
    public WriteQueryData listValuesToRemove(ImmutableMap<String, ImmutableList<Object>> listValuesToRemove) {
        preCondition(!ifNotExists.isPresent(), IllegalStateException::new);

        return new WriteQueryData(this.keys, 
                                   this.whereConditions,
                                   this.valuesToMutate, 
                                   this.setValuesToAdd,
                                   this.setValuesToRemove,
                                   this.listValuesToAppend,
                                   this.listValuesToPrepend,
                                   listValuesToRemove,
                                   this.mapValuesToMutate,
                                   this.onlyIfConditions,
                                   this.ifNotExists);
    }
 

    public WriteQueryData mapValuesToMutate(ImmutableMap<String, ImmutableMap<Object, Optional<Object>>> mapValuesToMutate) {
        preCondition(!ifNotExists.isPresent(), IllegalStateException::new);

        return new WriteQueryData(this.keys, 
                                   this.whereConditions,
                                   this.valuesToMutate, 
                                   this.setValuesToAdd,
                                   this.setValuesToRemove,
                                   this.listValuesToAppend,
                                   this.listValuesToPrepend,
                                   this.listValuesToRemove,
                                   mapValuesToMutate,
                                   this.onlyIfConditions,
                                   this.ifNotExists);
    }

    
    public WriteQueryData onlyIfConditions(ImmutableList<Clause> onlyIfConditions) {
        preCondition(!ifNotExists.isPresent(), IllegalStateException::new);

        return new WriteQueryData(this.keys, 
                                   this.whereConditions,
                                   this.valuesToMutate, 
                                   this.setValuesToAdd,
                                   this.setValuesToRemove,
                                   this.listValuesToAppend,
                                   this.listValuesToPrepend,
                                   this.listValuesToRemove,
                                   this.mapValuesToMutate,
                                   onlyIfConditions,
                                   this.ifNotExists);
    }

    public WriteQueryData ifNotExists(Optional<Boolean> ifNotExists) {
        preCondition(onlyIfConditions.isEmpty(), IllegalStateException::new);
        preCondition(whereConditions.isEmpty(), IllegalStateException::new);
        preCondition(setValuesToAdd.isEmpty(), IllegalStateException::new);
        preCondition(setValuesToRemove.isEmpty(), IllegalStateException::new);
        preCondition(listValuesToAppend.isEmpty(), IllegalStateException::new);
        preCondition(listValuesToPrepend.isEmpty(), IllegalStateException::new);
        preCondition(listValuesToRemove.isEmpty(), IllegalStateException::new);
        preCondition(mapValuesToMutate.isEmpty(), IllegalStateException::new);
        
        return new WriteQueryData(this.keys, 
                                   this.whereConditions,
                                   this.valuesToMutate, 
                                   this.setValuesToAdd,
                                   this.setValuesToRemove,
                                   this.listValuesToAppend,
                                   this.listValuesToPrepend,
                                   this.listValuesToRemove,
                                   this.mapValuesToMutate,
                                   this.onlyIfConditions,
                                   ifNotExists);
    }
    

    
    private <T extends RuntimeException> void preCondition(boolean condition, Supplier<T> suppier) {
        if (!condition) {
            throw suppier.get();
        }
    }

    
    public ImmutableMap<String, Object> getKeys() {
        return keys;
    }

    public ImmutableList<Clause> getWhereConditions() {
        return whereConditions;
    }

    public ImmutableMap<String, Optional<Object>> getValuesToMutate() {
        return valuesToMutate;
    }

    public ImmutableMap<String, ImmutableSet<Object>> getSetValuesToAdd() {
        return setValuesToAdd;
    }

    public ImmutableMap<String, ImmutableSet<Object>> getSetValuesToRemove() {
        return setValuesToRemove;
    }

    public ImmutableMap<String, ImmutableList<Object>> getListValuesToAppend() {
        return listValuesToAppend;
    }

    public ImmutableMap<String, ImmutableList<Object>> getListValuesToPrepend() {
        return listValuesToPrepend;
    }

    public ImmutableMap<String, ImmutableList<Object>> getListValuesToRemove() {
        return listValuesToRemove;
    }

    public ImmutableMap<String, ImmutableMap<Object, Optional<Object>>> getMapValuesToMutate() {
        return mapValuesToMutate;
    }

    public ImmutableList<Clause> getOnlyIfConditions() {
        return onlyIfConditions;
    }
    
    public Optional<Boolean> getIfNotExits() {
        return ifNotExists;
    }
}
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


import java.util.Optional;

import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.ConsistencyLevel;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;


 

public class DaoImpl implements Dao {
    private final Context defaultContext;

    
    
    public DaoImpl(Context defaultContext) {
        this.defaultContext = defaultContext;
    }
 
    
    private Context getDefaultContext() {
        return defaultContext;
    } 
    
    
    
    @Override
    public Dao withConsistency(ConsistencyLevel consistencyLevel) {
        return new DaoImpl(getDefaultContext().withConsistency(consistencyLevel));
    }
    
    @Override
    public Dao withSerialConsistency(ConsistencyLevel consistencyLevel) {
        return new DaoImpl(getDefaultContext().withSerialConsistency(consistencyLevel));
    }
 


    
    @Override
    public Insertion writeEntity(Object entity) {
        return WriteQuery.newInsertQuery(getDefaultContext(), entity);
    }
    
    @Override
    public UpdateWithValues<?> writeWhere(Clause... whereConditions) {
        return WriteQuery.newUpdate(getDefaultContext(), ImmutableList.copyOf(whereConditions));
    }
    
    @Override
    public Write writeWithKey(String keyName, Object keyValue) {
        return WriteQuery.newUpdate(getDefaultContext(), ImmutableMap.of(keyName, keyValue));
    }
    
    @Override
    public Write writeWithKey(String keyName1, Object keyValue1, String keyName2, Object keyValue2) {
        return WriteQuery.newUpdate(getDefaultContext(), ImmutableMap.of(keyName1, keyValue1, keyName2, keyValue2));
    }
    
    @Override
    public Write writeWithKey(String keyName1, Object keyValue1, String keyName2, Object keyValue2, String keyName3, Object keyValue3) {
        return WriteQuery.newUpdate(getDefaultContext(), ImmutableMap.of(keyName1, keyValue1, keyName2, keyValue2, keyName3, keyValue3));
    }

    @Override
    public Write writeWithKey(String keyName1, Object keyValue1, String keyName2, Object keyValue2, String keyName3, Object keyValue3, String keyName4, Object keyValue4) {
        return WriteQuery.newUpdate(getDefaultContext(), ImmutableMap.of(keyName1, keyValue1, keyName2, keyValue2, keyName3, keyValue3, keyName4, keyValue4));
    }
    
    
    
    @Override
    public Deletion deleteWhere(Clause... whereConditions) {
        
        return DeleteQuery.newDeleteQuery(getDefaultContext(), 
                                          ImmutableMap.of(),
                                          ImmutableList.copyOf(whereConditions),
                                          ImmutableList.of());
    };
    
    @Override
    public Deletion deleteWithKey(String keyName, Object keyValue) {
        
        return DeleteQuery.newDeleteQuery(getDefaultContext(),      
                                          ImmutableMap.of(keyName, keyValue),
                                          ImmutableList.of(),
                                          ImmutableList.of());
    }

    @Override
    public Deletion deleteWithKey(String keyName1, Object keyValue1, 
                                  String keyName2, Object keyValue2) {
        
        return DeleteQuery.newDeleteQuery(getDefaultContext(), 
                                          ImmutableMap.of(keyName1, keyValue1, keyName2, keyValue2),
                                          ImmutableList.of(),
                                          ImmutableList.of());
    }
    
    @Override
    public Deletion deleteWithKey(String keyName1, Object keyValue1, 
                                  String keyName2, Object keyValue2, 
                                  String keyName3, Object keyValue3) {
        
        return DeleteQuery.newDeleteQuery(getDefaultContext(),
                                          ImmutableMap.of(keyName1, keyValue1, keyName2, keyValue2, keyName3, keyValue3),
                                          ImmutableList.of(),                                           
                                          ImmutableList.of());
    }
    
    @Override
    public Deletion deleteWithKey(String keyName1, Object keyValue1, 
                                  String keyName2, Object keyValue2, 
                                  String keyName3, Object keyValue3,
                                  String keyName4, Object keyValue4) {
        
        return DeleteQuery.newDeleteQuery(getDefaultContext(), 
                                          ImmutableMap.of(keyName1, keyValue1, keyName2, keyValue2, keyName3, keyValue3, keyName4, keyValue4),
                                          ImmutableList.of(),
                                          ImmutableList.of());
    }
    
    
    
    
    @Override
    public SingleReadWithUnit<Optional<Record>> readWithKey(String keyName, Object keyValue) {
        return ReadQuery.newSingleReadQuery(getDefaultContext(), 
                                            ImmutableMap.of(keyName, keyValue),
                                            Optional.empty());
    }
    
    @Override
    public SingleReadWithUnit<Optional<Record>> readWithKey(String keyName1, Object keyValue1, String keyName2, Object keyValue2) {
        return ReadQuery.newSingleReadQuery(getDefaultContext(), 
                                            ImmutableMap.of(keyName1, keyValue1, keyName2, keyValue2), 
                                            Optional.of(ImmutableSet.of()));
    }
    
    @Override
    public SingleReadWithUnit<Optional<Record>> readWithKey(String keyName1, Object keyValue1, String keyName2, Object keyValue2, String keyName3, Object keyValue3) {
        return ReadQuery.newSingleReadQuery(getDefaultContext(), 
                                            ImmutableMap.of(keyName1, keyValue1, keyName2, keyValue2, keyName3, keyValue3), 
                                            Optional.of(ImmutableSet.of()));
    }
    
    @Override
    public SingleReadWithUnit<Optional<Record>> readWithKey(String keyName1, Object keyValue1, String keyName2, Object keyValue2, String keyName3, Object keyValue3, String keyName4, Object keyValue4) {
        return ReadQuery.newSingleReadQuery(getDefaultContext(), 
                                            ImmutableMap.of(keyName1, keyValue1, keyName2, keyValue2, keyName3, keyValue3, keyName4, keyValue4), 
                                            Optional.of(ImmutableSet.of()));
    }
    
    
    
    protected <E> SingleRead<Optional<E>> newSingleSelection(Context ctx, SingleReadWithUnit<Optional<Record>> read, Class<?> clazz) {
        return ReadQuery.newSingleEntityReadQuery(ctx, read, clazz);
    }

    
    
    @Override
    public ListReadWithUnit<RecordList> readWhere(Clause... clauses) {
        return ReadQuery.newListReadQuery(getDefaultContext(), 
                                          ImmutableSet.copyOf(clauses), 
                                          Optional.of(ImmutableSet.of()), 
                                          Optional.empty(), 
                                          Optional.empty(), 
                                          Optional.empty(),
                                          Optional.empty());
    }
     
    
    @Override
    public ListReadWithUnit<RecordList> readAll() {
        return ReadQuery.newListReadQuery(getDefaultContext(), 
                                          ImmutableSet.of(), 
                                          Optional.of(ImmutableSet.of()), 
                                          Optional.empty(), 
                                          Optional.empty(), 
                                          Optional.empty(),
                                          Optional.empty());
    }
  
  
    
    protected <E> ListRead<EntityList<E>> newListSelection(Context ctx, ListRead<RecordList> read, Class<?> clazz) {
        return ReadQuery.newListEntityReadQuery(ctx, read, clazz);
    }
}


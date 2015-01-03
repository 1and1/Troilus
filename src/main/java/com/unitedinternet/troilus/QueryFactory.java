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

import com.datastax.driver.core.BatchStatement.Type;
import com.datastax.driver.core.querybuilder.Clause;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.unitedinternet.troilus.Dao.Insertion;
import com.unitedinternet.troilus.Dao.ListRead;
import com.unitedinternet.troilus.Dao.ListReadWithUnit;
import com.unitedinternet.troilus.Dao.SingleReadWithUnit;
import com.unitedinternet.troilus.ReadQuery.ColumnToFetch;
import com.unitedinternet.troilus.ReadQuery.CountReadQuery;
import com.unitedinternet.troilus.ReadQuery.ListEntityReadQuery;
import com.unitedinternet.troilus.ReadQuery.ListReadQuery;
import com.unitedinternet.troilus.ReadQuery.SingleEntityReadQuery;
import com.unitedinternet.troilus.ReadQuery.SingleReadQuery;



interface QueryFactory {
    
    UpdateQuery newUpdateQuery(Context ctx, 
                               QueryFactory queryFactory,
                               ImmutableMap<String, Object> keys, 
                               ImmutableList<Clause> whereConditions, 
                               ImmutableMap<String, Optional<Object>> valuesToMutate, 
                               ImmutableMap<String, ImmutableSet<Object>> setValuesToAdd,
                               ImmutableMap<String, ImmutableSet<Object>> setValuesToRemove,
                               ImmutableMap<String, ImmutableList<Object>> listValuesToAppend, 
                               ImmutableMap<String, ImmutableList<Object>> listValuesToPrepend,
                               ImmutableMap<String, ImmutableList<Object>> listValuesToRemove,
                               ImmutableMap<String, ImmutableMap<Object, Optional<Object>>> mapValuesToMutate,
                               ImmutableList<Clause> ifConditions);
    
    @Deprecated
    Insertion newInsertionQuery(Context ctx, QueryFactory queryFactory, Object entity);
    
    InsertionQuery newInsertionQuery(Context ctx, QueryFactory queryFactory, ImmutableMap<String, Optional<Object>> valuesToMutate, boolean ifNotExists);
    
    DeleteQuery newDeleteQuery(Context ctx, 
                               QueryFactory queryFactory,
                               ImmutableMap<String, Object> keyNameValuePairs,  
                               ImmutableList<Clause> whereConditions, 
                               ImmutableList<Clause> ifConditions);
    
    

    SingleReadQuery newSingleReadQuery(Context ctx, 
                                       QueryFactory queryFactory,
                                       ImmutableMap<String, Object> keyNameValuePairs, 
                                       Optional<ImmutableSet<ColumnToFetch>> optionalColumnsToFetch);   
    
    <E> SingleEntityReadQuery<E> newSingleEntityReadQuery(Context ctx,
                                                          QueryFactory queryFactory,
                                                          SingleReadWithUnit<Optional<Record>> read, 
                                                          Class<?> clazz);
    
    
    ListReadQuery newListReadQuery(Context ctx,
                                   QueryFactory queryFactory, 
                                   ImmutableSet<Clause> clauses, 
                                   Optional<ImmutableSet<ColumnToFetch>> columnsToFetch, 
                                   Optional<Integer> optionalLimit, 
                                   Optional<Boolean> optionalAllowFiltering,
                                   Optional<Integer> optionalFetchSize,
                                   Optional<Boolean> optionalDistinct);
    
    <E> ListEntityReadQuery<E> newListEntityReadQuery(Context ctx,
                                                      QueryFactory queryFactory, 
                                                      ListRead<RecordList> read, Class<?> clazz);
    
    CountReadQuery newCountReadQuery(Context ctx, 
                                     QueryFactory queryFactory,
                                     ImmutableSet<Clause> clauses, 
                                     Optional<Integer> optionalLimit, 
                                     Optional<Boolean> optionalAllowFiltering,
                                     Optional<Integer> optionalFetchSize,
                                     Optional<Boolean> optionalDistinct); 
    
    
    BatchMutationQuery newBatchMutationQuery(Context ctx,    
                                             QueryFactory queryFactory,
                                             Type type, 
                                             ImmutableList<Batchable> batchables);
}




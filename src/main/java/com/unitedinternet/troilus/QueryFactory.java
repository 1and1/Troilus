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
import java.util.function.Consumer;

import com.datastax.driver.core.BatchStatement.Type;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.Select;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.unitedinternet.troilus.Dao.InsertionWithValues;
import com.unitedinternet.troilus.Dao.ListRead;
import com.unitedinternet.troilus.Dao.ListReadWithUnit;
import com.unitedinternet.troilus.Dao.SingleRead;
import com.unitedinternet.troilus.Dao.SingleReadWithUnit;


interface QueryFactory  {

    <E> SingleRead<Optional<E>> newSingleSelection(Context ctx, SingleRead<Optional<Record>> read, Class<?> clazz);

    SingleReadWithUnit<Optional<Record>> newSingleSelection(Context ctx, ImmutableMap<String, Object> keyNameValuePairs,Optional<ImmutableSet<ColumnToFetch>> optionalColumnsToFetch);

    
    static class ColumnToFetch implements Consumer<Select.Selection> {
        private final String name;
        private final boolean isFetchWritetime;
        private final boolean isFetchTtl;
        
        private ColumnToFetch(String name, boolean isFetchWritetime, boolean isFetchTtl) {
            this.name = name;
            this.isFetchWritetime = isFetchWritetime;
            this.isFetchTtl = isFetchTtl;
        }
        
        public static ColumnToFetch create(String name, boolean isFetchWritetime, boolean isFetchTtl) {
            return new ColumnToFetch(name, isFetchWritetime, isFetchTtl);
        }
        
        public static ImmutableSet<ColumnToFetch> create(ImmutableCollection<String> names) {
            return names.stream().map(name -> new ColumnToFetch(name, false, false)).collect(Immutables.toSet());
        }

        @Override
        public void accept(Select.Selection selection) {
             selection.column(name);

             if (isFetchTtl) {
                 selection.ttl(name);
             }

             if (isFetchWritetime) {
                 selection.writeTime(name);
             }
        }
    }
    
    
    
  //  DeleteQuery newDeletion(Context ctx, ImmutableMap<String, Object> keyNameValuePairs, ImmutableList<Clause> whereConditions, ImmutableList<Clause> ifConditions);
    
   // BatchMutation newBatchMutation(Context ctx, Type type, ImmutableList<Batchable> batchables);
    
   // OldWriteQuery newWrite(Context ctx, ImmutableMap<String, Object> keys, ImmutableList<? extends ValueToMutate> valuesToInsert);

  //  UpdateWithValues newUpdate(Context ctx, ImmutableList<? extends ValueToMutate> valuesToMutate, ImmutableMap<String, Object> keys, ImmutableList<Clause> whereConditions, ImmutableList<Clause> ifConditions);
    
 //   InsertWithValues newInsertion(Context ctx, ImmutableList<? extends ValueToMutate> valuesToMutate, boolean ifNotExists);


    ListRead<Count> newCountRead(Context ctx, 
                                       ImmutableSet<Clause> clauses, 
                                       Optional<Integer> optionalLimit, 
                                       Optional<Boolean> optionalAllowFiltering,       
                                       Optional<Integer> optionalFetchSize,    
                                       Optional<Boolean> optionalDistinct);
    
    
    ListReadWithUnit<RecordList> newListSelection(Context ctx, 
                                                  ImmutableSet<Clause> clauses,    
                                                  Optional<ImmutableSet<ColumnToFetch>> columnsToFetch, 
                                                  Optional<Integer> optionalLimit, 
                                                  Optional<Boolean> optionalAllowFiltering,
                                                  Optional<Integer> optionalFetchSize,    
                                                  Optional<Boolean> optionalDistinct);
    
    
    <E> ListRead<EntityList<E>> newListSelection(Context ctx, ListRead<RecordList> read, Class<?> clazz);
  
}



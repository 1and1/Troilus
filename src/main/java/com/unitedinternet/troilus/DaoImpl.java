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
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.ConsistencyLevel;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.unitedinternet.troilus.QueryFactory.ColumnToFetch;
import com.unitedinternet.troilus.QueryFactory.ValueToMutate;


 

@SuppressWarnings("rawtypes")
public class DaoImpl implements Dao {
    private final QueryFactory queryFactory = new QueryFactoryImpl();
    private final Context defaultContext;
    
    
    public DaoImpl(Context defaultContext) {
        this.defaultContext = defaultContext;
    }
 
    
    protected Context getDefaultContext() {
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
        return newInsertion(getDefaultContext(), ImmutableList.of(), false).values(getDefaultContext().toValues(entity));
    }
    
    protected InsertWithValues newInsertion(Context ctx, ImmutableList<? extends ValueToMutate> valuesToMutate, boolean ifNotExists) {
        return new InsertQuery(ctx, queryFactory, valuesToMutate, ifNotExists);
    }
    
    
    @Override
    public UpdateWithValues writeWhere(Clause... clauses) {
        return newUpdate(getDefaultContext(), ImmutableList.of(), ImmutableMap.of(), ImmutableList.copyOf(clauses), ImmutableList.of());
    }
    
    protected UpdateWithValues newUpdate(Context ctx, ImmutableList<? extends ValueToMutate> valuesToMutate, ImmutableMap<String, Object> keys, ImmutableList<Clause> whereConditions, ImmutableList<Clause> ifConditions) {
        return new UpdateQuery(ctx, queryFactory, valuesToMutate, keys, whereConditions, ifConditions);
    }
    

    @Override
    public WriteWithValues writeWithKey(String keyName, Object keyValue) {
        return newWrite(getDefaultContext(), ImmutableMap.of(keyName, keyValue), ImmutableList.of());
    }
    
    @Override
    public WriteWithValues writeWithKey(String keyName1, Object keyValue1, String keyName2, Object keyValue2) {
        return newWrite(getDefaultContext(), ImmutableMap.of(keyName1, keyValue1, keyName2, keyValue2), ImmutableList.of());
    }
    
    @Override
    public WriteWithValues writeWithKey(String keyName1, Object keyValue1, String keyName2, Object keyValue2, String keyName3, Object keyValue3) {
        return newWrite(getDefaultContext(), ImmutableMap.of(keyName1, keyValue1, keyName2, keyValue2, keyName3, keyValue3), ImmutableList.of());
    }

    @Override
    public WriteWithValues writeWithKey(String keyName1, Object keyValue1, String keyName2, Object keyValue2, String keyName3, Object keyValue3, String keyName4, Object keyValue4) {
        return newWrite(getDefaultContext(), ImmutableMap.of(keyName1, keyValue1, keyName2, keyValue2, keyName3, keyValue3, keyName4, keyValue4), ImmutableList.of());
    }

    protected WriteQuery newWrite(Context ctx, ImmutableMap<String, Object> keys, ImmutableList<? extends ValueToMutate> valuesToInsert) {
        return new WriteQuery(ctx, queryFactory, keys, valuesToInsert);
    }
    
    
    @Override
    public Deletion deleteWhere(Clause... whereConditions) {
        return newDeletion(getDefaultContext(), 
                           ImmutableMap.of(),
                           ImmutableList.copyOf(whereConditions),
                           ImmutableList.of());
    };
    
    @Override
    public Deletion deleteWithKey(String keyName, Object keyValue) {
        
        return newDeletion(getDefaultContext(), 
                           ImmutableMap.of(keyName, keyValue),
                           ImmutableList.of(),
                           ImmutableList.of());
    }

    @Override
    public Deletion deleteWithKey(String keyName1, Object keyValue1, 
                                  String keyName2, Object keyValue2) {
        
        return newDeletion(getDefaultContext(), 
                           ImmutableMap.of(keyName1, keyValue1,
                                           keyName2, keyValue2),
                           ImmutableList.of(),
                           ImmutableList.of());
    }
    
    @Override
    public Deletion deleteWithKey(String keyName1, Object keyValue1, 
                                  String keyName2, Object keyValue2, 
                                  String keyName3, Object keyValue3) {
        
        return newDeletion(getDefaultContext(), 
                           ImmutableMap.of(keyName1, keyValue1, 
                                           keyName2, keyValue2, 
                                           keyName3, keyValue3),
                           ImmutableList.of(),                                           
                           ImmutableList.of());
    }
    
    @Override
    public Deletion deleteWithKey(String keyName1, Object keyValue1, 
                                  String keyName2, Object keyValue2, 
                                  String keyName3, Object keyValue3,
                                  String keyName4, Object keyValue4) {
        
        return newDeletion(getDefaultContext(), 
                           ImmutableMap.of(keyName1, keyValue1, 
                                           keyName2, keyValue2, 
                                           keyName3, keyValue3, 
                                           keyName4, keyValue4),
                           ImmutableList.of(),
                           ImmutableList.of());
    }
    
    protected DeleteQuery newDeletion(Context ctx, ImmutableMap<String, Object> keyNameValuePairs, ImmutableList<Clause> whereConditions, ImmutableList<Clause> ifConditions) {
        return new DeleteQuery(ctx, queryFactory, keyNameValuePairs, whereConditions, ifConditions);
    }
    
    
    protected BatchMutation newBatchMutation(Context ctx, Type type, ImmutableList<Batchable> batchables) {
        return new BatchMutationQuery(ctx, queryFactory, type, batchables);
    }
    
    
    @Override
    public SingleReadWithUnit<Optional<Record>> readWithKey(String keyName, Object keyValue) {
        return newSingleSelection(getDefaultContext(), ImmutableMap.of(keyName, keyValue), Optional.empty());
    }
    
    @Override
    public SingleReadWithUnit<Optional<Record>> readWithKey(String keyName1, Object keyValue1, String keyName2, Object keyValue2) {
        return newSingleSelection(getDefaultContext(), ImmutableMap.of(keyName1, keyValue1, keyName2, keyValue2), Optional.of(ImmutableSet.of()));
    }
    
    @Override
    public SingleReadWithUnit<Optional<Record>> readWithKey(String keyName1, Object keyValue1, String keyName2, Object keyValue2, String keyName3, Object keyValue3) {
        return newSingleSelection(getDefaultContext(), ImmutableMap.of(keyName1, keyValue1, keyName2, keyValue2, keyName3, keyValue3), Optional.of(ImmutableSet.of()));
    }
    
    @Override
    public SingleReadWithUnit<Optional<Record>> readWithKey(String keyName1, Object keyValue1, String keyName2, Object keyValue2, String keyName3, Object keyValue3, String keyName4, Object keyValue4) {
        return newSingleSelection(getDefaultContext(), ImmutableMap.of(keyName1, keyValue1, keyName2, keyValue2, keyName3, keyValue3, keyName4, keyValue4), Optional.of(ImmutableSet.of()));
    }
    
    protected SingleReadWithUnit<Optional<Record>> newSingleSelection(Context ctx, 
                                                                      ImmutableMap<String, Object> keyNameValuePairs, 
                                                                      Optional<ImmutableSet<ColumnToFetch>> optionalColumnsToFetch) {
        return new SingleReadQuery(ctx, queryFactory, keyNameValuePairs, optionalColumnsToFetch);
    }
    
    
    protected <E> SingleRead<Optional<E>> newSingleSelection(Context ctx, SingleRead<Optional<Record>> read, Class<?> clazz) {
        return new SingleEntityReadQuery<E>(ctx, queryFactory, read, clazz);
    }

    
    private final class QueryFactoryImpl implements QueryFactory {
        
        @Override
        public InsertWithValues newInsertion(Context ctx, ImmutableList<? extends ValueToMutate> valuesToMutate, boolean ifNotExists) {
            return DaoImpl.this.newInsertion(ctx, valuesToMutate, ifNotExists);
        }
        
        @Override
        public UpdateWithValues newUpdate(Context ctx, ImmutableList<? extends ValueToMutate> valuesToMutate, ImmutableMap<String, Object> keys, ImmutableList<Clause> whereConditions, ImmutableList<Clause> ifConditions) {
            return DaoImpl.this.newUpdate(ctx, valuesToMutate, keys, whereConditions, ifConditions);
        }
        
        @Override
        public WriteQuery newWrite(Context ctx, ImmutableMap<String, Object> keys, ImmutableList<? extends ValueToMutate> valuesToInsert) {
            return DaoImpl.this.newWrite(ctx, keys, valuesToInsert);
        }
        
        @Override
        public DeleteQuery newDeletion(Context ctx, ImmutableMap<String, Object> keyNameValuePairs, ImmutableList<Clause> whereConditions, ImmutableList<Clause> ifConditions) {
            return DaoImpl.this.newDeletion(ctx, keyNameValuePairs, whereConditions, ifConditions);
        }
        
        @Override
        public <E> SingleRead<Optional<E>> newSingleSelection(Context ctx, SingleRead<Optional<Record>> read, Class<?> clazz) {
            return DaoImpl.this.newSingleSelection(ctx, read, clazz);
        }
        
        @Override
        public SingleReadWithUnit<Optional<Record>> newSingleSelection(Context ctx, ImmutableMap<String, Object> keyNameValuePairs, Optional<ImmutableSet<ColumnToFetch>> optionalColumnsToFetch) {
            return DaoImpl.this.newSingleSelection(ctx, keyNameValuePairs, optionalColumnsToFetch);
        }
        
        @Override
        public ListRead<Count> newCountRead(Context ctx, ImmutableSet<Clause> clauses, Optional<Integer> optionalLimit, Optional<Boolean> optionalAllowFiltering, Optional<Integer> optionalFetchSize, Optional<Boolean> optionalDistinct) {
            return DaoImpl.this.newCountRead(ctx, clauses, optionalLimit, optionalAllowFiltering, optionalFetchSize, optionalDistinct);
        }
        
        @Override
        public <E> ListRead<EntityList<E>> newListSelection(Context ctx, ListRead<RecordList> read, Class<?> clazz) {
            return DaoImpl.this.newListSelection(ctx, read, clazz);
        }
        
        @Override
        public ListReadWithUnit<RecordList> newListSelection(Context ctx, 
                                                             ImmutableSet<Clause> clauses,
                                                             Optional<ImmutableSet<ColumnToFetch>> columnsToFetch,
                                                             Optional<Integer> optionalLimit,
                                                             Optional<Boolean> optionalAllowFiltering,
                                                             Optional<Integer> optionalFetchSize,
                                                             Optional<Boolean> optionalDistinct) {
            return new ListReadQuery(ctx, 
                                     queryFactory, 
                                     clauses, 
                                     columnsToFetch, 
                                     optionalLimit, 
                                     optionalAllowFiltering, 
                                     optionalFetchSize, 
                                     optionalDistinct);
        }
        
        @Override
        public BatchMutation newBatchMutation(Context ctx, Type type, ImmutableList<Batchable> batchables) {
            return DaoImpl.this.newBatchMutation(ctx, type, batchables);
        }
    }
    

    
  
 

     
    
    static final class BuildinValueToMutate implements ValueToMutate {
        private final String name;
        private final Object value;
        
        @SuppressWarnings("unchecked")
        public BuildinValueToMutate(String name, Object value) {
            this.name = name;
            if (value instanceof Optional) {
                this.value = ((Optional) value).orElse(null);
            } else {
                this.value = value;
            }
        }
        
        
        @Override
        public String toString() {
            return name + "=" + value;
        }
        
        
        @Override
        public Object addPreparedToStatement(Context ctx, Insert insert) {
            insert.value(name, bindMarker());
            return value;
        }
        
        @Override
        public void addToStatement(Context ctx, Insert insert) {
            insert.value(name,  value);
        }

        public Object addPreparedToStatement(Context ctx, com.datastax.driver.core.querybuilder.Update update) {
            update.with(set(name, bindMarker()));
            return value;
        }
        
        
        @Override
        public void addToStatement(Context ctx, com.datastax.driver.core.querybuilder.Update update) {
            update.with(set(name, value));
        }
    }
   
    
    
    static final class UDTValueToMutate implements ValueToMutate {
        private final String columnName;
        private final Object value;
        
        @SuppressWarnings("unchecked")
        public UDTValueToMutate(String columnName, Object value) {
            this.columnName = columnName;
            if (value instanceof Optional) {
                this.value = ((Optional) value).orElse(null);
            } else {
                this.value = value;
            }
        }
        
        
        @Override
        public String toString() {
            return columnName + "=" + value;
        }
        
        
        @Override
        public Object addPreparedToStatement(Context ctx, Insert insert) {
            insert.value(columnName, bindMarker());
            return UDTValueMapper.toUdtValue(ctx, ctx.getColumnMetadata(columnName).getType(), value);
        }

        @Override
        public void addToStatement(Context ctx, Insert insert) {
            insert.value(columnName, value);
        }
        
        public Object addPreparedToStatement(Context ctx, com.datastax.driver.core.querybuilder.Update update) {
            update.with(set(columnName, bindMarker()));
            return UDTValueMapper.toUdtValue(ctx, ctx.getColumnMetadata(columnName).getType(), value);
        }
        
        @Override
        public void addToStatement(Context ctx, com.datastax.driver.core.querybuilder.Update update) {
            update.with(set(columnName, UDTValueMapper.toUdtValue(ctx, ctx.getColumnMetadata(columnName).getType(), value)));
        }
    }
   
        
        
    
    


  

    
    
    @Override
    public ListReadWithUnit<RecordList> readWhere(Clause... clauses) {
        return newListSelection(getDefaultContext(), 
                                ImmutableSet.copyOf(clauses), 
                                Optional.of(ImmutableSet.of()), 
                                Optional.empty(), 
                                Optional.empty(), 
                                Optional.empty(),
                                Optional.empty());
    }
     
    
    @Override
    public ListReadWithUnit<RecordList> readAll() {
        return newListSelection(getDefaultContext(), 
                                ImmutableSet.of(), 
                                Optional.of(ImmutableSet.of()), 
                                Optional.empty(), 
                                Optional.empty(), 
                                Optional.empty(),
                                Optional.empty());
    }
    
    

    protected ListRead<Count> newCountRead(Context ctx, 
                                                 ImmutableSet<Clause> clauses, 
                                                 Optional<Integer> optionalLimit, 
                                                 Optional<Boolean> optionalAllowFiltering,    
                                                 Optional<Integer> optionalFetchSize,    
                                                 Optional<Boolean> optionalDistinct) {
        return new CountReadQuery(ctx, queryFactory, clauses, optionalLimit, optionalAllowFiltering, optionalFetchSize, optionalDistinct);
    }
    
    
    
    protected ListReadWithUnit<RecordList> newListSelection(Context ctx, 
                                                            ImmutableSet<Clause> clauses, 
                                                            Optional<ImmutableSet<ColumnToFetch>> columnsToFetch, 
                                                            Optional<Integer> optionalLimit, 
                                                            Optional<Boolean> optionalAllowFiltering,
                                                            Optional<Integer> optionalFetchSize,    
                                                            Optional<Boolean> optionalDistinct) {
        return new ListReadQuery(ctx, queryFactory, clauses, columnsToFetch, optionalLimit, optionalAllowFiltering, optionalFetchSize, optionalDistinct);
    }

    
    
    protected <E> ListRead<EntityList<E>> newListSelection(Context ctx, ListRead<RecordList> read, Class<?> clazz) {
        return new ListEntityReadQuery<>(ctx, queryFactory, read, clazz);
    }
}


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

import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.policies.RetryPolicy;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.ConsistencyLevel;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.unitedinternet.troilus.interceptor.ListReadQueryData;
import com.unitedinternet.troilus.interceptor.QueryInterceptor;
import com.unitedinternet.troilus.interceptor.WriteQueryData;

 

public class DaoImpl implements Dao {
    
    private final Context ctx;
    
    public DaoImpl(Session session, String tablename) {
        this(new Context(session, tablename));
    }
     
    private DaoImpl(Context ctx) {
        this.ctx = ctx;
    }
    
   
   
    
    @Override
    public Dao withConsistency(ConsistencyLevel consistencyLevel) {
        return new DaoImpl(ctx.withConsistency(consistencyLevel));
    }
    
    @Override
    public Dao withSerialConsistency(ConsistencyLevel consistencyLevel) {
        return new DaoImpl(ctx.withSerialConsistency(consistencyLevel));
    }
 
    @Override
    public Dao withTracking() {
        return new DaoImpl(ctx.withEnableTracking());
    }
    
    @Override
    public Dao withoutTracking() {
        return new DaoImpl(ctx.withDisableTracking());
    }

    @Override
    public Dao withRetryPolicy(RetryPolicy policy) {
        return new DaoImpl(ctx.withRetryPolicy(policy));
    }

    
    @Override
    public Dao withInterceptor(QueryInterceptor queryInterceptor) {
        return new DaoImpl(ctx.withInterceptor(queryInterceptor));
    }
    
    
    @Override
    public Insertion writeEntity(Object entity) {
        return new UpdateQuery(ctx, new WriteQueryDataAdapter()).entity(entity);
    }
    
    @Override
    public UpdateWithValuesAndCounter writeWhere(Clause... clauses) {
        return new UpdateQuery(ctx, new WriteQueryDataAdapter().whereConditions((ImmutableList.copyOf(clauses))));
    }
    
  
    @Override
    public WriteWithCounter writeWithKey(ImmutableMap<String, Object> composedKeyParts) {
        return new UpdateQuery(ctx, new WriteQueryDataAdapter().keys(composedKeyParts));
    }
    
    
    @Override
    public WriteWithCounter writeWithKey(String keyName, Object keyValue) {
        return writeWithKey(ImmutableMap.of(keyName, keyValue));
    }
    
    @Override
    public WriteWithCounter writeWithKey(String keyName1, Object keyValue1, 
                                         String keyName2, Object keyValue2) {
        return writeWithKey(ImmutableMap.of(keyName1, keyValue1,
                                            keyName2, keyValue2));
        
    }
    
    @Override
    public WriteWithCounter writeWithKey(String keyName1, Object keyValue1, 
                                         String keyName2, Object keyValue2, 
                                         String keyName3, Object keyValue3) {
        return writeWithKey(ImmutableMap.of(keyName1, keyValue1, 
                                            keyName2, keyValue2, 
                                            keyName3, keyValue3));
        
    }

    
    @Override
    public <T> WriteWithCounter writeWithKey(Name<T> keyName, T keyValue) {
        return writeWithKey(keyName.getName(), (Object) keyValue); 
    }
    
    @Override
    public <T, E> WriteWithCounter writeWithKey(Name<T> keyName1, T keyValue1,
                                                Name<E> keyName2, E keyValue2) {
        return writeWithKey(keyName1.getName(), (Object) keyValue1,
                            keyName2.getName(), (Object) keyValue2); 
    }
    
    @Override
    public <T, E, F> WriteWithCounter writeWithKey(Name<T> keyName1, T keyValue1, 
                                                   Name<E> keyName2, E keyValue2, 
                                                   Name<F> keyName3, F keyValue3) {
        return writeWithKey(keyName1.getName(), (Object) keyValue1,
                            keyName2.getName(), (Object) keyValue2,
                            keyName3.getName(), (Object) keyValue3); 
    }
    
    
    @Override
    public Deletion deleteWhere(Clause... whereConditions) {
        return new DeleteQuery(ctx, new DeleteQueryDataImpl().whereConditions(ImmutableList.copyOf(whereConditions)));
    };
   
    
    @Override
    public Deletion deleteWithKey(String keyName, Object keyValue) {
        
        return deleteWithKey(ImmutableMap.of(keyName, keyValue));
    }

    @Override
    public Deletion deleteWithKey(String keyName1, Object keyValue1, 
                                  String keyName2, Object keyValue2) {
        
        return deleteWithKey(ImmutableMap.of(keyName1, keyValue1, 
                                             keyName2, keyValue2));
    }
    
    @Override
    public Deletion deleteWithKey(String keyName1, Object keyValue1, 
                                  String keyName2, Object keyValue2, 
                                  String keyName3, Object keyValue3) {
        
        return deleteWithKey(ImmutableMap.of(keyName1, keyValue1,
                                             keyName2, keyValue2, 
                                             keyName3, keyValue3));
    }
    

    @Override
    public <T> Deletion deleteWithKey(Name<T> keyName, T keyValue) {
        return deleteWithKey(keyName.getName(), (Object) keyValue);
    }
    
    @Override
    public <T, E> Deletion deleteWithKey(Name<T> keyName1, T keyValue1,
                                         Name<E> keyName2, E keyValue2) {
        return deleteWithKey(keyName1.getName(), (Object) keyValue1,
                             keyName2.getName(), (Object) keyName2);

    }
    
    @Override
    public <T, E, F> Deletion deleteWithKey(Name<T> keyName1, T keyValue1,
                                            Name<E> keyName2, E keyValue2, 
                                            Name<F> keyName3, F keyValue3) {
        return deleteWithKey(keyName1.getName(), (Object) keyValue1,
                             keyName2.getName(), (Object) keyName2,
                             keyName3.getName(), (Object) keyName3);
    }
    
    public DeleteQuery deleteWithKey(ImmutableMap<String, Object> keyNameValuePairs) {
        return new DeleteQuery(ctx, new DeleteQueryDataImpl().keys(keyNameValuePairs));
    }
    
    
    
    @Override
    public SingleReadWithUnit<Optional<Record>> readWithKey(ImmutableMap<String, Object> composedkey) {
        return new SingleReadQuery(ctx, new SingleReadQueryDataImpl().keyParts(composedkey));
    }
    
    
    @Override
    public SingleReadWithUnit<Optional<Record>> readWithKey(String keyName, Object keyValue) {
        return readWithKey(ImmutableMap.of(keyName, keyValue));
    }
     
    @Override
    public SingleReadWithUnit<Optional<Record>> readWithKey(String keyName1, Object keyValue1, 
                                                            String keyName2, Object keyValue2) {
        return readWithKey(ImmutableMap.of(keyName1, keyValue1, 
                           keyName2, keyValue2));
    }
    
    @Override
    public SingleReadWithUnit<Optional<Record>> readWithKey(String keyName1, Object keyValue1, 
                                                            String keyName2, Object keyValue2,
                                                            String keyName3, Object keyValue3) {
        return readWithKey(ImmutableMap.of(keyName1, keyValue1, 
                                           keyName2, keyValue2, 
                                           keyName3, keyValue3));
    }
    
    
    @Override
    public <T> SingleReadWithUnit<Optional<Record>> readWithKey(Name<T> keyName, T keyValue) {
        return readWithKey(keyName.getName(), (Object) keyValue);
    }
    
    @Override
    public <T, E> SingleReadWithUnit<Optional<Record>> readWithKey(Name<T> keyName1, T keyValue1,
                                                                   Name<E> keyName2, E keyValue2) {
        return readWithKey(keyName1.getName(), (Object) keyValue1,
                           keyName2.getName(), (Object) keyValue2);
    }
    
    @Override
    public <T, E, F> SingleReadWithUnit<Optional<Record>> readWithKey(Name<T> keyName1, T keyValue1, 
                                                                      Name<E> keyName2, E keyValue2,
                                                                      Name<F> keyName3, F keyValue3) {
        return readWithKey(keyName1.getName(), (Object) keyValue1,
                           keyName2.getName(), (Object) keyValue2,                         
                           keyName3.getName(), (Object) keyValue3);
    }
    
    
    @Override
    public ListReadWithUnit<RecordList> readWithKeys(String name, ImmutableList<Object> values) {
        return new ListReadQuery(ctx, new ListReadQueryDataAdapter().keys(ImmutableMap.of(name, values)));
    }
    
    @Override
    public ListReadWithUnit<RecordList> readWithKeys(String composedKeyNamePart1, Object composedKeyValuePart1,
                                                     String composedKeyNamePart2, ImmutableList<Object> composedKeyValuesPart2) {
        return new ListReadQuery(ctx, new ListReadQueryDataAdapter().keys(ImmutableMap.of(composedKeyNamePart1, ImmutableList.of(composedKeyValuePart1),
                                                                                       composedKeyNamePart2, composedKeyValuesPart2)));        
    }
    
    @Override
    public ListReadWithUnit<RecordList> readWithKeys(String composedKeyNamePart1, Object composedKeyValuePart1,
                                                     String composedKeyNamePart2, Object composedKeyValuePart2,
                                                     String composedKeyNamePart3, ImmutableList<Object> composedKeyValuesPart3) {
        return new ListReadQuery(ctx, new ListReadQueryDataAdapter().keys(ImmutableMap.of(composedKeyNamePart1, ImmutableList.of(composedKeyValuePart1),
                                                                                       composedKeyNamePart2, ImmutableList.of(composedKeyValuePart2),
                                                                                       composedKeyNamePart3, composedKeyValuesPart3)));        
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> ListReadWithUnit<RecordList> readWithKeys(Name<T> name, ImmutableList<T> values) {
        return readWithKeys(name.getName(), (ImmutableList<Object>) values);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T, E> ListReadWithUnit<RecordList> readWithKeys(Name<T> composedKeyNamePart1, T composedKeyValuePart1,
                                                            Name<E> composedKeyNamePart2, ImmutableList<E> composedKeyValuesPart2) {
        return readWithKeys(composedKeyNamePart1.getName(), (Object) composedKeyValuePart1,
                            composedKeyNamePart2.getName(), (ImmutableList<Object>) composedKeyValuesPart2);
    }
    
    
    @SuppressWarnings("unchecked")
    @Override
    public <T, E, F> ListReadWithUnit<RecordList> readWithKeys( Name<T> composedKeyNamePart1, T composedKeyValuePart1,
                                                                Name<E> composedKeyNamePart2, E composedKeyValuePart2,
                                                                Name<F> composedKeyNamePart3, ImmutableList<F> composedKeyValuesPart3) {
        return readWithKeys(composedKeyNamePart1.getName(), (Object) composedKeyValuePart1,
                            composedKeyNamePart2.getName(), (Object) composedKeyValuePart2,
                            composedKeyNamePart3.getName(), (ImmutableList<Object>) composedKeyValuesPart3);
        
    }
    
    @Override
    public ListReadWithUnit<RecordList> readWhere(Clause... clauses) {
        return new ListReadQuery(ctx, new ListReadQueryDataAdapter().whereClauses(ImmutableSet.copyOf(clauses)));
    }
     
    
    @Override
    public ListReadWithUnit<RecordList> readAll() {
        return new ListReadQuery(ctx, new ListReadQueryDataAdapter().columnsToFetch(ImmutableMap.of()));
    }
    
    
    
    
    static class ListReadQueryDataAdapter implements ListReadQueryData {

        private final ListReadQueryDataImpl data;

        
        ListReadQueryDataAdapter() {
            this(new ListReadQueryDataImpl());
        }

        private ListReadQueryDataAdapter(ListReadQueryDataImpl data) {
            this.data = data;
        }
        

        @Override
        public ListReadQueryDataAdapter keys(ImmutableMap<String, ImmutableList<Object>> keys) {
            return new ListReadQueryDataAdapter(data.keys(keys));  
        }
        
        @Override
        public ListReadQueryDataAdapter whereClauses(ImmutableSet<Clause> whereClauses) {
            return new ListReadQueryDataAdapter(data.whereConditions(whereClauses));  
        }

        @Override
        public ListReadQueryDataAdapter columnsToFetch(ImmutableMap<String, Boolean> columnsToFetch) {
            return new ListReadQueryDataAdapter(data.columnsToFetch(columnsToFetch));  
        }

        @Override
        public ListReadQueryDataAdapter limit(Optional<Integer> optionalLimit) {
            return new ListReadQueryDataAdapter(data.limit(optionalLimit.orElse(null)));  
        }

        @Override
        public ListReadQueryDataAdapter allowFiltering(Optional<Boolean> optionalAllowFiltering) {
            return new ListReadQueryDataAdapter(data.allowFiltering(optionalAllowFiltering.orElse(null)));  
        }

        @Override
        public ListReadQueryDataAdapter fetchSize(Optional<Integer> optionalFetchSize) {
            return new ListReadQueryDataAdapter(data.fetchSize(optionalFetchSize.orElse(null)));  
        }

        @Override
        public ListReadQueryDataAdapter distinct(Optional<Boolean> optionalDistinct) {
            return new ListReadQueryDataAdapter(data.distinct(optionalDistinct.orElse(null)));  
        }
        
        @Override
        public ImmutableMap<String, ImmutableList<Object>> getKeys() {
            return data.getKeys();
        }
        
        @Override
        public ImmutableSet<Clause> getWhereClauses() {
            return data.getWhereClauses();
        }

        @Override
        public ImmutableMap<String, Boolean> getColumnsToFetch() {
            return data.getColumnsToFetch();
        }

        @Override
        public Optional<Integer> getLimit() {
            return Optional.ofNullable(data.getLimit());
        }

        @Override
        public Optional<Boolean> getAllowFiltering() {
            return Optional.ofNullable(data.getAllowFiltering());
        }

        @Override
        public Optional<Integer> getFetchSize() {
            return Optional.ofNullable(data.getFetchSize());
        }

        @Override
        public Optional<Boolean> getDistinct() {
            return Optional.ofNullable(data.getDistinct());
        }
        
        static Statement toStatement(ListReadQueryData data, Context ctx) {
            com.unitedinternet.troilus.minimal.ListReadQueryData q = new ListReadQueryDataImpl().keys(data.getKeys())
                                                 .whereConditions(data.getWhereClauses())
                                                 .columnsToFetch(data.getColumnsToFetch())
                                                 .limit(data.getLimit().orElse(null))
                                                 .allowFiltering(data.getAllowFiltering().orElse(null))
                                                 .fetchSize(data.getFetchSize().orElse(null))
                                                 .distinct(data.getDistinct().orElse(null));
            return ListReadQueryDataImpl.toStatement(q, ctx);
        }
    }
    
    
    static class WriteQueryDataAdapter implements WriteQueryData {

        private final WriteQueryDataImpl data;
            
        WriteQueryDataAdapter() {
            this(new WriteQueryDataImpl());
        }

        private WriteQueryDataAdapter(WriteQueryDataImpl data) {
            this.data = data;
        }
        
        @Override
        public WriteQueryDataAdapter keys(ImmutableMap<String, Object> keys) {
            return new WriteQueryDataAdapter(data.keys(keys));
        }
        
        @Override
        public WriteQueryDataAdapter whereConditions(ImmutableList<Clause> whereConditions) {
            return new WriteQueryDataAdapter(data.whereConditions(whereConditions));
        }

        @Override
        public WriteQueryDataAdapter valuesToMutate(ImmutableMap<String, Optional<Object>> valuesToMutate) {
            return new WriteQueryDataAdapter(data.valuesToMutate(GuavaOptionals.toStringOptionalMap(valuesToMutate)));
        }
     
        @Override
        public WriteQueryDataAdapter setValuesToAdd(ImmutableMap<String, ImmutableSet<Object>> setValuesToAdd) {
            return new WriteQueryDataAdapter(data.setValuesToAdd(setValuesToAdd));
        }
        
        @Override
        public WriteQueryDataAdapter setValuesToRemove(ImmutableMap<String, ImmutableSet<Object>> setValuesToRemove) {
            return new WriteQueryDataAdapter(data.setValuesToRemove(setValuesToRemove));
        }
     
        @Override
        public WriteQueryDataAdapter listValuesToAppend(ImmutableMap<String, ImmutableList<Object>> listValuesToAppend) {
            return new WriteQueryDataAdapter(data.listValuesToAppend(listValuesToAppend));
        }
       
        @Override
        public WriteQueryDataAdapter listValuesToPrepend(ImmutableMap<String, ImmutableList<Object>> listValuesToPrepend) {
            return new WriteQueryDataAdapter(data.listValuesToPrepend(listValuesToPrepend));
        }
     
        @Override
        public WriteQueryDataAdapter listValuesToRemove(ImmutableMap<String, ImmutableList<Object>> listValuesToRemove) {
            return new WriteQueryDataAdapter(data.listValuesToRemove(listValuesToRemove));
        }
     
        @Override
        public WriteQueryDataAdapter mapValuesToMutate(ImmutableMap<String, ImmutableMap<Object, Optional<Object>>> mapValuesToMutate) {
            // convert java optional to guava optional
           Map<String, ImmutableMap<Object, com.google.common.base.Optional<Object>>> result = Maps.newHashMap();
            
            for (Entry<String, ImmutableMap<Object, Optional<Object>>> entry : mapValuesToMutate.entrySet()) {
                Map<Object, com.google.common.base.Optional<Object>> iresult = Maps.newHashMap();
                for (Entry<Object, Optional<Object>> entry2 : entry.getValue().entrySet()) {
                    iresult.put(entry2.getKey(), com.google.common.base.Optional.fromNullable(entry2.getValue().orElse(null)));
                }
                result.put(entry.getKey(), ImmutableMap.copyOf(iresult));
            }
            
            return new WriteQueryDataAdapter(data.mapValuesToMutate(ImmutableMap.copyOf(result)));
        }
       
        @Override
        public WriteQueryDataAdapter onlyIfConditions(ImmutableList<Clause> onlyIfConditions) {
            return new WriteQueryDataAdapter(data.onlyIfConditions(onlyIfConditions));
        }

        @Override
        public WriteQueryDataAdapter ifNotExists(Optional<Boolean> ifNotExists) {
            return new WriteQueryDataAdapter(data.ifNotExists(ifNotExists.orElse(null)));
        }
        
        @Override        
        public ImmutableMap<String, Object> getKeys() {
            return data.getKeyNameValuePairs();
        }

        @Override
        public ImmutableList<Clause> getWhereConditions() {
            return data.getWhereConditions();
        }

        @Override
        public ImmutableMap<String, Optional<Object>> getValuesToMutate() {
            return GuavaOptionals.fromStringOptionalMap(data.getValuesToMutate());
        }

        @Override
        public ImmutableMap<String, ImmutableSet<Object>> getSetValuesToAdd() {
            return data.getSetValuesToAdd();
        }

        @Override
        public ImmutableMap<String, ImmutableSet<Object>> getSetValuesToRemove() {
            return data.getSetValuesToRemove();
        }

        @Override
        public ImmutableMap<String, ImmutableList<Object>> getListValuesToAppend() {
            return data.getListValuesToAppend();
        }

        @Override
        public ImmutableMap<String, ImmutableList<Object>> getListValuesToPrepend() {
            return data.getListValuesToPrepend();
        }

        @Override
        public ImmutableMap<String, ImmutableList<Object>> getListValuesToRemove() {
            return data.getListValuesToRemove();
        }

        @Override
        public ImmutableMap<String, ImmutableMap<Object, Optional<Object>>> getMapValuesToMutate() {
            return GuavaOptionals.fromStringObjectOptionalMap(data.getMapValuesToMutate());
        }

        @Override
        public ImmutableList<Clause> getOnlyIfConditions() {
            return data.getOnlyIfConditions();
        }
        
        @Override
        public Optional<Boolean> getIfNotExits() {
            return Optional.ofNullable(data.getIfNotExits());
        }
        
        static Statement toStatement(WriteQueryData data, Context ctx) {
            WriteQueryDataImpl wqd = new WriteQueryDataImpl().keys(data.getKeys())
                                                             .whereConditions(data.getWhereConditions())
                                                             .valuesToMutate(GuavaOptionals.toStringOptionalMap(data.getValuesToMutate()))
                                                             .setValuesToAdd(data.getSetValuesToAdd())
                                                             .setValuesToRemove(data.getSetValuesToRemove())
                                                             .listValuesToAppend(data.getListValuesToAppend())
                                                             .listValuesToPrepend(data.getListValuesToPrepend())
                                                             .listValuesToRemove(data.getListValuesToRemove())
                                                             .mapValuesToMutate(GuavaOptionals.toStringObjectOptionalMap(data.getMapValuesToMutate()))
                                                             .onlyIfConditions(data.getOnlyIfConditions())
                                                             .ifNotExists(data.getIfNotExits().orElse(null));
            return WriteQueryDataImpl.toStatement(wqd, ctx);
        }
    }
}
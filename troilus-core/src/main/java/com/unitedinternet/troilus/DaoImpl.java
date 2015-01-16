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


import java.util.Iterator;
import java.util.Map;
import java.util.Optional;







import java.util.Map.Entry;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.RetryPolicy;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.ConsistencyLevel;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.unitedinternet.troilus.interceptor.ListReadQueryData;
import com.unitedinternet.troilus.interceptor.ListReadQueryPostInterceptor;
import com.unitedinternet.troilus.interceptor.QueryInterceptor;
import com.unitedinternet.troilus.interceptor.SingleReadQueryData;
import com.unitedinternet.troilus.interceptor.SingleReadQueryPostInterceptor;
import com.unitedinternet.troilus.interceptor.WriteQueryData;
import com.unitedinternet.troilus.interceptor.WriteQueryPreInterceptor;
import com.unitedinternet.troilus.interceptor.ListReadQueryPreInterceptor;

 

/**
 * DaoImpl
 */
public class DaoImpl implements Dao {
    
    private final Context ctx;
    
    /**
     * @param session     the underyling session
     * @param tablename   the tablename
     */
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
         
        Context context = ctx.withInterceptor(queryInterceptor);
        
        if (ListReadQueryPreInterceptor.class.isAssignableFrom(queryInterceptor.getClass())) {
            context = context.withInterceptor(new ListReadQueryPreInterceptorAdapter((ListReadQueryPreInterceptor) queryInterceptor));
        }

        if (ListReadQueryPostInterceptor.class.isAssignableFrom(queryInterceptor.getClass())) {
            context = context.withInterceptor(new ListReadQueryPostInterceptorAdapter((ListReadQueryPostInterceptor) queryInterceptor));
        } 
                
        if (SingleReadQueryPostInterceptor.class.isAssignableFrom(queryInterceptor.getClass())) {
            context = context.withInterceptor(new SingleReadQueryPostInterceptorAdapter((SingleReadQueryPostInterceptor) queryInterceptor));
        } 
        
        if (WriteQueryPreInterceptor.class.isAssignableFrom(queryInterceptor.getClass())) {
            context = context.withInterceptor(new WriteQueryPreInterceptorAdapter((WriteQueryPreInterceptor) queryInterceptor));
        } 
        
        return new DaoImpl(context);
    }
    
    
    @Override
    public Insertion writeEntity(Object entity) {
        return new UpdateQueryAdapter(ctx, new UpdateQuery(ctx, new WriteQueryDataImpl())).entity(entity);
    }
    
    @Override
    public UpdateWithValuesAndCounter writeWhere(Clause... clauses) {
        return new UpdateQueryAdapter(ctx, new UpdateQuery(ctx, new WriteQueryDataImpl().whereConditions((ImmutableList.copyOf(clauses)))));
    }
    
    @Override
    public WriteWithCounter writeWithKey(ImmutableMap<String, Object> composedKeyParts) {
        return new UpdateQueryAdapter(ctx, new UpdateQuery(ctx, new WriteQueryDataImpl().keys(composedKeyParts)));
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
        return new DeleteQueryAdapter(ctx, new DeleteQuery(ctx, new DeleteQueryDataImpl().whereConditions(ImmutableList.copyOf(whereConditions))));      
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
    
    public Deletion deleteWithKey(ImmutableMap<String, Object> keyNameValuePairs) {
        return new DeleteQueryAdapter(ctx, new DeleteQuery(ctx, new DeleteQueryDataImpl().keys(keyNameValuePairs)));      
    }
    
    
    
    @Override
    public SingleReadWithUnit<Optional<Record>> readWithKey(ImmutableMap<String, Object> composedkey) {
        return new SingleReadQueryAdapter(ctx, new SingleReadQuery(ctx, new SingleReadQueryDataImpl().key(composedkey)));
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
        return new ListReadQueryAdapter(ctx, new ListReadQuery(ctx, new ListReadQueryDataImpl().keys(ImmutableMap.of(name, values))));
    }
    
    @Override
    public ListReadWithUnit<RecordList> readWithKeys(String composedKeyNamePart1, Object composedKeyValuePart1,
                                                     String composedKeyNamePart2, ImmutableList<Object> composedKeyValuesPart2) {
        return new ListReadQueryAdapter(ctx, new ListReadQuery(ctx, new ListReadQueryDataImpl().keys(ImmutableMap.of(composedKeyNamePart1, ImmutableList.of(composedKeyValuePart1),
                composedKeyNamePart2, composedKeyValuesPart2))));
    }
    
    @Override
    public ListReadWithUnit<RecordList> readWithKeys(String composedKeyNamePart1, Object composedKeyValuePart1,
                                                     String composedKeyNamePart2, Object composedKeyValuePart2,
                                                     String composedKeyNamePart3, ImmutableList<Object> composedKeyValuesPart3) {
        return new ListReadQueryAdapter(ctx, new ListReadQuery(ctx, new ListReadQueryDataImpl().keys(ImmutableMap.of(composedKeyNamePart1, ImmutableList.of(composedKeyValuePart1),
                                                                                       composedKeyNamePart2, ImmutableList.of(composedKeyValuePart2),
                                                                                       composedKeyNamePart3, composedKeyValuesPart3))));        
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
        return new ListReadQueryAdapter(ctx, new ListReadQuery(ctx, new ListReadQueryDataImpl().whereConditions(ImmutableSet.copyOf(clauses))));
    }
     
    @Override
    public ListReadWithUnit<RecordList> readAll() {
        return new ListReadQueryAdapter(ctx, new ListReadQuery(ctx, new ListReadQueryDataImpl().columnsToFetch(ImmutableMap.of())));
    }

    
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                          .add("ctx", ctx)
                          .toString();
    }
    
    

    
    /**
     * Java8 adapter of a ListReadQueryData
     */
    static class ListReadQueryDataAdapter implements ListReadQueryData {

        private final com.unitedinternet.troilus.minimal.interceptor.ListReadQueryData data;

        
        ListReadQueryDataAdapter() {
            this(new ListReadQueryDataImpl());
        }

        private ListReadQueryDataAdapter(com.unitedinternet.troilus.minimal.interceptor.ListReadQueryData data) {
            this.data = data;
        }
        

        @Override
        public ListReadQueryDataAdapter keys(ImmutableMap<String, ImmutableList<Object>> keys) {
            return new ListReadQueryDataAdapter(data.keys(keys));  
        }
        
        @Override
        public ListReadQueryDataAdapter whereConditions(ImmutableSet<Clause> whereConditions) {
            return new ListReadQueryDataAdapter(data.whereConditions(whereConditions));  
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
        public ImmutableSet<Clause> getWhereConditions() {
            return data.getWhereConditions();
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
        
        static com.unitedinternet.troilus.minimal.interceptor.ListReadQueryData convert(ListReadQueryData data) {
            return new ListReadQueryDataImpl().keys(data.getKeys())
                                              .whereConditions(data.getWhereConditions())
                                              .columnsToFetch(data.getColumnsToFetch())
                                              .limit(data.getLimit().orElse(null))
                                              .allowFiltering(data.getAllowFiltering().orElse(null))
                                              .fetchSize(data.getFetchSize().orElse(null))
                                              .distinct(data.getDistinct().orElse(null));
        }
    }
    


    /**
     * Java8 adapter of a RecordList
     */
    static class RecordListAdapter implements RecordList {
        private final com.unitedinternet.troilus.minimal.MinimalDao.RecordList recordList;
        
        public RecordListAdapter(com.unitedinternet.troilus.minimal.MinimalDao.RecordList recordList) {
            this.recordList = recordList;
        }
        
        @Override
        public ExecutionInfo getExecutionInfo() {
            return recordList.getExecutionInfo();
        }
        
        @Override
        public ImmutableList<ExecutionInfo> getAllExecutionInfo() {
            return recordList.getAllExecutionInfo();
        }

        @Override
        public boolean wasApplied() {
            return recordList.wasApplied();
        }
        
        @Override
        public Iterator<Record> iterator() {
            
            return new Iterator<Record>() {
                private final Iterator<com.unitedinternet.troilus.minimal.Record> iterator = recordList.iterator();

                @Override
                public boolean hasNext() {
                    return iterator.hasNext();
                }
                
                @Override
                public Record next() {
                    return new RecordAdapter(iterator.next());
                }
            };
        }
        
        @SuppressWarnings({ "unchecked", "rawtypes" })
        @Override
        public void subscribe(Subscriber<? super Record> subscriber) {
            recordList.subscribe(new SubscriberAdapter(subscriber));
        }
        
        
        static com.unitedinternet.troilus.minimal.MinimalDao.RecordList convert(RecordList recordList) {
            
            return new com.unitedinternet.troilus.minimal.MinimalDao.RecordList() {
                
                @Override
                public boolean wasApplied() {
                    return recordList.wasApplied();
                }
                
                @Override
                public ExecutionInfo getExecutionInfo() {
                    return recordList.getExecutionInfo();
                }
                
                @Override
                public ImmutableList<ExecutionInfo> getAllExecutionInfo() {
                    return recordList.getAllExecutionInfo();
                }
                
                @SuppressWarnings({ "unchecked", "rawtypes" })
                public void subscribe(Subscriber<? super com.unitedinternet.troilus.minimal.Record> subscriber) {
                    recordList.subscribe(new SubscriberAdapter(subscriber));
                }
                
                public Iterator<com.unitedinternet.troilus.minimal.Record> iterator() {
                    
                    return new Iterator<com.unitedinternet.troilus.minimal.Record>() {
                        
                        private final Iterator<Record> iterator = recordList.iterator();

                        @Override
                        public boolean hasNext() {
                            return iterator.hasNext();
                        }
                        
                        @Override
                        public com.unitedinternet.troilus.minimal.Record next() {
                            return RecordAdapter.convert(iterator.next());
                        }
                    };
                }
            };
        }
   }

   
   
   static final class SubscriberAdapter<T> implements Subscriber<T> {
       private final Subscriber<T> subscriber;
       
       public SubscriberAdapter(Subscriber<T> subscriber) {
           this.subscriber = subscriber;
      }

      @Override
      public void onSubscribe(Subscription s) {
          subscriber.onSubscribe(s);
      }

      @Override
      public void onNext(T t) {
          subscriber.onNext(t);
      }

      @Override
      public void onError(Throwable t) {
          subscriber.onError(t);
      }
   
      @Override
      public void onComplete() {
          subscriber.onComplete();
      }
   }

   
        
   static class EntityListAdapter<F> implements EntityList<F> {
       private final com.unitedinternet.troilus.minimal.MinimalDao.EntityList<F> entityList;
   
       
       public EntityListAdapter(com.unitedinternet.troilus.minimal.MinimalDao.EntityList<F> entityList) {
           this.entityList = entityList;
       }
   
       @Override
       public ExecutionInfo getExecutionInfo() {
           return entityList.getExecutionInfo();
       }
       
       @Override
       public ImmutableList<ExecutionInfo> getAllExecutionInfo() {
           return entityList.getAllExecutionInfo();
       }
       
       @Override
       public boolean wasApplied() {
           return entityList.wasApplied();
       }
   
       @Override
       public Iterator<F> iterator() {
   
           return new Iterator<F>() {
               final Iterator<F> recordIt = entityList.iterator();
               
               @Override
               public boolean hasNext() {
                   return recordIt.hasNext();
               }
           
               @Override
               public F next() {
                   return recordIt.next();
               }
           };
       }
       
        
       @SuppressWarnings({ "unchecked", "rawtypes" })
       @Override
       public void subscribe(Subscriber<? super F> subscriber) {
           entityList.subscribe(new SubscriberAdapter(subscriber));
       }
   }
   

   
    
    private static class WriteQueryDataAdapter implements WriteQueryData {

        private final com.unitedinternet.troilus.minimal.interceptor.WriteQueryData data;
            
        WriteQueryDataAdapter(com.unitedinternet.troilus.minimal.interceptor.WriteQueryData data) {
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
            return new WriteQueryDataAdapter(data.valuesToMutate(toGuavaOptional(valuesToMutate)));
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
            return data.getKeys();
        }

        @Override
        public ImmutableList<Clause> getWhereConditions() {
            return data.getWhereConditions();
        }

        @Override
        public ImmutableMap<String, Optional<Object>> getValuesToMutate() {
            return fromGuavaOptional(data.getValuesToMutate());
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
        
        @Override
        public ImmutableList<Clause> getOnlyIfConditions() {
            return data.getOnlyIfConditions();
        }
        
        @Override
        public Optional<Boolean> getIfNotExits() {
            return Optional.ofNullable(data.getIfNotExits());
        }
        
        
        
        private  static ImmutableMap<String, Optional<Object>> fromGuavaOptional(ImmutableMap<String, com.google.common.base.Optional<Object>> map) {
            Map<String, Optional<Object>> result = Maps.newHashMap();
            for (Entry<String, com.google.common.base.Optional<Object>> entry : map.entrySet()) {
                result.put(entry.getKey(), Optional.ofNullable(entry.getValue().orNull()));
            }
            
            return ImmutableMap.copyOf(result);        
        }

        
        private static ImmutableMap<String, com.google.common.base.Optional<Object>> toGuavaOptional(ImmutableMap<String, Optional<Object>> map) {
            Map<String, com.google.common.base.Optional<Object>> result = Maps.newHashMap();
            for (Entry<String, Optional<Object>> entry : map.entrySet()) {
                result.put(entry.getKey(), com.google.common.base.Optional.fromNullable(entry.getValue().orElse(null)));
            }
                
            return ImmutableMap.copyOf(result);
        }
        

        private static ImmutableMap<String, ImmutableMap<Object, com.google.common.base.Optional<Object>>> toGuavaOptional(Map<String, ImmutableMap<Object, Optional<Object>>> map) {
            Map<String, ImmutableMap<Object, com.google.common.base.Optional<Object>>> result = Maps.newHashMap();
            for (Entry<String, ImmutableMap<Object, Optional<Object>>> entry : map.entrySet()) {
                Map<Object, com.google.common.base.Optional<Object>> iresult = Maps.newHashMap();
                for (Entry<Object, Optional<Object>> entry2 : entry.getValue().entrySet()) {
                    iresult.put(entry2.getKey(), com.google.common.base.Optional.fromNullable(entry2.getValue().orElse(null)));
                }
                result.put(entry.getKey(), ImmutableMap.copyOf(iresult));
            }
            
            return ImmutableMap.copyOf(result);
        }
        
        static com.unitedinternet.troilus.minimal.interceptor.WriteQueryData convert(WriteQueryData data) {
            return new WriteQueryDataImpl().keys(data.getKeys())
                                           .whereConditions(data.getWhereConditions())
                                           .valuesToMutate(toGuavaOptional(data.getValuesToMutate()))
                                           .setValuesToAdd(data.getSetValuesToAdd())
                                           .setValuesToRemove(data.getSetValuesToRemove())
                                           .listValuesToAppend(data.getListValuesToAppend())
                                           .listValuesToPrepend(data.getListValuesToPrepend())
                                           .listValuesToRemove(data.getListValuesToRemove())
                                           .mapValuesToMutate(toGuavaOptional(data.getMapValuesToMutate()))
                                           .onlyIfConditions(data.getOnlyIfConditions())
                                           .ifNotExists(data.getIfNotExits().orElse(null));
        }
    }
    
    

    
    private static final class ListReadQueryPreInterceptorAdapter implements com.unitedinternet.troilus.minimal.interceptor.ListReadQueryPreInterceptor {
        
        private ListReadQueryPreInterceptor interceptor;
        
        public ListReadQueryPreInterceptorAdapter(ListReadQueryPreInterceptor interceptor) {
            this.interceptor = interceptor;
        }
        
        @Override
        public com.unitedinternet.troilus.minimal.interceptor.ListReadQueryData onPreListRead(com.unitedinternet.troilus.minimal.interceptor.ListReadQueryData data) {
            return ListReadQueryDataAdapter.convert(interceptor.onPreListRead(new ListReadQueryDataAdapter(data)));
        }
        
        @Override
        public String toString() {
            return "ListReadQueryPreInterceptor (with " + interceptor + ")";
        }
    }
   
    
    private static final class ListReadQueryPostInterceptorAdapter implements com.unitedinternet.troilus.minimal.interceptor.ListReadQueryPostInterceptor {
        
        private ListReadQueryPostInterceptor interceptor;
        
        public ListReadQueryPostInterceptorAdapter(ListReadQueryPostInterceptor interceptor) {
            this.interceptor = interceptor;
        }
        
        @Override
        public com.unitedinternet.troilus.minimal.MinimalDao.RecordList onPostListRead(com.unitedinternet.troilus.minimal.interceptor.ListReadQueryData data, com.unitedinternet.troilus.minimal.MinimalDao.RecordList recordList) {
            return RecordListAdapter.convert(interceptor.onPostListRead(new ListReadQueryDataAdapter(data), new RecordListAdapter(recordList)));
        }
        
        @Override
        public String toString() {
            return "ListReadQueryPostInterceptor (with " + interceptor + ")";
        }
    }
    

    private static final class SingleReadQueryPostInterceptorAdapter implements com.unitedinternet.troilus.minimal.interceptor.SingleReadQueryPostInterceptor {
        
        private SingleReadQueryPostInterceptor interceptor;
        
        public SingleReadQueryPostInterceptorAdapter(SingleReadQueryPostInterceptor interceptor) {
            this.interceptor = interceptor;
        }
        
        @Override
        public com.unitedinternet.troilus.minimal.Record onPostSingleRead(SingleReadQueryData data, com.unitedinternet.troilus.minimal.Record record) {
            return RecordAdapter.convert(interceptor.onPostSingleRead(data, (record == null) ? Optional.empty() : Optional.of(new RecordAdapter(record))).orElse(null));
        }
        
        @Override
        public String toString() {
            return "SingleReadQueryPostInterceptorAdapter (with " + interceptor + ")";
        }
    }
    
    
    
    private static final class WriteQueryPreInterceptorAdapter implements com.unitedinternet.troilus.minimal.interceptor.WriteQueryPreInterceptor {
         
        private WriteQueryPreInterceptor interceptor;
        
        public WriteQueryPreInterceptorAdapter(WriteQueryPreInterceptor interceptor) {
            this.interceptor = interceptor;
        }
        
        @Override
        public com.unitedinternet.troilus.minimal.interceptor.WriteQueryData onPreWrite(com.unitedinternet.troilus.minimal.interceptor.WriteQueryData data) {
            return WriteQueryDataAdapter.convert(interceptor.onPreWrite(new WriteQueryDataAdapter(data)));
        }
        
        @Override
        public String toString() {
            return "WriteQueryPreInterceptorAdapter (with " + interceptor + ")";
        }
    }
}
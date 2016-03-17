/*
 * Copyright 1&1 Internet AG, https://github.com/1and1/
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.oneandone.troilus;


import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import net.oneandone.troilus.interceptor.CascadeOnDeleteInterceptor;
import net.oneandone.troilus.interceptor.CascadeOnWriteInterceptor;
import net.oneandone.troilus.interceptor.DeleteQueryData;
import net.oneandone.troilus.interceptor.DeleteQueryRequestInterceptor;
import net.oneandone.troilus.interceptor.QueryInterceptor;
import net.oneandone.troilus.interceptor.ReadQueryData;
import net.oneandone.troilus.interceptor.ReadQueryRequestInterceptor;
import net.oneandone.troilus.interceptor.ReadQueryResponseInterceptor;
import net.oneandone.troilus.interceptor.WriteQueryData;
import net.oneandone.troilus.interceptor.WriteQueryRequestInterceptor;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.RetryPolicy;
import com.datastax.driver.core.querybuilder.Clause;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

 

/**
 * DaoImpl
 */
public class DaoImpl implements Dao {
    
    private final Tablename tablename;
    private final Context ctx;
    
    /**
     * @param session     the underlying session which has an assigned keyspace
     * @param tablename   the table name
     */
    public DaoImpl(Session session, String tablename) {
        this(new Context(session), Tablename.newTablename(session, tablename));
    }

    
    /**
     * @param session      the underlying session
     * @param tablename    the table name
     * @param keyspacename the keyspacename
     */
    public DaoImpl(Session session, String keyspacename, String tablename) {
        this(new Context(session), Tablename.newTablename(keyspacename, tablename));
    }

 
    private DaoImpl(Context ctx, Tablename tablename) {
        this.ctx = ctx;
        this.tablename = tablename;
    }
    
    
    @Override
    public Dao withConsistency(ConsistencyLevel consistencyLevel) {
        return new DaoImpl(ctx.withConsistency(consistencyLevel), this.tablename);
    }
    
    @Override
    public Dao withSerialConsistency(ConsistencyLevel consistencyLevel) {
        return new DaoImpl(ctx.withSerialConsistency(consistencyLevel), this.tablename);
    }
 
    @Override
    public Dao withTracking() {
        return new DaoImpl(ctx.withTracking(), this.tablename);
    }
    
    @Override
    public Dao withoutTracking() {
        return new DaoImpl(ctx.withoutTracking(), this.tablename);
    }

    @Override
    public Dao withRetryPolicy(RetryPolicy policy) {
        return new DaoImpl(ctx.withRetryPolicy(policy), this.tablename);
    }

    
    @Override
    public Dao withInterceptor(QueryInterceptor queryInterceptor) {
        Context context = ctx.withInterceptor(queryInterceptor);
        
        if (ReadQueryRequestInterceptor.class.isAssignableFrom(queryInterceptor.getClass())) {
            context = context.withInterceptor(queryInterceptor);
        }

        if (ReadQueryResponseInterceptor.class.isAssignableFrom(queryInterceptor.getClass())) {
            context = context.withInterceptor(queryInterceptor);
        } 

        if (WriteQueryRequestInterceptor.class.isAssignableFrom(queryInterceptor.getClass())) {
            context = context.withInterceptor(queryInterceptor);
        } 

        if (DeleteQueryRequestInterceptor.class.isAssignableFrom(queryInterceptor.getClass())) {
            context = context.withInterceptor(queryInterceptor);
        } 

        if (CascadeOnWriteInterceptor.class.isAssignableFrom(queryInterceptor.getClass())) {
            context = context.withInterceptor(queryInterceptor);
        }

        if (CascadeOnDeleteInterceptor.class.isAssignableFrom(queryInterceptor.getClass())) {
            context = context.withInterceptor(queryInterceptor);
        }

        return new DaoImpl(context, this.tablename);
    }
    
    
    @Override
    public Insertion writeEntity(Object entity) {
        final ImmutableMap<String, Optional<Object>> values = ctx.getBeanMapper().toValues(entity, ctx.getCatalog().getColumnNames(tablename));
        return new InsertQuery(ctx, new WriteQueryDataImpl(tablename).valuesToMutate(values));
    }
    
    @Override
    public UpdateWithUnitAndCounter writeWhere(Clause... clauses) {
        return new UpdateQuery(ctx, new WriteQueryDataImpl(tablename).whereConditions((ImmutableList.copyOf(clauses))));
    }
    
    @Override
    public WriteWithCounter writeWithKey(ImmutableMap<String, Object> composedKeyParts) {
        return new WriteWithCounterQuery(ctx, new WriteQueryDataImpl(tablename).keys(composedKeyParts));
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
    public <T> WriteWithCounter writeWithKey(ColumnName<T> keyName, T keyValue) {
        return writeWithKey(keyName.getName(), (Object) keyValue); 
    }
    
    @Override
    public <T, E> WriteWithCounter writeWithKey(ColumnName<T> keyName1, T keyValue1,
                                                ColumnName<E> keyName2, E keyValue2) {
        return writeWithKey(keyName1.getName(), (Object) keyValue1,
                            keyName2.getName(), (Object) keyValue2); 
    }
    
    @Override
    public <T, E, F> WriteWithCounter writeWithKey(ColumnName<T> keyName1, T keyValue1, 
                                                   ColumnName<E> keyName2, E keyValue2, 
                                                   ColumnName<F> keyName3, F keyValue3) {
        return writeWithKey(keyName1.getName(), (Object) keyValue1,
                            keyName2.getName(), (Object) keyValue2,
                            keyName3.getName(), (Object) keyValue3); 
    }
    

    
    
    @Override
    public Deletion deleteWhere(final Clause... whereConditions) {
        return new DeleteQuery(ctx, new DeleteQueryDataImpl(tablename).whereConditions(ImmutableList.copyOf(whereConditions)));      
    }
   
    @Override
    public Deletion deleteWithKey(final String keyName, final Object keyValue) {
        return deleteWithKey(ImmutableMap.of(keyName, keyValue));
    }

    @Override
    public Deletion deleteWithKey(final String keyName1, final Object keyValue1, 
                                  final String keyName2, final Object keyValue2) {
        return deleteWithKey(ImmutableMap.of(keyName1, keyValue1, 
                                             keyName2, keyValue2));
    }
    
    @Override
    public Deletion deleteWithKey(final String keyName1, final Object keyValue1, 
                                  final String keyName2, final Object keyValue2, 
                                  final String keyName3, final Object keyValue3) {
        return deleteWithKey(ImmutableMap.of(keyName1, keyValue1,
                                             keyName2, keyValue2, 
                                             keyName3, keyValue3));
    }
    
    @Override
    public <T> Deletion deleteWithKey(final ColumnName<T> keyName, final T keyValue) {
        return deleteWithKey(keyName.getName(), (Object) keyValue);
    }
    
    @Override
    public <T, E> Deletion deleteWithKey(final ColumnName<T> keyName1, final T keyValue1,
                                         final ColumnName<E> keyName2, final E keyValue2) {
        return deleteWithKey(keyName1.getName(), (Object) keyValue1,
                             keyName2.getName(), (Object) keyValue2);

    }
    
    @Override
    public <T, E, F> Deletion deleteWithKey(final ColumnName<T> keyName1, final T keyValue1,
                                            final ColumnName<E> keyName2, final E keyValue2, 
                                            final ColumnName<F> keyName3, final F keyValue3) {
        return deleteWithKey(keyName1.getName(), (Object) keyValue1,
                             keyName2.getName(), (Object) keyValue2,
                             keyName3.getName(), (Object) keyValue3);
    }
    
    public Deletion deleteWithKey(final ImmutableMap<String, Object> keyNameValuePairs) {
        return new DeleteQuery(ctx, new DeleteQueryDataImpl(tablename).key(keyNameValuePairs));      
    }
    
    
    
    @Override
    public SingleReadWithUnit<Optional<Record>, Record> readWithKey(final ImmutableMap<String, Object> composedkey) {
        final Map<String, ImmutableList<Object>> keys = Maps.newHashMap();
        for (Entry<String, Object> entry : composedkey.entrySet()) {
            keys.put(entry.getKey(), ImmutableList.of(entry.getValue()));
        }
        
        return new SingleReadQuery(ctx, new ReadQueryDataImpl(tablename).keys(ImmutableMap.copyOf(keys)));
    }
    
    @Override
    public SingleReadWithUnit<Optional<Record>, Record> readWithKey(final String keyName, final Object keyValue) {
        return readWithKey(ImmutableMap.of(keyName, keyValue));
    }
     
    @Override
    public SingleReadWithUnit<Optional<Record>, Record> readWithKey(final String keyName1, final Object keyValue1, 
                                                                    final String keyName2, final Object keyValue2) {
        return readWithKey(ImmutableMap.of(keyName1, keyValue1, 
                           keyName2, keyValue2));
    }
    
    @Override
    public SingleReadWithUnit<Optional<Record>, Record> readWithKey(final String keyName1, final Object keyValue1, 
                                                                    final String keyName2, final Object keyValue2,
                                                                    final String keyName3, final Object keyValue3) {
        return readWithKey(ImmutableMap.of(keyName1, keyValue1, 
                                           keyName2, keyValue2, 
                                           keyName3, keyValue3));
    }
    
    @Override
    public <T> SingleReadWithUnit<Optional<Record>, Record> readWithKey(final ColumnName<T> keyName, final T keyValue) {
        return readWithKey(keyName.getName(), (Object) keyValue);
    }
    
    @Override
    public <T, E> SingleReadWithUnit<Optional<Record>, Record> readWithKey(final ColumnName<T> keyName1, final T keyValue1,
                                                                           final ColumnName<E> keyName2, final E keyValue2) {
        return readWithKey(keyName1.getName(), (Object) keyValue1,
                           keyName2.getName(), (Object) keyValue2);
    }
    
    @Override
    public <T, E, F> SingleReadWithUnit<Optional<Record>, Record> readWithKey(final ColumnName<T> keyName1, final T keyValue1, 
                                                                              final ColumnName<E> keyName2, final E keyValue2,
                                                                              final ColumnName<F> keyName3, final F keyValue3) {
        return readWithKey(keyName1.getName(), (Object) keyValue1,
                           keyName2.getName(), (Object) keyValue2,                         
                           keyName3.getName(), (Object) keyValue3);
    }
    
    
    @Override
    public ListReadWithUnit<ResultList<Record>, Record> readSequenceWithKeys(final String name, final ImmutableList<Object> values) {
        return new ListReadQuery(ctx, new ReadQueryDataImpl(tablename).keys(ImmutableMap.of(name, values)));
    }
    
    @Override
    public ListReadWithUnit<ResultList<Record>, Record> readSequenceWithKeys(final String composedKeyNamePart1, final Object composedKeyValuePart1,
                                                                             final String composedKeyNamePart2, final ImmutableList<Object> composedKeyValuesPart2) {
        return new ListReadQuery(ctx, new ReadQueryDataImpl(tablename).keys(ImmutableMap.of(composedKeyNamePart1, ImmutableList.of(composedKeyValuePart1),
                                                                                            composedKeyNamePart2, composedKeyValuesPart2)));
    }
    
    @Override
    public ListReadWithUnit<ResultList<Record>, Record> readSequenceWithKeys(final String composedKeyNamePart1, final Object composedKeyValuePart1,
                                                                             final String composedKeyNamePart2, final Object composedKeyValuePart2,
                                                                             final String composedKeyNamePart3, final ImmutableList<Object> composedKeyValuesPart3) {
        return new ListReadQuery(ctx, new ReadQueryDataImpl(tablename).keys(ImmutableMap.of(composedKeyNamePart1, ImmutableList.of(composedKeyValuePart1),
                                                                                            composedKeyNamePart2, ImmutableList.of(composedKeyValuePart2),
                                                                                            composedKeyNamePart3, composedKeyValuesPart3)));        
    }

    @Override
    public ListReadWithUnit<ResultList<Record>, Record> readSequenceWithKey(final String composedKeyNamePart1, final Object composedKeyValuePart1) {
        return new ListReadQuery(ctx, new ReadQueryDataImpl(tablename).keys(ImmutableMap.of(composedKeyNamePart1, ImmutableList.of(composedKeyValuePart1))));
    }

    @Override
    public ListReadWithUnit<ResultList<Record>, Record> readSequenceWithKey(final String composedKeyNamePart1, final Object composedKeyValuePart1,
                                                                            final String composedKeyNamePart2, final Object composedKeyValuePart2) {
        return new ListReadQuery(ctx, new ReadQueryDataImpl(tablename).keys(ImmutableMap.of(composedKeyNamePart1, ImmutableList.of(composedKeyValuePart1),
                                                                                            composedKeyNamePart2, ImmutableList.of(composedKeyValuePart2))));
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public <T> ListReadWithUnit<ResultList<Record>, Record> readSequenceWithKeys(final ColumnName<T> name, final ImmutableList<T> values) {
        return readSequenceWithKeys(name.getName(), (ImmutableList<Object>) values);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T, E> ListReadWithUnit<ResultList<Record>, Record> readSequenceWithKeys(final ColumnName<T> composedKeyNamePart1, final T composedKeyValuePart1,
                                                                                    final ColumnName<E> composedKeyNamePart2, final ImmutableList<E> composedKeyValuesPart2) {
        return readSequenceWithKeys(composedKeyNamePart1.getName(), (Object) composedKeyValuePart1,
                                    composedKeyNamePart2.getName(), (ImmutableList<Object>) composedKeyValuesPart2);
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public <T, E, F> ListReadWithUnit<ResultList<Record>, Record> readSequenceWithKeys(final ColumnName<T> composedKeyNamePart1, final T composedKeyValuePart1,
                                                                                       final ColumnName<E> composedKeyNamePart2, final E composedKeyValuePart2,
                                                                                       final ColumnName<F> composedKeyNamePart3, final ImmutableList<F> composedKeyValuesPart3) {
        return readSequenceWithKeys(composedKeyNamePart1.getName(), (Object) composedKeyValuePart1,
                                    composedKeyNamePart2.getName(), (Object) composedKeyValuePart2,
                                    composedKeyNamePart3.getName(), (ImmutableList<Object>) composedKeyValuesPart3);
    }

    @Override
    public <T> ListReadWithUnit<ResultList<Record>, Record> readSequenceWithKey(final ColumnName<T> name, final T value) {
        return readSequenceWithKey(name.getName(), (Object) value);
    }
    
    @Override
    public <T, E> ListReadWithUnit<ResultList<Record>, Record> readSequenceWithKey(final ColumnName<T> composedKeyNamePart1, final T composedKeyValuePart1,
                                                                                   final ColumnName<E> composedKeyNamePart2, final E composedKeyValuePart2) {
        return readSequenceWithKey(composedKeyNamePart1.getName(), (Object) composedKeyValuePart1,
                                   composedKeyNamePart2.getName(), (Object) composedKeyValuePart2);
    }
    
    @Override
    public ListReadWithUnit<ResultList<Record>, Record> readSequenceWhere(final Clause... clauses) {
        return new ListReadQuery(ctx, new ReadQueryDataImpl(tablename).whereConditions(ImmutableSet.copyOf(clauses)));
    }
     
    @Override
    public ListReadWithUnit<ResultList<Record>, Record> readSequence() {
        return new ListReadQuery(ctx, new ReadQueryDataImpl(tablename).columnsToFetch(ImmutableMap.of()));
    }
    
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                          .add("ctx", ctx)
                          .toString();
    }
}
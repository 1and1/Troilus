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


import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

import net.oneandone.troilus.interceptor.CascadeOnDeleteInterceptor;
import net.oneandone.troilus.interceptor.CascadeOnWriteInterceptor;
import net.oneandone.troilus.interceptor.DeleteQueryRequestInterceptor;
import net.oneandone.troilus.interceptor.QueryInterceptor;
import net.oneandone.troilus.interceptor.ReadQueryRequestInterceptor;
import net.oneandone.troilus.interceptor.ReadQueryResponseInterceptor;
import net.oneandone.troilus.interceptor.WriteQueryRequestInterceptor;

import com.datastax.driver.core.ConsistencyLevel;
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
    public Deletion deleteWhere(final Clause... whereConditions) {
        return new DeleteQuery(ctx, new DeleteQueryDataImpl(tablename).whereConditions(ImmutableList.copyOf(whereConditions)));      
    }
   
    public Deletion deleteWithKey(final ImmutableMap<String, Object> keyNameValuePairs) {
        return new DeleteQuery(ctx, new DeleteQueryDataImpl(tablename).key(keyNameValuePairs));      
    }
    
    @Override
    public SingleReadWithUnit<Record, Record> readWithKey(final ImmutableMap<String, Object> composedkey) {
        final Map<String, ImmutableList<Object>> keys = Maps.newHashMap();
        for (Entry<String, Object> entry : composedkey.entrySet()) {
            keys.put(entry.getKey(), ImmutableList.of(entry.getValue()));
        }
        
        return new SingleReadQuery(ctx, new ReadQueryDataImpl(tablename).keys(ImmutableMap.copyOf(keys)));
    }

    @Override
    public ListReadWithUnit<ResultList<Record>, Record> readSequenceWithKeys(final ImmutableMap<String, ImmutableList<Object>> keys) {
        return new ListReadQuery(ctx, new ReadQueryDataImpl(tablename).keys(keys));
    }
    
    @Override
    public ListReadWithUnit<ResultList<Record>, Record> readSequenceWithKey(final ImmutableMap<String, ImmutableList<Object>> key) {
        return new ListReadQuery(ctx, new ReadQueryDataImpl(tablename).keys(key));
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
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

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.UserType;
import com.datastax.driver.core.querybuilder.BuiltStatement;
import com.google.common.collect.ImmutableMap;


 
abstract class AbstractQuery<Q> {
    
    private final Context ctx;

    
    public AbstractQuery(Context ctx) {
        this.ctx = ctx;
    }
    
    @Deprecated
    protected Context getContext() {
        return ctx;
    }
    
    public Q withConsistency(ConsistencyLevel consistencyLevel) {
        return newQuery(ctx.withConsistency(consistencyLevel));
    }
  
    public Q withEnableTracking() {
        return newQuery(ctx.withEnableTracking());
    }
    
    public Q withDisableTracking() {
        return newQuery(ctx.withDisableTracking());
    }
 
    
    abstract protected Q newQuery(Context newContext);
    
    
    
    protected String getTable() {
        return ctx.getTable();
    }
  
    protected ProtocolVersion getProtocolVersion() {
        return ctx.getProtocolVersion();
    }
    
    
    protected ColumnMetadata getColumnMetadata(String columnName) {
        return ctx.getColumnMetadata(columnName);
    }

    
    protected UserType getUserType(String usertypeName) {
        return ctx.getUserType(usertypeName);
    }

    
    public <T> Optional<T> toOptional(T obj) {
        return ctx.toOptional(obj);
    }
    
 
    public boolean isOptional(Object obj) {
        if (obj == null) {
            return false;
        } else {
            return (Optional.class.isAssignableFrom(obj.getClass()));
        }
    }
    
    

    public boolean isBuildInType(DataType dataType) {
        
        if (dataType.isCollection()) {
            for (DataType type : dataType.getTypeArguments()) {
                if (!isBuildinType(type)) {
                    return false;
                }
            }
            return true;

        } else {
            return isBuildinType(dataType);
        }
    }
  
    private boolean isBuildinType(DataType type) {
        return ctx.isBuildInType(type);
    }   
    
    
    protected ImmutableMap<String, Optional<? extends Object>> toValues(Object entity) {
        return ctx.toValues(entity);
    }

    protected PreparedStatement prepare(BuiltStatement statement) {
        return ctx.prepare(statement);
    }
    
    protected CompletableFuture<ResultSet> performAsync(Statement statement) {
        return ctx.performAsync(statement);
    }

    
    protected <T> T fromValues(Class<?> clazz, TriFunction<String, Class<?>, Class<?>, Optional<?>> datasource) {
        return ctx.fromValues(clazz, datasource);
    }

    public Optional<ConsistencyLevel> getConsistencyLevel() {
        return ctx.getConsistencyLevel();
    }

    public Optional<ConsistencyLevel> getSerialConsistencyLevel() {
        return ctx.getSerialConsistencyLevel();
    }

    public Optional<Duration> getTtl() {
        return ctx.getTtl();
    }
}


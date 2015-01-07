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


import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.datastax.driver.core.querybuilder.QueryBuilder.insertInto;
import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.ttl;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Insert;
import com.google.common.collect.Lists;
import com.unitedinternet.troilus.Dao.Insertion;
import com.unitedinternet.troilus.Dao.Mutation;
import com.unitedinternet.troilus.interceptor.InsertQueryData;
import com.unitedinternet.troilus.interceptor.InsertQueryPreInterceptor;


 
class InsertionQuery extends MutationQuery<Insertion> implements Insertion {
    
    private final InsertQueryData data;
  
    public InsertionQuery(Context ctx, InsertQueryData data) {
        super(ctx);
        this.data = data;
    }
    
    
    @Override
    protected InsertionQuery newQuery(Context newContext) {
        return new InsertionQuery(newContext, data);
    }

    
    @Override
    public Mutation<?> ifNotExits() {
        return new InsertionQuery(getContext(), data.ifNotExits(true));
    }

    
    private Statement toStatement(InsertQueryData queryData) {
        Insert insert = insertInto(getContext().getTable());
        
        List<Object> values = Lists.newArrayList();
        queryData.getValuesToMutate().forEach((name, optionalValue) -> { insert.value(name, bindMarker());  values.add(toStatementValue(name, optionalValue.orElse(null))); } ); 
        
        if (queryData.isIfNotExists()) {
            insert.ifNotExists();
            getContext().getSerialConsistencyLevel().ifPresent(serialCL -> insert.setSerialConsistencyLevel(serialCL));
        }

        getTtl().ifPresent(ttl-> { insert.using(ttl(bindMarker()));  values.add((int) ttl.getSeconds()); });

        PreparedStatement stmt = prepare(insert);
        return stmt.bind(values.toArray());
    }


    @Override
    protected Statement getStatement() {
        InsertQueryData queryData = data;
        for (InsertQueryPreInterceptor interceptor : getContext().getInterceptors(InsertQueryPreInterceptor.class)) {
            queryData = interceptor.onPreInsert(queryData); 
        }
        
        return toStatement(queryData);
    }
    
    
    @Override
    public CompletableFuture<Result> executeAsync() {
        return super.executeAsync().thenApply(result -> {
                                                            // check cas result column '[applied]'
                                                            if (data.isIfNotExists() && !result.wasApplied()) {
                                                                throw new IfConditionException("duplicated entry");  
                                                            } 
                                                            return result;
                                                        });
    }
}

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



import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.insertInto;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.unitedinternet.troilus.Dao.Insertion;
import com.unitedinternet.troilus.Dao.Mutation;


 
class InsertionQuery extends MutationQuery<Insertion> implements Insertion {
    private final ImmutableMap<String, Optional<Object>> valuesToMutate;
    private final boolean ifNotExists;
  
  
    public InsertionQuery(Context ctx, QueryFactory queryFactory, ImmutableMap<String, Optional<Object>> valuesToMutate, boolean ifNotExists) {
        super(ctx, queryFactory);
        this.valuesToMutate = valuesToMutate;
        this.ifNotExists = ifNotExists;
    }
    
    
    @Override
    protected Insertion newQuery(Context newContext) {
        return newInsertionQuery(newContext, valuesToMutate, ifNotExists);
    }

    
    @Override
    public Mutation<?> ifNotExits() {
        return newInsertionQuery(valuesToMutate, true);
    }


    @Override
    protected Statement getStatement() {
        // statement
        Insert insert = insertInto(getTable());
         
        List<Object> values = Lists.newArrayList();
        valuesToMutate.forEach((name, optionalValue) -> { insert.value(name, bindMarker());  values.add(toStatementValue(name, optionalValue.orElse(null))); } ); 
        
        
        if (ifNotExists) {
            insert.ifNotExists();
            getSerialConsistencyLevel().ifPresent(serialCL -> insert.setSerialConsistencyLevel(serialCL));
        }

        getTtl().ifPresent(ttl-> { insert.using(QueryBuilder.ttl(bindMarker()));  values.add((int) ttl.getSeconds()); });

        PreparedStatement stmt = prepare(insert);
        return stmt.bind(values.toArray());
    }
    
    
    @Override
    public CompletableFuture<Result> executeAsync() {
        return super.executeAsync().thenApply(result -> {
                if (ifNotExists) {
                    // check cas result column '[applied]'
                    if (!result.wasApplied()) {
                        throw new IfConditionException("duplicated entry");  
                    }
                } 
                return result;
            });
    }
}

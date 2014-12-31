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
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Statement;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.unitedinternet.troilus.Dao.InsertWithValues;
import com.unitedinternet.troilus.Dao.Insertion;
import com.unitedinternet.troilus.QueryFactory.ValueToMutate;


 
class InsertQuery extends MutationQuery<InsertWithValues> implements InsertWithValues {
    
    private final QueryFactory queryFactory;
    private final ImmutableList<? extends ValueToMutate> valuesToMutate;
    private final boolean ifNotExists;

    
    public InsertQuery(Context ctx, QueryFactory queryFactory, ImmutableList<? extends ValueToMutate> valuesToMutate, boolean ifNotExists) {
        super(ctx, queryFactory);
        this.queryFactory = queryFactory;
        this.valuesToMutate = valuesToMutate;
        this.ifNotExists = ifNotExists;
    }
    
    @Override
    protected InsertWithValues newQuery(Context newContext) {
        return queryFactory.newInsertion(newContext, valuesToMutate, ifNotExists);
    }
    
    
    @Override
    public final  InsertWithValues values(ImmutableMap<String, ? extends Object> additionalvaluesToMutate) {
        InsertWithValues write = this;
        for (String name : additionalvaluesToMutate.keySet()) {
            write = write.value(name, additionalvaluesToMutate.get(name));
        }
        return write;
    }
    
      
    @Override
    public InsertWithValues value(String name, Object value) {
        if (getContext().isOptional(value)) {
            Optional<Object> optional = (Optional<Object>) value;
            if (optional.isPresent()) {
                return value(name, optional.get());
            } else {
                return this;
            }
        }
        
        ImmutableList<? extends ValueToMutate> newValuesToInsert;
        
        if (getContext().isBuildInType(getContext().getColumnMetadata(name).getType())) {
            newValuesToInsert = ImmutableList.<ValueToMutate>builder().addAll(valuesToMutate).add(new DaoImpl.BuildinValueToMutate(name, value)).build();
        } else {
            newValuesToInsert = ImmutableList.<ValueToMutate>builder().addAll(valuesToMutate).add(new DaoImpl.UDTValueToMutate(name, value)).build();
        }
        
        return queryFactory.newInsertion(getContext(), newValuesToInsert, ifNotExists);
    }
     
    
    @Override
    public Insertion ifNotExits() {
        return queryFactory.newInsertion(getContext(), valuesToMutate, true);
    }

    
    @Override
    public Statement getStatement() {
        
        // statement
        Insert insert = insertInto(getContext().getTable());
        
        List<Object> values = Lists.newArrayList();
        valuesToMutate.forEach(valueToInsert -> values.add(valueToInsert.addPreparedToStatement(getContext(), insert)));
        
        
        if (ifNotExists) {
            insert.ifNotExists();
            getContext().getSerialConsistencyLevel().ifPresent(serialCL -> insert.setSerialConsistencyLevel(serialCL));
        }

        getContext().getTtl().ifPresent(ttl-> {
                                        insert.using(QueryBuilder.ttl(bindMarker()));
                                        values.add((int) ttl.getSeconds());
                                     });

        PreparedStatement stmt = getContext().prepare(insert);
        return stmt.bind(values.toArray());
    }
    
    
    public Result execute() {
        try {
            return executeAsync().get(Long.MAX_VALUE, TimeUnit.DAYS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw Exceptions.unwrapIfNecessary(e);
        } 
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

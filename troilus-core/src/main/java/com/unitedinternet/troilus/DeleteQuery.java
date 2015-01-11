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
import static com.datastax.driver.core.querybuilder.QueryBuilder.delete;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import com.datastax.driver.core.querybuilder.Clause;


import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.Statement;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.unitedinternet.troilus.Dao.Deletion;
import com.unitedinternet.troilus.interceptor.DeleteQueryData;
import com.unitedinternet.troilus.interceptor.DeleteQueryPreInterceptor;



 
class DeleteQuery extends MutationQuery<Deletion> implements Deletion {

    private final DeleteQueryData data;
    
      
    protected DeleteQuery(Context ctx, DeleteQueryData data) {
        super(ctx);
        this.data = data;
    }


    


    @Override
    protected Deletion newQuery(Context newContext) {
        return new DeleteQuery(newContext, data);
    }
    
    
    @Override
    public Deletion onlyIf(Clause... onlyIfConditions) {
        return new DeleteQuery(getContext(), data.onlyIfConditions(ImmutableList.copyOf(onlyIfConditions)));
    }
    
    
    private Statement toStatement(DeleteQueryData queryData) {
        Delete delete = delete().from(getContext().getTable());
        
        // key-based delete    
        if (queryData.getWhereConditions().isEmpty()) {
            queryData.getOnlyIfConditions().forEach(condition -> delete.onlyIf(condition));
            
            ImmutableSet<Clause> whereClauses = queryData.getKeyNameValuePairs().keySet().stream().map(name -> eq(name, bindMarker())).collect(Immutables.toSet());
            whereClauses.forEach(whereClause -> delete.where(whereClause));
            
            List<Object> values = Lists.newArrayList();
            queryData.getKeyNameValuePairs().keySet().forEach(keyname -> values.add(getContext().toStatementValue(keyname, queryData.getKeyNameValuePairs().get(keyname))));
            
            return prepare(delete).bind(values.toArray());

            
        // where condition-based delete    
        } else {
            queryData.getOnlyIfConditions().forEach(condition -> delete.onlyIf(condition));
            queryData.getWhereConditions().forEach(whereClause -> delete.where(whereClause));
           
            return delete;
        }        
    }
 
    @Override
    public Statement getStatement() {
        DeleteQueryData queryData = data;
        for (DeleteQueryPreInterceptor interceptor : getContext().getInterceptorRegistry().getInterceptors(DeleteQueryPreInterceptor.class)) {
            queryData = interceptor.onPreDelete(queryData); 
        }

        return toStatement(queryData);
    }
    
    public CompletableFuture<Result> executeAsync() {
        return new CompletableDbFuture(performAsync(getStatement()))
                        .thenApply(resultSet -> Result.newResult(resultSet))
                        .thenApply(result -> {
                                                // check cas result column '[applied]'
                                                if (!data.getOnlyIfConditions().isEmpty() && !result.wasApplied()) {
                                                    throw new IfConditionException("if condition does not match");  
                                                } 
                                                return result;

                                             });
    }
}
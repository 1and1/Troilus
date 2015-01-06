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



import java.util.concurrent.CompletableFuture;

import com.datastax.driver.core.querybuilder.Clause;


import com.datastax.driver.core.Statement;
import com.google.common.collect.ImmutableList;
import com.unitedinternet.troilus.Dao.Deletion;



 
class DeleteQuery extends MutationQuery<Deletion> implements Deletion {

    private final DeleteQueryData data;
    
      
    protected DeleteQuery(Context ctx, 
                          DeleteQueryData data) {
        super(ctx);
        this.data = data;
    }
    

    @Override
    protected Deletion newQuery(Context newContext) {
        return new DeleteQuery(newContext, data);
    }
    
    
    @Override
    public Deletion onlyIf(Clause... onlyIfConditions) {
        return new DeleteQuery(getContext(), data.withOnlyIfConditions(ImmutableList.copyOf(onlyIfConditions)));
    }
    
    
 
    @Override
    public Statement getStatement(Context ctx) {
        DeleteQueryData d = data;
        for (DeleteQueryBeforeInterceptor interceptor : ctx.getInterceptors(DeleteQueryBeforeInterceptor.class)) {
            d = interceptor.onBeforeDelete(d); 
        }

        return data.toStatement(ctx);
    }
    

    @Override
    public CompletableFuture<Result> executeAsync() {
        return super.executeAsync().thenApply(result -> {
                                                            // check cas result column '[applied]'
                                                            if (!data.getOnlyIfConditions().isEmpty() && !result.wasApplied()) {
                                                                throw new IfConditionException("if condition does not match");  
                                                            } 
                                                            return result;
                                                        });
    }
}
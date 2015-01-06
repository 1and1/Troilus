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

import com.datastax.driver.core.Statement;
import com.unitedinternet.troilus.Dao.Insertion;
import com.unitedinternet.troilus.Dao.Mutation;


 
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
        return new InsertionQuery(getContext(), data.withIfNotExits(true));
    }


    @Override
    protected Statement getStatement(Context ctx) {
        return data.toStatement(ctx);
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

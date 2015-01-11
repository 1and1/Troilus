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

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.BatchStatement.Type;
import com.google.common.collect.ImmutableList;
import com.unitedinternet.troilus.Dao.BatchMutation;
import com.unitedinternet.troilus.Dao.Batchable;



 
/**
 * Mutation query base class
 * @param <Q> the conrete query 
 */
abstract class MutationQuery<Q> extends AbstractQuery<Q> implements Batchable {
    
    /**
     * @param ctx the context 
     */
    MutationQuery(Context ctx) {
        super(ctx);
    }
    
    public Q withTtl(Duration ttl) {
        return newQuery(getContext().withTtl(ttl.getSeconds()));
    }
    
    public BatchMutation combinedWith(Batchable other) {
        return new BatchMutationQuery(getContext(), Type.LOGGED, ImmutableList.of(this, other));
    }
       
    @Override
    public void addTo(BatchStatement batchStatement) {
        batchStatement.add(getStatement());
    }

    protected abstract Statement getStatement();
}


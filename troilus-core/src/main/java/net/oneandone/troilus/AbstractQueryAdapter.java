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




import java.time.Duration;
import java.util.concurrent.CompletableFuture;




import java.util.concurrent.Executor;

import com.datastax.driver.core.Statement;

import net.oneandone.troilus.AbstractQuery;
import net.oneandone.troilus.Context;
import net.oneandone.troilus.Context.DBSession;
import net.oneandone.troilus.Result;


 
abstract class AbstractQueryAdapter<Q> extends AbstractQuery<Q> {
    
    private final net.oneandone.troilus.java7.Mutation<?, Result> query;
    
   
    public AbstractQueryAdapter(Context ctx, net.oneandone.troilus.java7.Mutation<?, Result> query) {
        super(ctx);
        this.query = query;
    }

    public Q withTtl(Duration ttl) {
        return super.withTtl((int) ttl.getSeconds());
    }
    
    public Result execute() {
        return CompletableFutures.getUninterruptibly(executeAsync());
    }
    
    public CompletableFuture<Result> executeAsync() {
        return CompletableFutures.toCompletableFuture(query.executeAsync());
    }  
    
    public CompletableFuture<Statement> getStatementAsync(ExecutionSpec executionSpec, DBSession dbSession, Executor executor) {
       return CompletableFutures.toCompletableFuture(query.getStatementAsync(executionSpec, dbSession, executor));
    }
}

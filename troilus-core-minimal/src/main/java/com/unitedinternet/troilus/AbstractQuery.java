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


import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;




import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.policies.RetryPolicy;
import com.datastax.driver.core.querybuilder.BuiltStatement;


 
abstract class AbstractQuery<Q> {
    
    private final Context ctx;
    
    
    public AbstractQuery(Context ctx) {
        this.ctx = ctx;
    }

    
    abstract protected Q newQuery(Context newContext);
    

    
    ////////////////////////
    // default implementations
  
    public Q withConsistency(ConsistencyLevel consistencyLevel) {
        return newQuery(ctx.withConsistency(consistencyLevel));
    }

    public Q withEnableTracking() {
        return newQuery(ctx.withEnableTracking());
    }
    
    public Q withDisableTracking() {
        return newQuery(ctx.withDisableTracking());
    }
    
    public Q withRetryPolicy(RetryPolicy policy) {
        return newQuery(ctx.withRetryPolicy(policy));
    }
    
    public Q withWritetime(long writetimeMicrosSinceEpoch) {
        return newQuery(getContext().withWritetime(writetimeMicrosSinceEpoch));
    }
       
    public Q withSerialConsistency(ConsistencyLevel consistencyLevel) {
        return newQuery(getContext().withSerialConsistency(consistencyLevel));
    }

    

    
    
    ////////////////////////
    // utility methods
    protected Context getContext() {
        return ctx; 
    }
    
    protected ProtocolVersion getProtocolVersion() {
        return ctx.getSession().getCluster().getConfiguration().getProtocolOptions().getProtocolVersionEnum();
    }

  
    
    protected PreparedStatement prepare(BuiltStatement statement) {
        try {
            return ctx.getPreparedStatementsCache().get(statement.getQueryString(), new PreparedStatementLoader(statement));
        } catch (ExecutionException e) {
            throw Exceptions.unwrapIfNecessary(e);
        }
    }
    
    
    public class PreparedStatementLoader implements Callable<PreparedStatement> { 
        private final BuiltStatement statement;
    
        public PreparedStatementLoader(BuiltStatement statement) {
            this.statement = statement;
        }

        @Override
        public PreparedStatement call() throws Exception {
            return ctx.getSession().prepare(statement);
        } 
     }
    
    
    
    protected ResultSetFuture performAsync(Statement statement) {
        if (getContext().getConsistencyLevel() != null) {
            statement.setConsistencyLevel(getContext().getConsistencyLevel());
        }
        
        if (getContext().getWritetime() != null) {
            statement.setDefaultTimestamp(getContext().getWritetime());
        }

        if (getContext().getRetryPolicy() != null) {
            statement.setRetryPolicy(getContext().getRetryPolicy());
        }

        if (getContext().getEnableTracing() != null) {
            if (getContext().getEnableTracing()) {
                statement.enableTracing();
            } else {
                statement.disableTracing(); 
            }
        }
        
        return ctx.getSession().executeAsync(statement);
    }
}


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





import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.policies.RetryPolicy;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;


 
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
    

    protected <R extends Result> R assertResultIsAppliedWhen(boolean additionalCondition, R result, String message) throws IfConditionException {
        if (additionalCondition && !result.wasApplied()) {
            throw new IfConditionException(message);  
        }
        
        return result;
    }
    
    /**
     * @param rs  the underlying result set
     * @return the new result 
     */
    Result newResult(ResultSet rs) {
        return new ResultImpl(rs);
    }
    
    
    private static class ResultImpl implements Result {
        private final ResultSet rs;
        
        public ResultImpl(ResultSet rs) {
            this.rs = rs;
        }
        
        @Override
        public boolean wasApplied() {
            return rs.wasApplied();
        }
        
        @Override
        public ExecutionInfo getExecutionInfo() {
            return rs.getExecutionInfo();
        }
        
        @Override
        public ImmutableList<ExecutionInfo> getAllExecutionInfo() {
            return ImmutableList.copyOf(rs.getAllExecutionInfo());
        }
        

        public String toString() {
            StringBuilder builder = new StringBuilder(); 
            for (ExecutionInfo info : getAllExecutionInfo())  {

                builder.append("queried=" + info.getQueriedHost());
                builder.append("\r\ntried=")
                       .append(Joiner.on(",").join(info.getTriedHosts()));


                if (info.getAchievedConsistencyLevel() != null) {
                    builder.append("\r\nachievedConsistencyLevel=" + info.getAchievedConsistencyLevel());
                }
                
                if (info.getQueryTrace() != null) {
                    builder.append("\r\ntraceid=" + info.getQueryTrace().getTraceId());
                    builder.append("\r\nevents:\r\n" + Joiner.on("\r\n").join(info.getQueryTrace().getEvents()));
                }
            }
            return builder.toString();
        }
    }    
}


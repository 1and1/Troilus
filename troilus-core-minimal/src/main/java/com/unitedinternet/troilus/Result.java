/*
 * Copyright (c) 2015 1&1 Internet AG, Germany, http://www.1und1.de
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


import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.ResultSet;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;


/**
 * The query result
 */
public abstract class Result {

    /**
     * @return the execution info for the last query made for this ResultSet
     */
    public abstract ExecutionInfo getExecutionInfo();
    
    /**
     * @return a list of the execution info for all the queries made for this ResultSet
     */
    public abstract ImmutableList<ExecutionInfo> getAllExecutionInfo();
    
    /**
     * @return if the query was a conditional update, whether it was applied. true for other types of queries.
     */
    public abstract boolean wasApplied();
    
    
    /**
     * @param rs  the underlying result set
     * @return the new result 
     */
    static Result newResult(ResultSet rs) {
        return new ResultImpl(rs);
    }
    
    
    private static class ResultImpl extends Result {
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




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




import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.ResultSet;
import com.google.common.collect.ImmutableList;



public abstract class Count implements Result {
    
    public abstract long getCount();
 
    static Count newCountResult(ResultSet rs) {
        return new CountResultImpl(rs);
    }
    
    
    private static final class CountResultImpl extends Count {
        private final ResultSet rs;
        private final long count;

        
        public CountResultImpl(ResultSet rs) {
            this.rs = rs;
            this.count = rs.one().getLong("count");
        }

        @Override
        public ExecutionInfo getExecutionInfo() {
            return rs.getExecutionInfo();
        }
        
        @Override
        public ImmutableList<ExecutionInfo> getAllExecutionInfo() {
            return ImmutableList.copyOf(rs.getAllExecutionInfo());
        }

        @Override
        public boolean wasApplied() {
            return rs.wasApplied();
        }

        
        @Override
        public long getCount() {
            return count;
        }
    }
}




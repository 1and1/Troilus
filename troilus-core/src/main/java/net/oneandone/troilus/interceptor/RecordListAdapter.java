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
package net.oneandone.troilus.interceptor;




import net.oneandone.troilus.FetchingIterator;
import net.oneandone.troilus.Record;
import net.oneandone.troilus.ResultList;

import com.datastax.driver.core.ExecutionInfo;
import com.google.common.collect.ImmutableList;


 
public class RecordListAdapter implements ResultList<Record> {
    private final ResultList<Record> recordList;
    
    public RecordListAdapter(ResultList<Record> recordList) {
        this.recordList = recordList;
    }
    
    @Override
    public ExecutionInfo getExecutionInfo() {
        return recordList.getExecutionInfo();
    }
    
    @Override
    public ImmutableList<ExecutionInfo> getAllExecutionInfo() {
        return recordList.getAllExecutionInfo();
    }

    @Override
    public boolean wasApplied() {
        return recordList.wasApplied();
    }
    
    @Override
    public FetchingIterator<Record> iterator() {
        return recordList.iterator();
    }   
}     
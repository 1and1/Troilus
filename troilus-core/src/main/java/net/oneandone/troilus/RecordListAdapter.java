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




import com.datastax.driver.core.ExecutionInfo;
import com.google.common.collect.ImmutableList;


 
public class RecordListAdapter<T> implements ResultList<T> {
    private final ResultList<T> list;
    
    public RecordListAdapter(ResultList<T> list) {
        this.list = list;
    }
    
    @Override
    public ExecutionInfo getExecutionInfo() {
        return list.getExecutionInfo();
    }
    
    @Override
    public ImmutableList<ExecutionInfo> getAllExecutionInfo() {
        return list.getAllExecutionInfo();
    }

    @Override
    public boolean wasApplied() {
        return list.wasApplied();
    }
    
    @Override
    public FetchingIterator<T> iterator() {
        return list.iterator();
    }   
}     
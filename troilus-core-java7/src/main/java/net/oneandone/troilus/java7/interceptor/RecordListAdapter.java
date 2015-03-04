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
package net.oneandone.troilus.java7.interceptor;

import java.util.Iterator;

import net.oneandone.troilus.DatabaseSubscription;
import net.oneandone.troilus.ResultList;
import net.oneandone.troilus.java7.Record;

import org.reactivestreams.Subscriber;

import com.datastax.driver.core.ExecutionInfo;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;


 
public class RecordListAdapter implements ResultList<Record> {
    private boolean subscribed = false; // true after first subscribe
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
    public ListenableFuture<Void> fetchMoreResults() {
        return recordList.fetchMoreResults();
    }
    
    @Override
    public boolean isFullyFetched() {
        return recordList.isFullyFetched();
    }
    
    
    @Override
    public Iterator<Record> iterator() {
        return recordList.iterator();
    }
    
    @Override
    public void subscribe(Subscriber<? super Record> subscriber) {
        synchronized (this) {
            if (subscribed == true) {
                subscriber.onError(new IllegalStateException("subscription already exists. Multi-subscribe is not supported"));  // only one allowed
            } else {
                subscribed = true;
                new DatabaseSubscription<Record>(subscriber, this).ready();
            }
        } 
    }
}     
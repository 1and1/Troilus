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

import java.util.Iterator;

import net.oneandone.troilus.java7.Record;
import net.oneandone.troilus.java7.RecordList;

import org.reactivestreams.Subscriber;

import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;




class RecordListImpl implements RecordList {
    private boolean subscribed = false; // true after first subscribe
    
    private final Context ctx;
    private final ResultSet rs;

    private final Iterator<Row> iterator;
    
    RecordListImpl(Context ctx, ResultSet rs) {
        this.ctx = ctx;
        this.rs = rs;
        this.iterator = rs.iterator();
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
    public ListenableFuture<Void> fetchMoreResults() {
        return rs.fetchMoreResults();
    }
    
    @Override
    public boolean isFullyFetched() {
        return rs.isFullyFetched();
    }
    
    @Override
    public Iterator<Record> iterator() {
        
        return new Iterator<Record>() {

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }
            
            @Override
            public Record next() {
                return new RecordImpl(ctx, RecordListImpl.this, iterator.next());
            }

           @Override
           public void remove() {
               throw new UnsupportedOperationException();
           }
        };
    }
    
    @Override
    public void subscribe(Subscriber<? super Record> subscriber) {
        synchronized (this) {
            if (subscribed == true) {
                subscriber.onError(new IllegalStateException("subscription already exists. Multi-subscribe is not supported"));  // only one allowed
            } else {
                subscribed = true;
                new DatabaseSubscription<Record>(subscriber, iterator(), this).ready();
            }
        }
    }
}     
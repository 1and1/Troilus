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

import java.util.concurrent.ExecutionException;


import net.oneandone.troilus.java7.FetchingIterator;
import net.oneandone.troilus.java7.Record;
import net.oneandone.troilus.java7.ResultList;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;



class FetchableListPublisher implements Publisher<Record> {
    
    private boolean subscribed = false; // true after first subscribe
    
    private ErrorResultFetchingIterator datasource = null;
    private FetchableListSubscription pendingSubscription = null;;

    
    public FetchableListPublisher(final ListenableFuture<ResultList<Record>> recordsFuture) {

        Runnable listener = new Runnable() {
            
            @Override
            public void run() {
                init(recordsFuture);
            }
        };
        
        recordsFuture.addListener(listener, MoreExecutors.directExecutor());
    }
    
    
    
    private void init(ListenableFuture<ResultList<Record>> recordsFuture) {
        synchronized (this) {
            this.datasource = new ErrorResultFetchingIterator(recordsFuture);
            
            if (pendingSubscription != null) {
                pendingSubscription.ready(datasource);
            }
        }
    }
    
    
    @Override
    public void subscribe(Subscriber<? super Record> subscriber) {
        synchronized (this) {
            if (subscribed == true) {
                subscriber.onError(new IllegalStateException("subscription already exists. Multi-subscribe is not supported"));  // only one allowed
            } else {
                subscribed = true;
                
                FetchableListSubscription databaseSubscription = new FetchableListSubscription(subscriber);
                if (datasource == null) {
                    pendingSubscription = databaseSubscription;
                } else {
                    databaseSubscription.ready(datasource);
                }
            }
        }
    }
    
    
    private static class ErrorResultFetchingIterator implements FetchingIterator<Record> {
        private FetchingIterator<Record> it;
        
        public ErrorResultFetchingIterator(ListenableFuture<ResultList<Record>> recordsFuture) {

            try {
                ResultList<Record> resultList =  recordsFuture.get();
                it = resultList.iterator();
            } catch (InterruptedException | ExecutionException | RuntimeException e) {
                it = new ErrorIterator(e);
            }
        }
       
        
        @Override
        public boolean hasNext() {
            return it.hasNext();
        }
        
        @Override
        public Record next() {
            return it.next();
        }
        
        public boolean isFullyFetched() {
            return it.isFullyFetched();
        }
        
        public ListenableFuture<Void> fetchMoreResults() {
            return it.fetchMoreResults();
        }
        
        
        private static final class ErrorIterator implements FetchingIterator<Record> {
            private final Throwable error;
            
            public ErrorIterator(Throwable error) {
                this.error = error;
            }
            
            @Override
            public boolean hasNext() {
                return true;
            }
            
            @Override
            public Record next() {
                throw new RuntimeException(error); 
            }
            
            @Override
            public ListenableFuture<Void> fetchMoreResults() {
                return Futures.immediateFuture(null);
            }
            
            @Override
            public boolean isFullyFetched() {
                return false;
            }
        }
    }
}


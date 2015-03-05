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
import net.oneandone.troilus.java7.ResultList;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;



class FetchableListPublisher<R> implements Publisher<R> {
    
    private boolean subscribed = false; // true after first subscribe
    
    private ErrorResultFetchingIterator<R> datasource = null;
    private FetchableListSubscription<R> pendingSubscription = null;;

    
    public FetchableListPublisher(final ListenableFuture<ResultList<R>> recordsFuture) {

        Runnable listener = new Runnable() {
            
            @Override
            public void run() {
                init(recordsFuture);
            }
        };
        
        recordsFuture.addListener(listener, MoreExecutors.directExecutor());
    }
    
    
    
    private void init(ListenableFuture<ResultList<R>> recordsFuture) {
        synchronized (this) {
            this.datasource = new ErrorResultFetchingIterator<R>(recordsFuture);
            
            if (pendingSubscription != null) {
                pendingSubscription.ready(datasource);
            }
        }
    }
    
    
    @Override
    public void subscribe(Subscriber<? super R> subscriber) {
        synchronized (this) {
            if (subscribed == true) {
                subscriber.onError(new IllegalStateException("subscription already exists. Multi-subscribe is not supported"));  // only one allowed
            } else {
                subscribed = true;
                
                FetchableListSubscription<R> databaseSubscription = new FetchableListSubscription<>(subscriber);
                if (datasource == null) {
                    pendingSubscription = databaseSubscription;
                } else {
                    databaseSubscription.ready(datasource);
                }
            }
        }
    }
    
    
    private static class ErrorResultFetchingIterator<R> implements FetchingIterator<R> {
        private FetchingIterator<R> it;
        
        public ErrorResultFetchingIterator(ListenableFuture<ResultList<R>> recordsFuture) {

            try {
                ResultList<R> resultList =  recordsFuture.get();
                it = resultList.iterator();
            } catch (InterruptedException | ExecutionException | RuntimeException e) {
                it = new ErrorIterator<R>(e);
            }
        }
       
        
        @Override
        public boolean hasNext() {
            return it.hasNext();
        }
        
        @Override
        public R next() {
            return it.next();
        }
        
        public boolean isFullyFetched() {
            return it.isFullyFetched();
        }
        
        public ListenableFuture<Void> fetchMoreResults() {
            return it.fetchMoreResults();
        }
        
        
        private static final class ErrorIterator<R> implements FetchingIterator<R> {
            private final Throwable error;
            
            public ErrorIterator(Throwable error) {
                this.error = error;
            }
            
            @Override
            public boolean hasNext() {
                return true;
            }
            
            @Override
            public R next() {
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


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



class ResultListPublisher<R> implements Publisher<R> {
    
    private boolean subscribed = false; // true after first subscribe
    private LazyInitializer lazyInitializer;
    
    
    public ResultListPublisher(final ListenableFuture<ResultList<R>> resultlistFuture) {
        lazyInitializer = new LazyInitializer(resultlistFuture);
    }
    
    @Override
    public void subscribe(Subscriber<? super R> subscriber) {
        synchronized (this) {
            try {
                if (subscribed == true) {
                    subscriber.onError(new IllegalStateException("subscription already exists. Multi-subscribe is not supported"));  // only one allowed
                } else {
                    subscribed = true;
                    lazyInitializer.subscribe(subscriber);
                }
            } finally {
                // While the Subscription is not cancelled, Subscription.cancel() MUST request the Publisher to eventually drop any references to the corresponding subscriber.
                lazyInitializer = null;
            }
        }
    }
    
    
    
    private final class LazyInitializer implements Runnable {
        private final ListenableFuture<ResultList<R>> resultlistFuture;
     
        // will be set later
        private boolean isInitialized = false;
        private Subscriber<? super R> subscriber = null; 
        private FetchingIterator<R> iterator = null;
        
        
        public LazyInitializer(ListenableFuture<ResultList<R>> resultlistFuture) {
            this.resultlistFuture = resultlistFuture;
            resultlistFuture.addListener(this, MoreExecutors.directExecutor());
        }
        
        
        @Override
        public void run() {
            synchronized (this) {
                try {
                    this.iterator = resultlistFuture.get().iterator();
                } catch (InterruptedException | ExecutionException | RuntimeException e) {
                    this.iterator = new ErrorIterator<R>(e);
                }
                
                init();
            }
        }
        
        public void subscribe(Subscriber<? super R> subscriber) {
            synchronized (this) {
                if (isInitialized) {
                    subscriber.onError(new IllegalStateException("subscription already exists. Multi-subscribe is not supported"));  // only one allowed
                    return;
                }
                this.subscriber = subscriber;
                
                init();
            }
        }
        
        private void init() {
            synchronized (this) {
                if ((!isInitialized) && (subscriber != null) && (iterator != null)) {
                    isInitialized = true;
                    new ResultListSubscription<>(subscriber, iterator);
                }
            }
        }
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
    


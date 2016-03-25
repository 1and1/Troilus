/*
h * Copyright 1&1 Internet AG, https://github.com/1and1/
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

import java.util.concurrent.CompletableFuture;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import com.datastax.driver.core.ResultSet;



/**
 * ResultList-based publisher
 *
 * @param <R> the element type
 */
class ResultListPublisher<R> implements Publisher<R> {
    private boolean subscribed = false; // true after first subscribe
    private LazyInitializer lazyInitializer;
    
    
    /**
     * @param resultlistFuture  the future result list
     */
    public ResultListPublisher(final CompletableFuture<ResultList<R>> resultlistFuture) {
        this.lazyInitializer = new LazyInitializer(resultlistFuture);
    }
    
    @Override
    public void subscribe(final Subscriber<? super R> subscriber) {
        
        synchronized (this) {
            // https://github.com/reactive-streams/reactive-streams-jvm#1.9
            if (subscriber == null) {  
                throw new NullPointerException("subscriber is null");
            }
            
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
    
    
    
    private final class LazyInitializer {
        // will be set later
        private boolean isInitialized = false;
        private Subscriber<? super R> subscriber = null; 
        private FetchingIterator<R> iterator = null;
        
        
        /**
         * @param resultlistFuture the resultlist future
         */
        public LazyInitializer(final CompletableFuture<ResultList<R>> resultlistFuture) {
            resultlistFuture.thenApplyAsync(resultList -> resultList.iterator())
                            .exceptionally(error -> new ErrorIterator<R>(error))
                            .thenAccept(iterator -> init(iterator));
        }
        
        /**
         * @param subscriber the subscriber to subscribe
         */
        public void subscribe(final Subscriber<? super R> subscriber) {
            synchronized (this) {
                if (isInitialized) {
                    subscriber.onError(new IllegalStateException("subscription already exists. Multi-subscribe is not supported"));  // only one allowed
                    return;
                }
                this.subscriber = subscriber;
                
                init();
            }
        }
        
        private void init(final FetchingIterator<R> iterator) {
            synchronized (this) {
                this.iterator = iterator;
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
        
        public ErrorIterator(final Throwable error) {
            this.error = error;
        }
        
        @Override
        public boolean hasNext() {
            return true;
        }
        
        @Override
        public int getAvailableWithoutFetching() {
            return 1;
        }
        
        @Override
        public R next() {
            throw new RuntimeException(error); 
        }
        
        @Override
        public CompletableFuture<ResultSet> fetchMoreResultsAsync() {
            return CompletableFuture.completedFuture(null);
        }
        
        @Override
        public boolean isFullyFetched() {
            return false;
        }
    }
}      
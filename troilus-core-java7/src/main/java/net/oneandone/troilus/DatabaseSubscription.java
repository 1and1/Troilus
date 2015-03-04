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
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;


import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ListenableFuture;



public class DatabaseSubscription<R> implements Subscription {
    private static final Logger LOG = LoggerFactory.getLogger(DatabaseSubscription.class);

    private final Executor executor = Executors.newCachedThreadPool();
        
    private final Object subscriberCallbackLock = new Object();
    private final Object dbQueryLock = new Object();
    
    private final Subscriber<? super R> subscriber;
    private final Iterator<? extends R> it;
    private final ResultList<? extends R> fetchControl;
    
    private final AtomicLong numRequestedReads = new AtomicLong();
    
    private Runnable runningDatabaseQuery = null;
    private boolean isOpen = true;

    private final Runnable requestTask = new ProcessingTask();

    
    
    public DatabaseSubscription(Subscriber<? super R> subscriber, ResultList<? extends R> resultList) {
        this.subscriber = subscriber;
        this.it = resultList.iterator();
        this.fetchControl = resultList;
    }
    
    public DatabaseSubscription<R> ready() {
        synchronized (subscriberCallbackLock) {
            subscriber.onSubscribe(this);
        }
        return this;
    }
    
    @Override
    public void request(long n) {                
        if(n <= 0) {
            // https://github.com/reactive-streams/reactive-streams#3.9
            synchronized (subscriberCallbackLock) {
                subscriber.onError(new IllegalArgumentException("Non-negative number of elements must be requested: https://github.com/reactive-streams/reactive-streams#3.9"));
            }
            return;
        }
        numRequestedReads.addAndGet(n);
        
        // Subscription: MUST NOT allow unbounded recursion such as Subscriber.onNext -> Subscription.request -> Subscriber.onNext
        executor.execute(requestTask);
    }
    
    private final class ProcessingTask implements Runnable {
    
        @Override
        public void run() {
            processReadRequests();
               
        }
    }
    
    private void processReadRequests() {
        processAvailableDatabaseRecords();

        // more db records required? 
        if (numRequestedReads.get() > 0) {
            // [synchronization note] under some circumstances the method requestDatabaseForMoreRecords()
            // will be executed without the need of more records. However, it does not matter
            requestDatabaseForMoreRecords();
        }
        
    }
    
    
    private void processAvailableDatabaseRecords() {
        synchronized (subscriberCallbackLock) {
            if (isOpen) {
                while (it.hasNext() && numRequestedReads.get() > 0) {
                    try {
                        numRequestedReads.decrementAndGet();
                        subscriber.onNext(it.next());
                    } catch (RuntimeException rt) {
                        LOG.warn("processing error occured", rt);
                        teminateWithError(rt);
                    }
                }

            }
        }
    }
    
    
    private void requestDatabaseForMoreRecords() {
        // no more data to fetch?
        if (fetchControl.isFullyFetched()) {
            terminateRegularly(true);
            return;
        } 
        
        synchronized (dbQueryLock) {
            if (runningDatabaseQuery == null) {
                Runnable databaseRequest = new Runnable() {
                                                    @Override
                                                    public void run() {
                                                        synchronized (dbQueryLock) {
                                                            runningDatabaseQuery = null; 
                                                        }
                                                        processReadRequests();
                                                    }                                                                           
                                           };
                runningDatabaseQuery = databaseRequest;
                
                ListenableFuture<Void> future = fetchControl.fetchMoreResults();
                future.addListener(databaseRequest, executor);
            }
        }
    }

    
    @Override
    public void cancel() {
        terminateRegularly(false);
    }


    ////////////
    // terminate methods: Once a terminal state has been signaled (onError, onComplete) it is REQUIRED that no further signals occur
    
    private void terminateRegularly(boolean signalOnComplete) {
        synchronized (subscriberCallbackLock) {
            if (isOpen) {
                isOpen = false;
                if(signalOnComplete) {
                    subscriber.onComplete();
                }
            }
        }
    }
    
    private void teminateWithError(Throwable t) {
        synchronized (subscriberCallbackLock) {
            if (isOpen) {
                isOpen = false;
                subscriber.onError(t);
            }
        }
    }
    
}

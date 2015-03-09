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

import java.io.Closeable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import net.oneandone.troilus.java7.FetchingIterator;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Queues;
import com.google.common.util.concurrent.ListenableFuture;



/**
 * ResultListSubscription
 * 
 * @param <T> the element type
 */
class ResultListSubscription<T> implements Subscription {
    private static final Logger LOG = LoggerFactory.getLogger(ResultListSubscription.class);
    
    private final DatabaseSource<T> databaseSource;
    private final SubscriberNotifier<T> subscriberNotifier;
  


    /**
     * @param subscriber  the subscriber 
     * @param iterator    the underlying iterator
     */
    public ResultListSubscription(Subscriber<? super T> subscriber, FetchingIterator<T> iterator) {
        Executor executor = Executors.newCachedThreadPool();

        this.subscriberNotifier = new SubscriberNotifier<>(executor, subscriber);
        this.databaseSource = new DatabaseSource<>(executor, subscriberNotifier, iterator);
        subscriberNotifier.emitNotification(new OnSubscribe());
    }

    
    private class OnSubscribe extends net.oneandone.troilus.ResultListSubscription.SubscriberNotifier.Notification<T> {
        
        @Override
        public void send(Subscriber<? super T> subscriber) {
            subscriber.onSubscribe(ResultListSubscription.this);
        }
    }
    
    
    
    @Override
    public void cancel() {
        databaseSource.close();
    }
    
    
    @Override
    public void request(long n) {                
        if(n <= 0) {
            // https://github.com/reactive-streams/reactive-streams#3.9
            subscriberNotifier.emitNotification(new OnError<T>(new IllegalArgumentException("Non-negative number of elements must be requested: https://github.com/reactive-streams/reactive-streams#3.9")));
        } else {
            databaseSource.request(n);
        }
    }
    
 
    
    // Once a terminal state has been signaled (onError, onComplete) it is REQUIRED that no further signals occur
    private static class OnError<R> extends net.oneandone.troilus.ResultListSubscription.SubscriberNotifier.TerminatingNotification<R> {
        private final Throwable error;
        
        public OnError(Throwable error) {
            this.error = error;
        }
        
        @Override
        public void send(Subscriber<? super R> subscriber) {
            LOG.warn("processing error occured", error);
            try {
                subscriber.onError(error);
            } catch (RuntimeException rt) {
                LOG.warn("error occured by notifying error ", rt);
            }
        }
    }

    
    
    
    private static final class DatabaseSource<R> implements Closeable {
        private final Object dbQueryLock = new Object();
        private final AtomicBoolean isOpen = new AtomicBoolean(true);

        private final AtomicLong numRequestedReads = new AtomicLong();
        
        private final Executor executor;
        private final SubscriberNotifier<R> notifier;
        private final FetchingIterator<R> iterator;
        
        private Runnable runningDatabaseQuery = null;


        DatabaseSource(Executor executor, SubscriberNotifier<R> notifier, FetchingIterator<R> iterator) {
            this.executor = executor;
            this.notifier = notifier;
            this.iterator = iterator;
        }
        
        @Override
        public void close() {
            isOpen.set(false);
        }
          
        
        void request(long num) {
            if (isOpen.get()) {
                numRequestedReads.addAndGet(num);
                processReadRequests();
            } 
        }
    
    
        private void processReadRequests() {
            if (isOpen.get()) {
                processAvailableDatabaseRecords();
        
                // more db records required? 
                if (numRequestedReads.get() > 0) {
                    // [synchronization note] under some circumstances the method requestDatabaseForMoreRecords()
                    // will be executed without the need of more records. However, it does not matter
                    requestDatabaseForMoreRecords();
                }
            }
        }
    
        
        private void processAvailableDatabaseRecords() {
            while ((iterator.getAvailableWithoutFetching() > 0) && numRequestedReads.get() > 0) {
                try {
                    numRequestedReads.decrementAndGet();
                    notifier.emitNotification(new OnNext(iterator.next()));
                } catch (RuntimeException rt) {
                    notifier.emitNotification(new OnError<R>(rt));
                }
            }
        }

        
        
        void requestDatabaseForMoreRecords() {
            // no more data to fetch?
            if (iterator.isFullyFetched()) {
                notifier.emitNotification(new OnComplete());
                
            // yes, more elements can be fetched   
            } else { 
                
                // submit async database request 
                synchronized (dbQueryLock) {

                    // will start a new query, if no query is already running 
                    if (runningDatabaseQuery == null) {
                        ListenableFuture<Void> future = iterator.fetchMoreResultsAsync();
                        
                        Runnable databaseRequest = new Runnable() {
                                                            @Override
                                                            public void run() {
                                                                synchronized (dbQueryLock) {
                                                                    runningDatabaseQuery = null; 
                                                                }
                                                                processReadRequests();
                                                            }                                                                           
                                                   };
                        future.addListener(databaseRequest, executor);
                        
                        runningDatabaseQuery = databaseRequest;
                    }
                }
            }
        }
        
        
        private class OnNext extends net.oneandone.troilus.ResultListSubscription.SubscriberNotifier.Notification<R> {
            private final R element;
            
            public OnNext(R element) {
                this.element = element;
            }
            
            @Override
            public void send(Subscriber<? super R> subscriber) {
                subscriber.onNext(element);
            }
        }


        // Once a terminal state has been signaled (onError, onComplete) it is REQUIRED that no further signals occur
        private class OnComplete extends net.oneandone.troilus.ResultListSubscription.SubscriberNotifier.TerminatingNotification<R> {
            
            @Override
            public void send(Subscriber<? super R> subscriber) {
                subscriber.onComplete();
            }
        }
    }
    
    

    private static final class SubscriberNotifier<R> implements Runnable {
        private final ConcurrentLinkedQueue<Notification<R>> notifications = Queues.newConcurrentLinkedQueue();
        private final Executor executor;
        private boolean isOpen = true;
        
        private final Subscriber<? super R> subscriber;
        
        public SubscriberNotifier(Executor executor, Subscriber<? super R> subscriber) {
            this.executor = executor;
            this.subscriber = subscriber;
        }
        
        private void close() {
            synchronized (subscriber) {
                isOpen = false;
                notifications.clear();  
            }
        }
        
        public void emitNotification(Notification<R> notification) {
            synchronized (subscriber) {
                if (isOpen) {
                    if (notifications.offer(notification)) {
                        tryScheduleToExecute();
                    }
                }
            }
        }

        private final void tryScheduleToExecute() {
            try {
                executor.execute(this);
            } catch (Throwable t) {
                close(); // no further notifying (executor does not work anyway)
                subscriber.onError(t);
            }
        }
        

        // main "event loop" 
        @Override 
        public final void run() {
            synchronized (subscriber) {
                if (isOpen) {
                    try {
                        Notification<R> notification = notifications.poll(); 
                        if (notification != null) {
                            if (notification.isTerminating()) {
                                close();
                            }
                            notification.send(subscriber);
                        }
                    } finally {
                        if(!notifications.isEmpty()) {
                            tryScheduleToExecute(); 
                        }
                    }
                }
            }
        }
      
        
        private static abstract class Notification<R> { 
            
            abstract void send(Subscriber<? super R> subscriber);
            
            boolean isTerminating() {
                return false;
            }
        };
        
        
        private static abstract class TerminatingNotification<R> extends Notification<R> { 
            boolean isTerminating() {
                return true;
            }
        };
    }
}
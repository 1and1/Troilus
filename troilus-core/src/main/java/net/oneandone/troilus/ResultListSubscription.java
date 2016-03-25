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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ResultSet;
import com.google.common.collect.Queues;


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
    public ResultListSubscription(final Subscriber<? super T> subscriber, FetchingIterator<T> iterator) {
        final Executor executor = Executors.newCachedThreadPool();

        this.subscriberNotifier = new SubscriberNotifier<>(executor, subscriber);
        this.databaseSource = new DatabaseSource<>(subscriberNotifier, iterator);
       
        subscriberNotifier.emitNotification(new OnSubscribe());
    }

    
    private class OnSubscribe extends SubscriberNotifier.Notification<T> {
        
        @Override
        public void signalTo(final Subscriber<? super T> subscriber) {
            subscriber.onSubscribe(ResultListSubscription.this);
        }
    }
    
    
    
    @Override
    public void cancel() {
        databaseSource.close();
    }
    
    
    @Override
    public void request(final long n) {                
        if(n <= 0) {
            // https://github.com/reactive-streams/reactive-streams#3.9
            subscriberNotifier.emitNotification(new OnError<T>(new IllegalArgumentException("Non-negative number of elements must be requested: https://github.com/reactive-streams/reactive-streams#3.9")));
        } else {
            databaseSource.request(n);
        }
    }
    
 
    
    private static class OnError<R> extends SubscriberNotifier.TerminatingNotification<R> {
        private static final Logger LOG = LoggerFactory.getLogger(OnError.class);
        private final Throwable error;
        
        public OnError(Throwable error) {
            this.error = error;
        }
        
        @Override
        public void signalTo(final Subscriber<? super R> subscriber) {
            LOG.debug("processing error occured", error);
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
        private final SubscriberNotifier<R> notifier;
        private final FetchingIterator<R> iterator;
        private CompletableFuture<ResultSet> runningDatabaseQuery = null;


        DatabaseSource(final SubscriberNotifier<R> notifier, final FetchingIterator<R> iterator) {
            this.notifier = notifier;
            this.iterator = iterator;
        }
        
        @Override
        public void close() {
            isOpen.set(false);
        }
          
        
        void request(final long num) {
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
                    LOG.debug("no record available. Requesting database for more records");

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

            // more data to fetch available?
            if (iterator.isFullyFetched()) {
                LOG.debug("database is already fully fetched");

                // no, all data has been read
                notifier.emitNotification(new OnComplete());
                
            // yes, more elements can be fetched   
            } else { 
                synchronized (dbQueryLock) {
                    // submit an async database query (if not already running) 
                    if (runningDatabaseQuery == null) {
                        this.runningDatabaseQuery = iterator.fetchMoreResultsAsync();
                        runningDatabaseQuery.whenComplete((resultSet, error) -> {
                                                                                    synchronized (dbQueryLock) {
                                                                                        runningDatabaseQuery = null; 
                                                                                    }

                                                                                    if (error == null) {
                                                                                        processReadRequests();
                                                                                    } else {
                                                                                        close();
                                                                                    }
                                                                                });
                    } else {
                        LOG.debug("a database request is already running");
                    }
                }
            }
        }
        
        
        private class OnNext extends SubscriberNotifier.Notification<R> {
            private final R element;
            
            public OnNext(R element) {
                this.element = element;
            }
            
            @Override
            public void signalTo(Subscriber<? super R> subscriber) {
                subscriber.onNext(element);
            }
        }


        private class OnComplete extends SubscriberNotifier.TerminatingNotification<R> {
            
            @Override
            public void signalTo(Subscriber<? super R> subscriber) {
                subscriber.onComplete();
            }
        }
    }
    
    

    private static final class SubscriberNotifier<R> implements Runnable {
        private final ConcurrentLinkedQueue<Notification<R>> notifications = Queues.newConcurrentLinkedQueue();
        private final Executor executor;
        private final AtomicBoolean isOpen = new AtomicBoolean(true);
        
        private final Subscriber<? super R> subscriber;
        
        public SubscriberNotifier(Executor executor, Subscriber<? super R> subscriber) {
            this.executor = executor;
            this.subscriber = subscriber;
        }
        
        private void close() {
            isOpen.set(false);
            notifications.clear();  
        }
        
        public void emitNotification(Notification<R> notification) {
            if (isOpen.get()) {
                if (notifications.offer(notification)) {
                    tryScheduleToExecute();
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
            
            if (isOpen.get()) {
                
                synchronized (subscriber) {
                    try {
                        Notification<R> notification = notifications.poll(); 
                        if (notification != null) {
                            if (notification.isTerminating()) {
                                close();
                            }
                            notification.signalTo(subscriber);
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
            
            abstract void signalTo(Subscriber<? super R> subscriber);
            
            boolean isTerminating() {
                return false;
            }
        };
        
        
        
        // Once a terminal state has been signaled (onError, onComplete) it is REQUIRED that no further signals occur
        private static abstract class TerminatingNotification<R> extends Notification<R> { 
            
            boolean isTerminating() {
                return true;
            }
        };
    }
}
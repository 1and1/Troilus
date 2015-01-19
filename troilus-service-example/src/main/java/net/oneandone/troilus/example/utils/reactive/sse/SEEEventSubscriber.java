/*
 * Copyright (c) 2015 1&1 Internet AG, Germany, http://www.1und1.de
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *  http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.oneandone.troilus.example.utils.reactive.sse;





import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

import javax.servlet.ServletOutputStream;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;





class SEEEventSubscriber implements Subscriber<SSEEvent> {
    private static final Logger LOG = Logger.getLogger(SEEEventSubscriber.class.getName());
    
    private final AtomicBoolean isOpen = new AtomicBoolean(true);
    private final AtomicReference<Subscription> subscriptionRef = new AtomicReference<>(new IllegalStateSubscription());
    
    private final SSEWriteableChannel channel;
    
    
    public SEEEventSubscriber(ServletOutputStream out, ScheduledExecutorService executor) {
        this.channel = new SSEWriteableChannel(out, error -> onError(error), executor);
    }   


   
   
    
    @Override
    public void onSubscribe(Subscription subscription) {
        subscriptionRef.set(subscription);
        requestNext();
    }

    
    private void requestNext() {
        subscriptionRef.get().request(1);    
    }
    
    
    @Override
    public void onNext(SSEEvent event) {
        channel.writeEventAsync(event)           
               .thenAccept(written -> channel.whenWritePossibleAsync()
                                             .thenAccept(Void -> requestNext())); 
    }
    

    @Override
    public void onError(Throwable t) {
        LOG.fine("error on source stream. stop streaming " + t.getMessage());
        close();
    }

  
    @Override
    public void onComplete() {
        close();
    }
    



    private void close() {
        if (isOpen.getAndSet(false)) {
            subscriptionRef.get().cancel();
            channel.close();          
        }
    }
    
    
    
    private static final class IllegalStateSubscription implements Subscription {
        
        @Override
        public void request(long n) {
            throw new IllegalStateException();
        }
        
        @Override
        public void cancel() {
            throw new IllegalStateException();
        }
    }
}

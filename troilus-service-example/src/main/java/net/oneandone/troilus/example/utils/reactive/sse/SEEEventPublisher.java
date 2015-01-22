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
package net.oneandone.troilus.example.utils.reactive.sse;




import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import javax.servlet.ServletInputStream;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;




class SEEEventPublisher implements Publisher<SSEEvent> {     
    
    private final AtomicReference<Optional<Subscriber<? super SSEEvent>>> subscriberRef = new AtomicReference<>(Optional.empty());
    private final ServletInputStream in;
    
    
    public SEEEventPublisher(ServletInputStream in) {
        this.in = in;
    }
    
    
    @Override
    public void subscribe(Subscriber<? super SSEEvent> subscriber) {
        
        synchronized (subscriberRef) {     
            if (subscriberRef.get().isPresent()) {
                throw new IllegalStateException("subscriber is already registered (publisher does not support multi-subscriptions)");
            } 

            subscriberRef.set(Optional.of(subscriber));
            subscriber.onSubscribe(new SEEEventReaderSubscription(in, subscriber));
        }
    }
    
  
 
    
    
    private static final class SEEEventReaderSubscription implements Subscription {
       
        private final SSEReadableChannel channel;
        private final Subscriber<? super SSEEvent> subscriber;
        
       
        public SEEEventReaderSubscription(ServletInputStream in, Subscriber<? super SSEEvent> subscriber) {
            this.subscriber = subscriber;
            this.channel = new SSEReadableChannel(in,                                   // servlet input stream
                                                             error -> subscriber.onError(error),   // error consumer
                                                             Void -> subscriber.onComplete());     // completion consumer         
        }
        
        
        @Override
        public void cancel() {
            channel.close();
        }

        
        @Override
        public void request(long n) {
            channel.readEventAsync()
                   .thenAccept(event -> subscriber.onNext(event));
        }
    }
}

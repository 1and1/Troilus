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
package com.unitedinternet.troilus.example.utils.reactive.stream;



import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;



public class PublisherBasedReadableStream<T> implements ReadableStream<T>, Subscription {
    private final AtomicBoolean isAutoRequest = new AtomicBoolean(true);
    private final AtomicReference<Subscription> subscriptionRef = new AtomicReference<>(new UnsetSubscription());

    private final AtomicBoolean isOpen = new AtomicBoolean();

    private final AtomicReference<Consumer<? super T>> consumerRef = new AtomicReference<>(new UnsetConsumer<>());
    private final AtomicReference<Consumer<? super Throwable>> errorConsumerRef = new AtomicReference<>(new DefaultErrorConsumer());
    
    
    
    
    public PublisherBasedReadableStream(Publisher<T> source) {
        source.subscribe(new Reader());
    }

    
    private final class Reader implements Subscriber<T> {
        
        @Override
        public void onSubscribe(Subscription subscription) {
            subscriptionRef.set(subscription);
        }
        
        @Override
        public void onComplete() {
            close();
        }
        
        @Override
        public void onError(Throwable t) {
            getErrorConsumer().accept(t);
        }
                
        @Override
        public void onNext(T t) {
            try {
                getConsumer().accept(t);
            } finally {
                if (isAutoRequest.get()) {
                    request(1); 
                }
            }
        }
    }
    
    
    @Override
    public void close() {
        if (!isOpen.getAndSet(false)) {
            cancel();
        }
    }
    
    
    @Override
    public void request(long n) {
        subscriptionRef.get().request(n);
        
    }
    
    @Override
    public void cancel() {
        subscriptionRef.get().cancel();
    }

    
    @Override
    public <V> PublisherBasedReadableStream<V> map(Function<? super T, ? extends V> fn) {
        return new MappingReadableStream<>(this, fn);
    }

    
    @Override
    public void consume(Subscriber<? super T> subscriber) {
        isAutoRequest.set(false);

        consume(element -> subscriber.onNext(element), error -> subscriber.onError(error));
        subscriber.onSubscribe(this);
    }
    
    
    @Override
    public void consume(Consumer<? super T> consumer) {
        consume(consumer, errorConsumerRef.get());
    }

    
    @Override
    public void consume(Consumer<? super T> consumer, Consumer<? super Throwable> errorConsumer) {
        this.consumerRef.set(consumer);
        this.errorConsumerRef.set(errorConsumer);

        if (isAutoRequest.get()) {
            request(1); 
        }
        
        onConsumingStarted();
    }
    
    
    
    protected void onConsumingStarted() {      
    }
      
    
    protected Consumer<? super T> getConsumer() {
       return consumerRef.get();  
    }
    
    
    protected Consumer<? super Throwable> getErrorConsumer() {
        return errorConsumerRef.get();  
    }
    


    private class DefaultErrorConsumer implements Consumer<Throwable> {
        
        @Override
        public void accept(Throwable t) {
            close();
        }
    }
    

    

    private static class UnsetConsumer<T> implements Consumer<T> {
        
        @Override
        public void accept(T t) {
            throw new IllegalStateException();
        }
    }
    
    

    private static class UnsetSubscription implements Subscription {
        
        @Override
        public void request(long n) {
            throw new IllegalStateException();            
        }
        
        @Override
        public void cancel() {
            throw new IllegalStateException();
        }
    }
    
    
    
    
    
    
    private static final class MappingReadableStream<T, V> extends PublisherBasedReadableStream<V> {
        private final ReadableStream<T> underlyingStream; 
        private final Function<? super T, ? extends V> fn;

        MappingReadableStream(PublisherBasedReadableStream<T> underlyingStream, Function<? super T, ? extends V> fn) {
            super(new SingleSubscribePublisherAdapter<>(underlyingStream));
            this.underlyingStream = underlyingStream;
            this.fn = fn;
        }
        
        @Override
        protected void onConsumingStarted() {
            underlyingStream.consume(element -> getConsumer().accept(fn.apply(element)),
                                     error -> getErrorConsumer().accept(error));
        }
    }
    
    
    
    private static final class SingleSubscribePublisherAdapter<T> implements Publisher<T> {
        private final Subscription subscription;
        
        public SingleSubscribePublisherAdapter(Subscription subscription) {
            this.subscription = subscription;
        }
        
        @Override
        public void subscribe(Subscriber<? super T> subscriber) {
            subscriber.onSubscribe(subscription);
        }
    }
}




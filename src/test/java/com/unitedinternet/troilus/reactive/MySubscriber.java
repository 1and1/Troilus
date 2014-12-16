package com.unitedinternet.troilus.reactive;



import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;


public class MySubscriber<T> implements Subscriber<T> {
    private final List<T> elements = Lists.newArrayList();
    private final AtomicBoolean isCompleted = new AtomicBoolean();
    private final AtomicReference<Throwable> errorRef = new AtomicReference<>();
    
    private final AtomicReference<Subscription> subscriptionRef = new AtomicReference<>();
    
    @Override
    public void onSubscribe(Subscription subscription) {
        this.subscriptionRef.set(subscription);
        subscription.request(2);
    }
    
    @Override
    public void onComplete() {
        synchronized (this) {
            isCompleted.set(true);
            notifyAll();
        }
    }
    
    
    @Override
    public void onError(Throwable t) {
        synchronized (this) {
            errorRef.set(t);
            notifyAll();
        }
    }
    
    @Override
    public void onNext(T element) {
        synchronized (this) {
            elements.add(element);
        }

        subscriptionRef.get().request(1);
    }
    
    
    public ImmutableList<T> getAll() {

        synchronized (this) {
            if (!isCompleted.get()) {
                try {
                    wait();
                } catch (InterruptedException ignore) { }
            }
        
            return ImmutableList.copyOf(elements);
        }
    }
}


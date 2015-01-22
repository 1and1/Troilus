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
package net.oneandone.troilus.example;



import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;


import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;


public class HotelSubscriber implements Subscriber<Hotel> {
    private final List<Hotel> elements = Lists.newArrayList();
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
    public void onNext(Hotel element) {
        synchronized (this) {
            elements.add(element);
        }

        subscriptionRef.get().request(1);
    }
    
    
    public ImmutableList<Hotel> getAll() {

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


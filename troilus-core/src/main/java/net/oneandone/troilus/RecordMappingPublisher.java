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



import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;



class RecordMappingPublisher implements Publisher<Record> {
    
    private final Publisher<net.oneandone.troilus.java7.Record> publisher;
    
    public RecordMappingPublisher(Publisher<net.oneandone.troilus.java7.Record> publisher) {
        this.publisher = publisher;
    }
    
    
    @Override
    public void subscribe(Subscriber<? super Record> subcriber) {
        publisher.subscribe(new RecordMappingSubscriber(subcriber));
    }
    
    
    private static class RecordMappingSubscriber implements Subscriber<net.oneandone.troilus.java7.Record> {

        private final Subscriber<? super Record> subcriber;
        
        public RecordMappingSubscriber(Subscriber<? super Record> subcriber) {
            this.subcriber = subcriber;
        }
        
        @Override
        public void onSubscribe(Subscription subscription) {
            subcriber.onSubscribe(subscription);
        }
        
        @Override
        public void onComplete() {
            subcriber.onComplete();
        }
        
        @Override
        public void onError(Throwable t) {
            subcriber.onError(t);
        }
        
        @Override
        public void onNext(net.oneandone.troilus.java7.Record record) {
            subcriber.onNext(RecordAdapter.convertFromJava7(record));
        }
    }
}
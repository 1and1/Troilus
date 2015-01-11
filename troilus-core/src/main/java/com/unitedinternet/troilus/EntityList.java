/*
 * Copyright (c) 2014 1&1 Internet AG, Germany, http://www.1und1.de
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
package com.unitedinternet.troilus;



import java.util.Iterator;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import com.datastax.driver.core.ExecutionInfo;
import com.google.common.collect.ImmutableList;



public abstract class EntityList<E> extends Result implements Iterable<E>, Publisher<E> {

    public void remove() {
        throw new UnsupportedOperationException();
    }
    
    
    static <F> EntityList<F> newEntityList(Context ctx, RecordList recordList, Class<F> clazz) {
        return new EntityListImpl<>(ctx, recordList, clazz);
    }
    
    

    private static final class EntityListImpl<F> extends EntityList<F> {
        private final Context ctx;
        private final RecordList recordList;
        private final Class<F> clazz;

        
        public EntityListImpl(Context ctx, RecordList recordList, Class<F> clazz) {
            this.ctx = ctx;
            this.recordList = recordList;
            this.clazz = clazz;
        }

        @Override
        public ExecutionInfo getExecutionInfo() {
            return recordList.getExecutionInfo();
        }
        
        @Override
        public ImmutableList<ExecutionInfo> getAllExecutionInfo() {
            return recordList.getAllExecutionInfo();
        }
        
        @Override
        public boolean wasApplied() {
            return recordList.wasApplied();
        }

        @Override
        public Iterator<F> iterator() {

            return new Iterator<F>() {
                private final Iterator<Record> recordIt = recordList.iterator();
                
                @Override
                public boolean hasNext() {
                    return recordIt.hasNext();
                }
            
                
                @Override
                public F next() {
                    return ctx.getBeanMapper().fromValues(clazz, recordIt.next());
                }
            };
        }
        
         
        @Override
        public void subscribe(Subscriber<? super F> subscriber) {
            recordList.subscribe(new MappingSubscriber<F>(ctx, clazz, subscriber));
        }
        
        private final class MappingSubscriber<G> implements Subscriber<Record> {
            private final Context ctx;
            private final Class<?> clazz;
            
            private Subscriber<? super G> subscriber;
            
            public MappingSubscriber(Context ctx, Class<?> clazz, Subscriber<? super G> subscriber) {
                this.ctx = ctx;
                this.clazz = clazz;
                this.subscriber = subscriber;
            }
            
            @Override
            public void onSubscribe(Subscription subscription) {
                subscriber.onSubscribe(subscription);
            }
            
            @Override
            public void onNext(Record record) {
                subscriber.onNext(ctx.getBeanMapper().fromValues(clazz, record));
            }

            @Override
            public void onError(Throwable t) {
                subscriber.onError(t);
            }
            
            @Override
            public void onComplete() {
                subscriber.onComplete();
            }
        }
    }
}




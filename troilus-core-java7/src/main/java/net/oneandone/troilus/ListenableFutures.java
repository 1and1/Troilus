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



import java.util.concurrent.ExecutionException;

import java.util.concurrent.Executor;

import com.google.common.base.Function;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;


 
class ListenableFutures {
    
    private ListenableFutures() {  }
    
    public static <T> T getUninterruptibly(ListenableFuture<T> future) {
        try {
            return future.get();
        } catch (ExecutionException | InterruptedException | RuntimeException e) {
            throw unwrapIfNecessary(e);
        }
    }
    
    
    
    
    public static <T, E> ListenableFuture<E> transform(ListenableFuture<T> future, Function<T, ListenableFuture<E>> mapperFunction, Executor executor) {
        return new QueryFuture<>(future, mapperFunction, executor);
    }
    
        
    private static final class QueryFuture<T, E> extends AbstractFuture<E> implements Runnable {
        private final ListenableFuture<T> future;
        private final Function<T, ListenableFuture<E>> func;
        
        public QueryFuture(ListenableFuture<T> future, Function<T, ListenableFuture<E>> func, Executor executor) {
            this.future = future;
            this.func = func;
            future.addListener(this, executor);
        }
        
        public void run() {
            
            try {
                final ListenableFuture<E> iFuture = func.apply(future.get());
                
                Runnable resultForwarder = new Runnable() {
                    
                    @Override
                    public void run() {
                        try {
                            set(iFuture.get());
                        } catch (InterruptedException | ExecutionException | RuntimeException e) {
                            setException(ListenableFutures.unwrapIfNecessary(e));
                        }
                    }
                };
                iFuture.addListener(resultForwarder, MoreExecutors.directExecutor());
                
            } catch (InterruptedException | ExecutionException | RuntimeException e) {
                setException(ListenableFutures.unwrapIfNecessary(e));
            }
        };
    }
    
    
    /**
     * @param throwable the Throwable to unwrap
     * @return the unwrapped throwable
     */
    public static RuntimeException unwrapIfNecessary(Throwable throwable)  {
        return unwrapIfNecessary(throwable, 5);
    }
    
    /**
     * @param throwable the Throwable to unwrap
     * @param maxDepth  the max depth
     * @return the unwrapped throwable
     */
    private static RuntimeException unwrapIfNecessary(Throwable throwable , int maxDepth)  {
        
        if (ExecutionException.class.isAssignableFrom(throwable.getClass())) {
            Throwable e = ((ExecutionException) throwable).getCause();

            if (maxDepth > 1) {
                throwable = unwrapIfNecessary(e, maxDepth - 1);
            }
        }
        
        if (throwable instanceof RuntimeException) {
            return (RuntimeException) throwable;
        } else {
            return new RuntimeException(throwable);
        }
    }   
}


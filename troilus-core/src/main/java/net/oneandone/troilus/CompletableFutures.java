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




import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;



class CompletableFutures {
    
    private CompletableFutures() { }
    
  
    public static <V> CompletableFuture<V> failedFuture(final Throwable throwable) {
        final CompletableFuture<V> future = new CompletableFuture<>();
        future.completeExceptionally(throwable);
        return future;
    }

    
    public static <T> CompletableFuture<ImmutableList<T>> join(final ImmutableList<CompletableFuture<T>> futures) {
        return new JoinFuture<>(futures);
    }
    
    private static class JoinFuture<T> extends CompletableFuture<ImmutableList<T>>  {
        private final List<T> results = Lists.newArrayList(); 
        private final AtomicInteger numPending;
        
        public JoinFuture(final ImmutableList<CompletableFuture<T>> futures) {
            numPending = new AtomicInteger(futures.size());
            futures.forEach(future -> future.whenComplete(this::join));
        }
        
        private void join(final T result, final Throwable error) {
            synchronized (results) {
                numPending.decrementAndGet();
                if (error == null) {
                    results.add(result);
                    if (numPending.get() == 0) {
                        complete(ImmutableList.copyOf(results));
                    }
                } else {
                    completeExceptionally(error);
                }
            }
        }
    }
    
    
    
    public static <T> CompletableFuture<T> toCompletableFuture(final ListenableFuture<T> future) {
        return new ListenableToCompletableFutureAdapter<>(future);
    }
    
    
    /**
     * Adapter which maps a ListenableFuture into a CompletableFuture  
     */
    private static class ListenableToCompletableFutureAdapter<T> extends CompletableFuture<T> {
        
        /**
         * @param rsFuture the underlying ResultSetFuture
         */
        public ListenableToCompletableFutureAdapter(final ListenableFuture<T> future) {
            Runnable resultHandler = () -> { 
                try {
                    complete(future.get());
                } catch (ExecutionException ee) {
                    completeExceptionally((ee.getCause() == null) ? ee : ee.getCause());
                } catch (InterruptedException | RuntimeException e) {
                    completeExceptionally(e);
                }
            };
            
            future.addListener(resultHandler, ForkJoinPool.commonPool());
        }
    }   
    
    /**
     * unwraps an exception 
     *  
     * @param ex         the exception to unwrap
     * @return the unwrapped exception
     */
    public static RuntimeException propagate(Throwable ex)  {
        Throwable t = unwrap(ex);
        return (t instanceof RuntimeException) ? (RuntimeException) t : new RuntimeException(t);
    }
    
    
    /**
     * unwraps an exception 
     *  
     * @param ex         the exception to unwrap
     * @return the unwrapped exception
     */
    public static Throwable unwrap(Throwable ex)  {
        return unwrap(ex, 9);
    }
    
    /**
     * unwraps an exception 
     *  
     * @param ex         the exception to unwrap
     * @param maxDepth   the max depth
     * @return the unwrapped exception
     */
    private static Throwable unwrap(Throwable ex, int maxDepth)  {
        if (isCompletionException(ex) || isExecutionException(ex)) {
            Throwable e = ex.getCause();
            if (e != null) {
                if (maxDepth > 1) {
                    return unwrap(e, maxDepth - 1);
                } else {
                    return e;
                }
            }
        }
            
        return ex;
    }
    
    private static boolean isCompletionException(Throwable t) {
        return CompletionException.class.isAssignableFrom(t.getClass());
    }
    
    private static boolean isExecutionException(Throwable t) {
        return ExecutionException.class.isAssignableFrom(t.getClass());
    }
}
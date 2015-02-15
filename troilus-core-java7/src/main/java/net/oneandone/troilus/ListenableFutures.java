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



import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
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
    

    public static <T> ListenableFuture<ImmutableSet<T>> flat(ListenableFuture<ImmutableSet<ListenableFuture<T>>>futureSet, Executor executor) {
        return new FlatFuture<>(futureSet, executor);
    }

    private static final class FlatFuture<T> extends FutureImplBase<ImmutableSet<T>> {
        private final Executor executor;
        private final Set<T> result = Sets.newHashSet();
        private int numPendingFutures = 0;
        
        public FlatFuture(ListenableFuture<ImmutableSet<ListenableFuture<T>>>futureSet, Executor executor) {
            this.executor = executor;
            futureSet.addListener(new FutureSetListner(futureSet), executor);
        }
        
        private void onResult(ListenableFuture<T> future) {
            synchronized (this) {
                try {
                    T t = future.get();
                    if (t != null) {
                        result.add(t);
                    }
                    
                    numPendingFutures--;
                    if (numPendingFutures == 0) {
                        set(ImmutableSet.copyOf(result));
                    }
                    
                } catch (InterruptedException | ExecutionException | RuntimeException e) {
                    setException(ListenableFutures.unwrapIfNecessary(e));
                }
            }
        }
        
        private final class FutureSetListner implements Runnable  {
            private final ListenableFuture<ImmutableSet<ListenableFuture<T>>> futureSet;
            
            public FutureSetListner(ListenableFuture<ImmutableSet<ListenableFuture<T>>>futureSet) {
                this.futureSet = futureSet;
            }
            
            @Override
            public void run() {
                synchronized (FlatFuture.this) {
                    try {
                        for (ListenableFuture<T> future : futureSet.get()) {
                            numPendingFutures++;
                            future.addListener(new FutureListner(future), executor);
                        }
                        
                    } catch (InterruptedException | ExecutionException | RuntimeException e) {
                        setException(ListenableFutures.unwrapIfNecessary(e));
                    }
                }
            }
        }
        
        private final class FutureListner implements Runnable  {
            private final ListenableFuture<T> future;
            
            public FutureListner(ListenableFuture<T> future) {
                this.future = future;
            }
            
            @Override
            public void run() {
                onResult(future);
            }
        }
    }
    
        
    
    public static <T> ListenableFuture<ImmutableSet<T>> flat(ImmutableSet<ListenableFuture<ImmutableSet<T>>> futureSet, Executor executor) {
        return new FlattingFuture<>(futureSet, executor);
    }
    
    
    private static final class FlattingFuture<T> extends FutureImplBase<ImmutableSet<T>> {
        private final Set<T> result = Sets.newHashSet();
        private int numPendingFutures;
        
        public FlattingFuture(ImmutableSet<ListenableFuture<ImmutableSet<T>>> futureSet, Executor executor) {
            numPendingFutures = futureSet.size();
            
            for (ListenableFuture<ImmutableSet<T>> future : futureSet) {
                future.addListener(new FutureListner(future), executor);
            }
        }
        
        private void onResult(ListenableFuture<ImmutableSet<T>> future) {
            synchronized (this) {
                try {
                    result.addAll(future.get());
                    numPendingFutures--;
                    
                    if (numPendingFutures == 0) {
                        set(ImmutableSet.copyOf(result));
                    }
                } catch (InterruptedException | ExecutionException | RuntimeException e) {
                    setException(ListenableFutures.unwrapIfNecessary(e));
                }
            }
        }
        
        private final class FutureListner implements Runnable  {
            private final ListenableFuture<ImmutableSet<T>> future;
            
            public FutureListner(ListenableFuture<ImmutableSet<T>> future) {
                this.future = future;
            }
            
            @Override
            public void run() {
                onResult(future);
            }
        }
    }
        
  
    
    
    
        
    
    public static <T> ListenableFuture<ImmutableSet<T>> join(ListenableFuture<ImmutableSet<T>> futureSet, ListenableFuture<T> future, Executor executor) {
        return new JoiningFuture<>(futureSet, future, executor);
    }
    

    private static final class JoiningFuture<T> extends FutureImplBase<ImmutableSet<T>> {
        private Optional<T> futureResult = null; 
        private ImmutableSet<T> futureSetResult = null;
        
        public JoiningFuture(ListenableFuture<ImmutableSet<T>> futureSet, ListenableFuture<T> future, Executor executor) {
            future.addListener(new FutureListner(future), executor);
            futureSet.addListener(new FutureSetListner(futureSet), executor);
        }
        
        private void onSet() {
            if ((futureResult != null) && (futureSetResult != null)) {
                if (futureResult.isPresent()) {
                    set(ImmutableSet.<T>builder().addAll(futureSetResult).add(futureResult.get()).build());
                } else {
                    set(futureSetResult);
                }
            }
        }

        private void onSetResult(ListenableFuture<ImmutableSet<T>> futureSet) {
            synchronized (this) {
                try {
                    futureSetResult = futureSet.get();
                    onSet();
                } catch (InterruptedException | ExecutionException | RuntimeException e) {
                    setException(ListenableFutures.unwrapIfNecessary(e));
                }
            }
        }
        
        private void onSingleResult(ListenableFuture<T> future) {
            synchronized (this) {
                try {
                    futureResult = Optional.fromNullable(future.get());
                    onSet();
                } catch (InterruptedException | ExecutionException | RuntimeException e) {
                    setException(ListenableFutures.unwrapIfNecessary(e));
                }
            }
        }
        
        private final class FutureListner implements Runnable  {
            private final ListenableFuture<T> future;
            
            public FutureListner(ListenableFuture<T> future) {
                this.future = future;
            }
            
            @Override
            public void run() {
                onSingleResult(future);
            }
        }

        private final class FutureSetListner implements Runnable  {
            private final ListenableFuture<ImmutableSet<T>> futureSet;
            
            public FutureSetListner(ListenableFuture<ImmutableSet<T>> futureSet) {
                this.futureSet = futureSet;
            }
            
            @Override
            public void run() {
                onSetResult(futureSet);
            }
        }
    }
        
  
    
    
    public static <T, E> ListenableFuture<E> transform(ListenableFuture<T> future, Function<T, ListenableFuture<E>> mapperFunction, Executor executor) {
        return new MappingFuture<>(future, mapperFunction, executor);
    }
    
        
    private static final class MappingFuture<T, E> extends AbstractFuture<E> implements Runnable {
        private final ListenableFuture<T> future;
        private final Function<T, ListenableFuture<E>> func;
        
        public MappingFuture(ListenableFuture<T> future, Function<T, ListenableFuture<E>> func, Executor executor) {
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
            Throwable e = throwable.getCause();
            if (e != null) {
                if (maxDepth > 1) {
                    throwable = unwrapIfNecessary(e, maxDepth - 1);
                }
            }
        }
        
        if (throwable instanceof RuntimeException) {
            return (RuntimeException) throwable;
        } else {
            return new RuntimeException(throwable);
        }
    }
    
    
    
    

    private static abstract class FutureImplBase<T> extends AbstractFuture<T> {
        private final AtomicBoolean isHandled = new AtomicBoolean();

        @Override
        protected boolean set(T value) {
            if (!isHandled.getAndSet(true)) {
                return super.set(value);
            } else {
                return false;
            }
        }
        
        @Override
        protected boolean setException(Throwable throwable) {
            if (!isHandled.getAndSet(true)) {
                return super.setException(throwable);
            } else {
                return false;
            } 
        }
    }
    

}


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


import java.util.NoSuchElementException;

import net.oneandone.troilus.java7.FetchingIterator;
import net.oneandone.troilus.java7.ResultList;
import net.oneandone.troilus.java7.interceptor.ResultListAdapter;

import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.ResultSet;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * SingleEntryResultListAdapter
 *
 * @param <T>
 */
class SingleEntryResultListAdapter<T extends Result> extends ResultListAdapter<T> {

    public SingleEntryResultListAdapter(T element) {
        super(new ResultListImpl<>(element));
    }
    
    private static final class ResultListImpl<T extends Result> implements ResultList<T> {
        
        private final FetchingIterator<T> it; 
        private final Result result;
        
        public ResultListImpl(T element) {
            this.result = element;
            this.it = new FetchingIteratorImpl<T>(element);
        }
        
        @Override
        public boolean wasApplied() {
            return result.wasApplied();
        }
        
        @Override
        public ImmutableList<ExecutionInfo> getAllExecutionInfo() {
            return result.getAllExecutionInfo();
        }
        
        @Override
        public ExecutionInfo getExecutionInfo() {
            return result.getExecutionInfo();
        }
        
        @Override
        public FetchingIterator<T> iterator() {
            return it;
        }
        
        
        private static final class FetchingIteratorImpl<T> implements FetchingIterator<T> {
            private T element;
            
            public FetchingIteratorImpl(T element) {
                this.element = element;
            }
            
            @Override
            public boolean hasNext() {
                synchronized (this) {
                    return element != null;
                }
            }
            
            @Override
            public int getAvailableWithoutFetching() {
                return hasNext() ? 1 : 0;
            }
            
            @Override
            public T next() throws NoSuchElementException {
                synchronized (this) {
                    if (element == null) {
                        throw new NoSuchElementException();
                    } else {
                        T e = element;
                        element = null;
                        return e;
                    }
                }
            }

            @Override
            public boolean isFullyFetched() {
                return true;
            }
            
            @Override
            public ListenableFuture<ResultSet> fetchMoreResultsAsync() {
                return Futures.immediateFuture(null);
            }
        }
    }
}     
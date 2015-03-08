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

import java.util.Iterator;
import java.util.concurrent.CompletableFuture;



/**
 * Iterator which supports fetching methods
 * 
 * @param <E> the element type
 */
public interface FetchingIterator<E> extends Iterator<E> {

    /**
     * @return The number of rows that can be retrieved from this result set without blocking to fetch.
     */
    int getAvailableWithoutFetching();
    
    
    /**
     * @return whether all results have been fetched.
     */
    boolean isFullyFetched();
    
    /**
     * @return a future on the completion of fetching the next page of results. 
     *         If the result set is already fully retrieved (isFullyFetched() == true), 
     *         then the returned future will return immediately but not particular error 
     *         will be thrown (you should thus call isFullyFetched() to know if calling this method can be of any use).
     */
    CompletableFuture<Void> fetchMoreResultsAsync();
}


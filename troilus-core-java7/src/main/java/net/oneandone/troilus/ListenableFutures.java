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
package net.oneandone.troilus;





import java.util.concurrent.ExecutionException;

import com.google.common.util.concurrent.ListenableFuture;


 
class ListenableFutures {
    
    private ListenableFutures() {  }
    
    public static <T> T getUninterruptibly(ListenableFuture<T> future) {
        try {
            return future.get();
        } catch (ExecutionException | InterruptedException | RuntimeException e) {
            throw unwrapIfNecessary(e);
        }
    }
    
    
    
    /**
     * @param throwable the Throwable to unwrap
     * @return the unwrapped throwable
     */
    public static RuntimeException unwrapIfNecessary(Throwable throwable )  {
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
            throw (RuntimeException) throwable;
        } else {
            throw new RuntimeException(throwable);
        }
    }   
}


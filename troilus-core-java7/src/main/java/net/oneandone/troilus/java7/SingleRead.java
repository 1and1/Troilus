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
package net.oneandone.troilus.java7;


import com.datastax.driver.core.ConsistencyLevel;


/**
 * Single read query 
 *
 * @param <T>  the result type
 */
public interface SingleRead<T> extends Query<T> {

    /**
     * @return a cloned Dao instance with activated tracking
     */
    SingleRead<T> withTracking();

    /**
     * @return a cloned query instance with deactivated tracking 
     */
    SingleRead<T> withoutTracking();

    
    /**
     * @param consistencyLevel   the  consistency level to use
     * @return a cloned query instance with the modified behavior
     */
    SingleRead<T> withConsistency(ConsistencyLevel consistencyLevel);
}

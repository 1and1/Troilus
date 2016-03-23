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




/**
 * Counter-aware write query
 */
public interface WithCounter {

    /**
     * @param name the name of the value to decrement
     * @return a cloned query instance with the modified behavior
     */
    default CounterMutation decr(final String name) {
        return decr(name, 1);
    }
    
    /**
     * @param name  the name of the value to decrement
     * @param value the value
     * @return a cloned query instance with the modified behavior
     */
    CounterMutation decr(String name, long value); 

    
    /**
     * @param name the name of the value to increment
     * @return a cloned query instance with the modified behavior 
     */
    default CounterMutation incr(final String name) {  
        return incr(name, 1);
    }
    
    /**
     * @param name   the name  of the value to increment
     * @param value  the value
     * @return a cloned query instance with the modified behavior
     */
    CounterMutation incr(String name, long value);   
}


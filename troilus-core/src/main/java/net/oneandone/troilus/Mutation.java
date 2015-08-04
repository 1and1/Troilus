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

import java.util.concurrent.CompletableFuture;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.policies.RetryPolicy;


/**
 * Mutation query
 * 
 * @param <Q> the response type
 * @param <R> the query type
 */
public interface Mutation<Q, R> extends Query<R> {

    /**
     * @param consistencyLevel  the consistency level to use
     * @return a cloned query instance with the modified behavior
     */
    Q withConsistency(ConsistencyLevel consistencyLevel);

    /**
     * @param consistencyLevel  the consistency level to use
     * @return a cloned query instance with the modified behavior
     */
    Q withSerialConsistency(ConsistencyLevel consistencyLevel);


    /**
     * @return a cloned query instance with activated tracking
     */ 
    Q withTracking();

    /**
     * @return a cloned query instance with deactivated tracking
     */
    Q withoutTracking();
    
    /**
     * @param policy  the retry policy
     * @return a cloned query instance with the modified behavior
     */
    Q withRetryPolicy(RetryPolicy policy);
    
    /**
     * @return the statement future
     */
    CompletableFuture<Statement> getStatementAsync(Context ctx);
}


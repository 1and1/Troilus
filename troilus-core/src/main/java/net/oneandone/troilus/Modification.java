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

import java.time.Duration;




/**
 * Modification query (insert or update)
 * 
 * @param <Q> the query type
 */
public interface Modification<Q extends Modification<Q>> extends ConfiguredQuery<Q, Result> {

    /**
     * @param ttl  the time-to-live set
     * @return a cloned query instance with the modified behavior
     */
    Modification<Q> withTtl(Duration ttl);

    /**
     * @param microsSinceEpoch  the writetime in since epoch to set
     * @return a cloned query instance with the modified behavior
     */
    Modification<Q> withWritetime(long microsSinceEpoch);
}
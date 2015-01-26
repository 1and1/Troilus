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

import net.oneandone.troilus.java7.Insertion;






/**
 * insert query implementation
 */
class InsertQuery extends WriteQuery<Insertion> implements Insertion {
  
    
    /**
     * @param ctx   the context
     * @param data  the data
     */
    InsertQuery(Context ctx, WriteQueryDataImpl data) {
        super(ctx, data);
    }
    
    
    @Override
    protected InsertQuery newQuery(Context newContext) {
        return new InsertQuery(newContext, getData());
    }
    
    @Override
    public InsertQuery withTtl(int ttlSec) {
        return newQuery(getContext().withTtl(ttlSec));
    }
    
    @Override
    public InsertQuery ifNotExists() {
        return new InsertQuery(getContext(), getData().ifNotExists(true));
    }
}
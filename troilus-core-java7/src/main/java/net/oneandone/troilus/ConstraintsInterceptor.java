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

import net.oneandone.troilus.java7.interceptor.WriteQueryData;
import net.oneandone.troilus.java7.interceptor.WriteQueryRequestInterceptor;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;




public class ConstraintsInterceptor implements WriteQueryRequestInterceptor  {
    
    private final ImmutableSet<String> notNullColumns;
    private final ImmutableSet<String> immutableColumns;
    
    
    private ConstraintsInterceptor(ImmutableSet<String> notNullColumns, ImmutableSet<String> immutableColumns) {
        this.notNullColumns = notNullColumns;
        this.immutableColumns = immutableColumns;
    }

    public static ConstraintsInterceptor newConstraints() {
        return new ConstraintsInterceptor(ImmutableSet.<String>of(), ImmutableSet.<String>of());
    }
    
    public ConstraintsInterceptor withNotNullColumn(String columnName) {
        return new ConstraintsInterceptor(Immutables.merge(notNullColumns, columnName), immutableColumns);
    }
    
    public ConstraintsInterceptor withNotNullColumn(Name<?> columnName) {
        return new ConstraintsInterceptor(Immutables.merge(notNullColumns, columnName.getName()), immutableColumns);
    }

    public ConstraintsInterceptor withImmutableColumn(String columnName) {
        return new ConstraintsInterceptor(notNullColumns, Immutables.merge(immutableColumns, columnName));
    }
    
    public ConstraintsInterceptor withImmutableColumn(Name<?> columnName) {
        return new ConstraintsInterceptor(notNullColumns, Immutables.merge(immutableColumns, columnName.getName()));
    }

    
    @Override
    public ListenableFuture<WriteQueryData> onWriteRequestAsync(WriteQueryData queryData) throws ConstraintException {
        
        // NOT NULL column check for INSERT
        if (isInsert(queryData)) {
            Set<String> missingColumns = Sets.newHashSet(notNullColumns);
            
            // notNull is key column
            missingColumns.removeAll(queryData.getKeys().keySet()); 

            // notNull is non-key column
            for (String column : Sets.newHashSet(missingColumns)) {
                
                if ((queryData.getValuesToMutate().get(column) != null) && queryData.getValuesToMutate().get(column).isPresent()) {
                    missingColumns.remove(column);
                }
            }
            
            if (!missingColumns.isEmpty()) {
                throw new ConstraintException("NOT NULL column(s) " + Joiner.on(", ").join(missingColumns) + " has to be set");
            }
            
            
        // NOT NULL column check for UPDATE
        } else {
            for (String notNullColumn : notNullColumns) {
                if ((queryData.getValuesToMutate().containsKey(notNullColumn) && ((queryData.getValuesToMutate().get(notNullColumn) != null) && !queryData.getValuesToMutate().get(notNullColumn).isPresent()))) {
                    throw new ConstraintException("NOT NULL column " + notNullColumn + " can not be set with NULL");
                }
            }
        }
        
        
        // NOT UPDATEABLE check
        if (!isInsert(queryData)) {
            for (String immutableColumn : immutableColumns) {
                if ((queryData.getValuesToMutate().containsKey(immutableColumn) && ((queryData.getValuesToMutate().get(immutableColumn) != null) && queryData.getValuesToMutate().get(immutableColumn).isPresent()))) {
                    throw new ConstraintException("immutable column " + immutableColumn + " can not be updated (column can be set within ifNotExists() write query only)");
                }
            }
        }
        
        return Futures.immediateFuture(queryData);
    }
    
    private boolean isInsert(WriteQueryData queryData) {
        return (queryData.getIfNotExits() != null) && queryData.getIfNotExits();
    }
}    




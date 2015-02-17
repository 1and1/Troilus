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
package net.oneandone.troilus.interceptor;




import java.util.Set;

import net.oneandone.troilus.ConstraintException;
import net.oneandone.troilus.ColumnName;
import net.oneandone.troilus.java7.interceptor.WriteQueryData;
import net.oneandone.troilus.java7.interceptor.WriteQueryRequestInterceptor;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

 

/**
 * ConstraintsInterceptor
 */
public class ConstraintsInterceptor implements WriteQueryRequestInterceptor {
    
    private final ImmutableSet<String> notNullColumns;
    private final ImmutableSet<String> immutableColumns;
    
   /**
    * constructor 
    *   
    * @param notNullColumns     the not null columns
    * @param immutableColumns   the immutable columns
    */
    private ConstraintsInterceptor(ImmutableSet<String> notNullColumns, ImmutableSet<String> immutableColumns) {
        this.notNullColumns = notNullColumns;
        this.immutableColumns = immutableColumns;
    }

    /**
     * @return a new instance
     */
    public static ConstraintsInterceptor newConstraints() {
        return new ConstraintsInterceptor(ImmutableSet.<String>of(), ImmutableSet.<String>of());
    }

    /**
     * @param columnName the column name of the not null column
     * @return a new instance updated with this constraint
     */
    public ConstraintsInterceptor withNotNullColumn(ColumnName<?> columnName) {
        return withNotNullColumn(columnName.getName());
    }

    /**
     * @param columnName the column name of the not null column
     * @return a new instance updated with this constraint
     */
    public ConstraintsInterceptor withNotNullColumn(String columnName) {
        return new ConstraintsInterceptor(ImmutableSet.<String>builder().addAll(notNullColumns).add(columnName).build(), immutableColumns);
    }

    /**
     * @param columnName the column name of the immutable column. Please consider, that every write operation without ifNotExists() will count as an update
     * @return a new instance updated with this constraint
     */
    public ConstraintsInterceptor withImmutableColumn(ColumnName<?> columnName) {
        return withImmutableColumn(columnName.getName());
    }

    /**
     * @param columnName the column name of the immutable column. Please consider, that every write operation without ifNotExists() will count as an update
     * @return a new instance updated with this constraint
     */
    public ConstraintsInterceptor withImmutableColumn(String columnName) {
        return new ConstraintsInterceptor(notNullColumns, ImmutableSet.<String>builder().addAll(immutableColumns).add(columnName).build());
    }

    
    @Override
    public ListenableFuture<WriteQueryData> onWriteRequestAsync(WriteQueryData queryData) throws ConstraintException {
        checkNotNullColumn(queryData);
        checkImmutableColumn(queryData);
        
        return Futures.immediateFuture(queryData);
    }
 
    
    private void checkNotNullColumn(WriteQueryData queryData) {

        // NOT NULL column check for INSERT
        if (isLwtQuery(queryData)) {
            Set<String> missingColumns = Sets.newHashSet(notNullColumns);
            
            // notNull is key column
            missingColumns.removeAll(queryData.getKeys().keySet()); 

            // notNull is non-key column
            for (String column : Sets.newHashSet(missingColumns)) {
                
                if (queryData.hasValueToMutate(column)) {
                    missingColumns.remove(column);
                }
            }
            
            if (!missingColumns.isEmpty()) {
                throw new ConstraintException("NOT NULL column(s) " + Joiner.on(", ").join(missingColumns) + " has to be set");
            }
            
            
        // NOT NULL column check for UPDATE
        } else {
            for (String notNullColumn : notNullColumns) {
                if (queryData.hasValueToMutate(notNullColumn) && (queryData.getValueToMutate(notNullColumn) == null)) {
                    throw new ConstraintException("NOT NULL column " + notNullColumn + " can not be set with NULL");
                }
            }
        }
    }
    

    private void checkImmutableColumn(WriteQueryData queryData) {

        if (!isLwtQuery(queryData)) {
            for (String immutableColumn : immutableColumns) {
                if (queryData.hasValueToMutate(immutableColumn) || 
                    queryData.hasSetValuesToAdd(immutableColumn) || 
                    queryData.hasSetValueToRemove(immutableColumn) ||
                    queryData.hasListValueToAppend(immutableColumn) ||
                    queryData.hasListValueToPrepend(immutableColumn) ||
                    queryData.hasListValueToRemove(immutableColumn) ||
                    queryData.hasMapValueToMutate(immutableColumn)) {
                    throw new ConstraintException("immutable column " + immutableColumn + " can not be updated (column can be set within ifNotExists() write query only)");
                }
            }
        }
    }


    
    
    private boolean isLwtQuery(WriteQueryData queryData) {
        return (queryData.getIfNotExits() != null) && queryData.getIfNotExits();
    }
}    




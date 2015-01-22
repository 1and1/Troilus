package net.oneandone.troilus;




import java.util.Set;

import net.oneandone.troilus.interceptor.QueryInterceptor;
import net.oneandone.troilus.java7.interceptor.WriteQueryData;
import net.oneandone.troilus.java7.interceptor.WriteQueryRequestInterceptor;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;




public class Constraints  {
    
    private final ImmutableSet<String> notNullColumns;
    private final ImmutableSet<String> notUpdatableColumns;
    
    
    private Constraints(ImmutableSet<String> notNullColumns, ImmutableSet<String> notUpdatableColumns) {
        this.notNullColumns = notNullColumns;
        this.notUpdatableColumns = notUpdatableColumns;
    }

    public static Constraints newConstraints() {
        return new Constraints(ImmutableSet.<String>of(), ImmutableSet.<String>of());
    }
    
    public Constraints withNotNullColumn(String columnName) {
        return new Constraints(Immutables.merge(notNullColumns, columnName), notUpdatableColumns);
    }
    
    public Constraints withNotNullColumn(Name<?> columnName) {
        return new Constraints(Immutables.merge(notNullColumns, columnName.getName()), notUpdatableColumns);
    }

    public Constraints withNotUpdatableColumn(String columnName) {
        return new Constraints(notNullColumns, Immutables.merge(notUpdatableColumns, columnName));
    }
    
    public Constraints withNotUpdatableColumn(Name<?> columnName) {
        return new Constraints(notNullColumns, Immutables.merge(notUpdatableColumns, columnName.getName()));
    }

    
    
    QueryInterceptor getInterceptor() {
        return new ConstraintsInterceptor();
    }
    
    
    
    private final class ConstraintsInterceptor implements WriteQueryRequestInterceptor {
        
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
                for (String notUpdatableColumn : notUpdatableColumns) {
                    if ((queryData.getValuesToMutate().containsKey(notUpdatableColumn) && ((queryData.getValuesToMutate().get(notUpdatableColumn) != null) && queryData.getValuesToMutate().get(notUpdatableColumn).isPresent()))) {
                        throw new ConstraintException("NOT UPDATEABLE column " + notUpdatableColumn + " can not be updated (column can be set within ifNotExists() write query only)");
                    }
                }
            }
            
            return Futures.immediateFuture(queryData);
        }
        
        private boolean isInsert(WriteQueryData queryData) {
            return (queryData.getIfNotExits() != null) && queryData.getIfNotExits();
        }
    }
}



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



import java.util.ArrayList;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Clause;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;


/**
 * delete query implementation
 */
class DeleteQuery extends MutationQuery<Deletion> implements Deletion {
    private final DeleteQueryData data;
    
    
    /**
     * @param ctx    the context
     * @param data   the data
     */
    DeleteQuery(final Context ctx, final DeleteQueryData data) {
        super(ctx);
        this.data = data;
    }
    
    
    ////////////////////
    // factory methods

    @Override
    protected DeleteQuery newQuery(final Context newContext) {
        return new DeleteQuery(newContext, data);
    }
    
    private DeleteQuery newQuery(final DeleteQueryData data) {
        return new DeleteQuery(getContext(), data);
    }

    //
    ////////////////////

    
    @Override
    public BatchMutationQuery combinedWith(final Batchable<?> other) {
        return new BatchMutationQuery(getContext(), this, other);
    }
    
    @Override
    public DeleteQuery onlyIf(final Clause... onlyIfConditions) {
        return newQuery(data.onlyIfConditions(ImmutableList.copyOf(onlyIfConditions)));
    }
   
    @Override
    public DeleteQuery ifExists() {
        return newQuery(data.ifExists(true));
    }
        
    /**
     * this method builds DeleteQuery of map entries to be removed from 
     * a map column or map columns
     * 
     * @param columnName
     * @param mapKey
     * @return
     */
    @Override
    public DeleteQuery removeMapValue(final String columnName, final Object mapKey) {
        final Map<String, List<Object>> persistentMap = data.getMapValuesToRemove() !=null ? 
    	        Maps.newHashMap(data.getMapValuesToRemove()) : new HashMap<String, List<Object>>();
    			
    	//if map value exists, get existing values and add the new one if not a duplicate
    	if(mapKey!=null) {
    	    final List<Object> list = new ArrayList<Object>();
    		if(data.getMapValuesToRemove() != null) { 
    		    final List<Object> existingList = data.getMapValuesToRemove().get(columnName);
    			if(existingList !=null) {
    				list.addAll(existingList);
    			}
    		}
    		if(!list.contains(mapKey)) {
    			list.add(mapKey);
    		}
    		persistentMap.put(columnName, list);
    	}
    	final ImmutableMap<String, List<Object>> map = ImmutableMap.copyOf(persistentMap);
    	return newQuery(data.mapValuesToRemove(map));
    }
    
    /**
     * this method allows the caller to provide a ColumnName object 
     * and a mapKey to remove a map entry
     * 
     */
    @Override
    public <T,V> Deletion removeMapValue(ColumnName<Map<T, V>> column, Object mapKey) {
    	return removeMapValue(column.getName(), mapKey);
    }
    
    @Override
    public CompletableFuture<Result> executeAsync() {
        return super.executeAsync()
                    .thenApply(result -> {
                                            if (!data.getOnlyIfConditions().isEmpty() && !result.wasApplied()) {
                                                throw new IfConditionException(result, "if condition does not match");
                                            }
                                            return result;
                                         });
    }

    @Override
    public CompletableFuture<Statement> getStatementAsync(final DBSession dbSession) {
        return DeleteQueryDataImpl.toStatementAsync(data, getExecutionSpec(), getUDTValueMapper(), dbSession);
    }
}
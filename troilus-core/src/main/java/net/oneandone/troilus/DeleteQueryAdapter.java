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




import java.util.Map;

import com.datastax.driver.core.querybuilder.Clause;





/**
 * Java8 adapter of a DeleteQuery
 */
class DeleteQueryAdapter extends AbstractQueryAdapter<Deletion> implements Deletion {

    private final DeleteQuery query;
    
    
    /**
     * @param ctx     the context
     * @param query   the query
     */
    DeleteQueryAdapter(Context ctx, DeleteQuery query) {
        super(ctx, query);
        this.query = query;
    }
    
    
    ////////////////////
    // factory methods
    
    @Override
    protected Deletion newQuery(Context newContext) {
        return new DeleteQueryAdapter(newContext, query.newQuery(newContext));
    }

    private Deletion newQuery(DeleteQuery query) {
        return new DeleteQueryAdapter(getContext(), query.newQuery(getContext()));
    }

    // 
    ////////////////////

    
    @Override
    public BatchMutation combinedWith(Batchable<?> other) {
        return new BatchMutationQueryAdapter(getContext(), query.combinedWith(Mutations.toJava7Mutation(other)));
    }
    
    @Override
    public Deletion onlyIf(Clause... onlyIfConditions) {
        return newQuery(query.onlyIf(onlyIfConditions));
    }
    
    @Override
    public Deletion ifExists() {
        return newQuery(query.ifExists());
    }
    
    @Override
    public Deletion removeMapValue(String columnName, Object mapKey) {
    	return newQuery(query.removeMapValue(columnName, mapKey));
    }


	@Override
	public <T, V> Deletion removeMapValue(ColumnName<Map<T, V>> column,
			Object mapKey) {
		return removeMapValue(column.getName(), mapKey);
	}
}
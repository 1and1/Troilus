/*
 * Copyright (c) 2014 1&1 Internet AG, Germany, http://www.1und1.de
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *  http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.unitedinternet.troilus;



import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.insertInto;
import static com.datastax.driver.core.querybuilder.QueryBuilder.set;
import static com.datastax.driver.core.querybuilder.QueryBuilder.update;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.unitedinternet.troilus.Dao.Insertion;
import com.unitedinternet.troilus.Dao.Mutation;
import com.unitedinternet.troilus.Dao.UpdateWithValues;
import com.unitedinternet.troilus.Dao.Update;
import com.unitedinternet.troilus.Dao.Write;


 
class WriteQuery extends MutationQuery<WriteQuery> implements Write {
    
    private final ImmutableMap<String, Object> keys;
    private final ImmutableList<Clause> whereConditions;

    private final ImmutableMap<String, Optional<Object>> valuesToMutate;
    
    
   
    protected static Insertion newInsertionQuery(Context ctx, Object entity) {
        return new InsertionQuery(ctx, entity, false);
    }
    
    protected static InsertionQuery newInsertionQuery(Context ctx, ImmutableMap<String, Optional<Object>> valuesToMutate, boolean ifNotExists) {
        return new InsertionQuery(ctx, valuesToMutate, ifNotExists);
    }
    
    protected static UpdateWithValues<?> newUpdateQuery(Context ctx, ImmutableList<Clause> whereConditions) {
        return new WriteQuery(ctx, ImmutableMap.of(), whereConditions, ImmutableMap.of());
    }
    
    protected static Write newUpdateQuery(Context ctx, ImmutableMap<String, Object> keys) {
        return new WriteQuery(ctx, keys, ImmutableList.of(), ImmutableMap.of());
    }
    
    
    
    
    public WriteQuery(Context ctx, ImmutableMap<String, Object> keys, ImmutableList<Clause> whereConditions, ImmutableMap<String, Optional<Object>> valuesToMutate) {
        super(ctx);
        this.keys = keys;
        this.whereConditions = whereConditions;
        this.valuesToMutate = valuesToMutate;
    }

    
    @Override
    protected WriteQuery newQuery(Context newContext) {
        return new WriteQuery(newContext, keys, whereConditions, valuesToMutate);
    }
    
        
    @Override
    public Write value(String name, Object value) {
        return new WriteQuery(getContext(), keys, whereConditions, Immutables.merge(valuesToMutate, name, toOptional(value)));
    }

    
    @Override
    public Write values(ImmutableMap<String, Object> nameValuePairsToAdd) {
        return new WriteQuery(getContext(), keys, whereConditions, Immutables.merge(valuesToMutate, Immutables.transform(nameValuePairsToAdd, name -> name, value -> toOptional(value))));
    }
    
    

    @Override
    public Insertion ifNotExits() {
        return newInsertionQuery(getContext(), Immutables.merge(valuesToMutate, Immutables.transform(keys, name -> name, value -> toOptional(value))), true);
    }

    @Override
    public Update onlyIf(Clause... conditions) {
        return new UpdateQuery(getContext(), keys, whereConditions, valuesToMutate, ImmutableList.copyOf(conditions));
    }
    
    @Override
    protected Statement getStatement() {
        return new UpdateQuery(getContext(), keys, whereConditions, valuesToMutate, ImmutableList.of()).getStatement();
    }    
    
 
     
    private static final class InsertionQuery extends MutationQuery<Insertion> implements Insertion {
        private final ImmutableMap<String, Optional<Object>> valuesToMutate;
        private final boolean ifNotExists;

        
        public InsertionQuery(Context ctx, Object entity, boolean ifNotExists) {
            this(ctx, ctx.toValues(entity), ifNotExists);
        }
            

        
        public InsertionQuery(Context ctx, ImmutableMap<String, Optional<Object>> valuesToMutate, boolean ifNotExists) {
            super(ctx);
            this.valuesToMutate = valuesToMutate;
            this.ifNotExists = ifNotExists;
        }
        
        
        @Override
        protected Insertion newQuery(Context newContext) {
            return newInsertionQuery(newContext, valuesToMutate, ifNotExists);
        }

        
        @Override
        public Mutation<?> ifNotExits() {
            return newInsertionQuery(getContext(), valuesToMutate, true);
        }


        @Override
        protected Statement getStatement() {
            // statement
            Insert insert = insertInto(getTable());
            
            List<Object> values = Lists.newArrayList();
            valuesToMutate.forEach((name, optionalValue) -> { insert.value(name, bindMarker());  values.add(toStatementValue(name, optionalValue.orElse(null))); } ); 
            
            
            if (ifNotExists) {
                insert.ifNotExists();
                getSerialConsistencyLevel().ifPresent(serialCL -> insert.setSerialConsistencyLevel(serialCL));
            }

            getTtl().ifPresent(ttl-> { insert.using(QueryBuilder.ttl(bindMarker()));  values.add((int) ttl.getSeconds()); });

            PreparedStatement stmt = prepare(insert);
            return stmt.bind(values.toArray());
        }
        
        
        @Override
        public CompletableFuture<Result> executeAsync() {
            return super.executeAsync().thenApply(result -> {
                    if (ifNotExists) {
                        // check cas result column '[applied]'
                        if (!result.wasApplied()) {
                            throw new IfConditionException("duplicated entry");  
                        }
                    } 
                    return result;
                });
        }
    }

    
    
    
    
    private static final class UpdateQuery extends MutationQuery<Update> implements Update {

        private final ImmutableMap<String, Object> keys;
        private final ImmutableMap<String, Optional<Object>> valuesToMutate;
        private final ImmutableList<Clause> ifConditions;
        private final ImmutableList<Clause> whereConditions;


        
        public UpdateQuery(Context ctx, ImmutableMap<String, Object> keys, ImmutableList<Clause> whereConditions, ImmutableMap<String, Optional<Object>> valuesToMutate, ImmutableList<Clause> ifConditions) {
            super(ctx);
            this.keys = keys;
            this.whereConditions = whereConditions;
            this.valuesToMutate = valuesToMutate;
            this.ifConditions = ifConditions;
        }
        
        @Override
        protected Update newQuery(Context newContext) {
            return new UpdateQuery(newContext, 
                                   keys, 
                                   whereConditions,  
                                   valuesToMutate,  
                                   ifConditions);
        }
   
        @Override
        public Mutation<?> onlyIf(Clause... conditions) {
            return new UpdateQuery(getContext(),
                                   keys, 
                                   whereConditions, 
                                   valuesToMutate, 
                                   ImmutableList.<Clause>builder().addAll(ifConditions).addAll(ImmutableList.copyOf(conditions)).build());
        }
   
        
        @Override
        protected Statement getStatement() {
            
            // statement
            com.datastax.driver.core.querybuilder.Update update = update(getContext().getTable());
            
            ifConditions.forEach(condition -> update.onlyIf(condition));

            // key-based update
            if (whereConditions.isEmpty()) {
                List<Object> values = Lists.newArrayList();
                valuesToMutate.forEach((name, optionalValue) -> { update.with(set(name, bindMarker())); values.add(toStatementValue(name, optionalValue.orElse(null))); });
                
                keys.keySet().forEach(keyname -> { update.where(eq(keyname, bindMarker())); values.add(keys.get(keyname)); } );
                
                
                ifConditions.forEach(condition -> update.onlyIf(condition));
       
                getTtl().ifPresent(ttl-> {
                                                update.using(QueryBuilder.ttl(bindMarker()));
                                                values.add((int) ttl.getSeconds());
                                             });
                
                PreparedStatement stmt = prepare(update);
                return stmt.bind(values.toArray());

                
            // where condition-based update
            } else {
                valuesToMutate.forEach((name, optionalValue) -> update.with(set(name, toStatementValue(name, optionalValue.orElse(null)))));
                
                getTtl().ifPresent(ttl-> update.using(QueryBuilder.ttl((int) ttl.getSeconds())));
                
                com.datastax.driver.core.querybuilder.Update.Where where = null;
                
                for (Clause whereClause : whereConditions) {
                    if (where == null) {
                        where = update.where(whereClause);
                    } else {
                        where = where.and(whereClause);
                    }
                }
                
                return update;
            }
        }
        
        @Override
        public CompletableFuture<Result> executeAsync() {
            return super.executeAsync().thenApply(result ->  {
                if (!ifConditions.isEmpty()) {
                    // check cas result column '[applied]'
                    if (!result.wasApplied()) {
                        throw new IfConditionException("if condition does not match");  
                    }
                } 
                return result;
            });
        }
    }
}


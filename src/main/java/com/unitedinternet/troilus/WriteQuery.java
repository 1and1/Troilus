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


import java.util.List;
import java.util.Optional;

import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Statement;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.unitedinternet.troilus.Dao.Insertion;
import com.unitedinternet.troilus.Dao.Update;
import com.unitedinternet.troilus.Dao.Write;
import com.unitedinternet.troilus.Dao.WriteWithValues;


 
class WriteQuery extends MutationQuery<Write> implements WriteWithValues {
    private final QueryFactory queryFactory;
    private final ImmutableMap<String, Object> keys;
    private final ImmutableList<? extends ValueToMutate> valuesToMutate;
    
    public WriteQuery(Context ctx, QueryFactory queryFactory, ImmutableMap<String, Object> keys, ImmutableList<? extends ValueToMutate> valuesToMutate) {
        super(ctx, queryFactory);
        this.queryFactory = queryFactory;
        this.keys = keys;
        this.valuesToMutate = valuesToMutate;
    }
    
    
    @Override
    protected Write newQuery(Context newContext) {
        return queryFactory.newWrite(newContext, keys, valuesToMutate);
    }
    


    @Override
    public Update onlyIf(Clause... conditions) {
        return queryFactory.newUpdate(getContext(), valuesToMutate, keys, ImmutableList.of(), ImmutableList.copyOf(conditions)); 
    }

    @Override
    public Insertion ifNotExits() {
        return queryFactory.newInsertion(getContext(), valuesToMutate, false).values(keys).ifNotExits();
    }
    
    @Override
    public final  WriteWithValues values(ImmutableMap<String, ? extends Object> valuesToMutate) {
        WriteWithValues write = this;
        for (String name : valuesToMutate.keySet()) {
            write = write.value(name, valuesToMutate.get(name));
        }
        return write;
    }
    
 
    @Override
    public WriteWithValues value(String name, Object value) {
        
        if (isOptional(value)) {
            Optional<Object> optional = (Optional<Object>) value;
            if (optional.isPresent()) {
                return value(name, optional.get());
            } else {
                return this;
            }
        }
        
        ImmutableList<? extends ValueToMutate> newValuesToInsert;
        
        if (isBuildInType(getColumnMetadata(name).getType())) {
            newValuesToInsert = ImmutableList.<ValueToMutate>builder().addAll(valuesToMutate).add(new BuildinValueToMutate(name, value)).build();
        } else {
            newValuesToInsert = ImmutableList.<ValueToMutate>builder().addAll(valuesToMutate).add(new UDTValueToMutate(name, value)).build();
        }
        
        return queryFactory.newWrite(getContext(), keys, newValuesToInsert);
    }

 
    
    @Override
    public Statement getStatement() {
        
        // statement
        com.datastax.driver.core.querybuilder.Update update = update(getContext().getTable());
        
        
        
        List<Object> values = Lists.newArrayList();
        valuesToMutate.forEach(valueToInsert -> values.add(valueToInsert.addPreparedToStatement(update)));
        
        keys.keySet().forEach(keyname -> { update.where(eq(keyname, bindMarker())); values.add(keys.get(keyname)); } );
        

        getTtl().ifPresent(ttl-> {
                                        update.using(QueryBuilder.ttl(bindMarker()));
                                        values.add((int) ttl.getSeconds());
                                     });

        PreparedStatement stmt = prepare(update);
        return stmt.bind(values.toArray());
    }
}


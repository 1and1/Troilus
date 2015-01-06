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




import static com.datastax.driver.core.querybuilder.QueryBuilder.insertInto;

import java.util.List;
import java.util.Optional;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;



 
public class InsertQueryData extends QueryData {
    
    private final ImmutableMap<String, Optional<Object>> valuesToMutate;
    private final boolean ifNotExists;
  


    public InsertQueryData(ImmutableMap<String, Optional<Object>> valuesToMutate, 
                           boolean ifNotExists) {
        this.valuesToMutate = valuesToMutate;
        this.ifNotExists = ifNotExists;
    }
    
    

    public InsertQueryData withValuesToMutate(ImmutableMap<String, Optional<Object>> valuesToMutate) {
        return new InsertQueryData(valuesToMutate, 
                                   this.ifNotExists);  
    }
    
    public InsertQueryData withIfNotExits(boolean ifNotExists) {
        return new InsertQueryData(this.valuesToMutate, 
                                   ifNotExists);  
    }
 
    
    public ImmutableMap<String, Optional<Object>> getValuesToMutate() {
        return valuesToMutate;
    }

    public boolean isIfNotExists() {
        return ifNotExists;
    }

    
    
    Statement toStatement(Context ctx) {
        Insert insert = insertInto(ctx.getTable());
        
        List<Object> values = Lists.newArrayList();
        valuesToMutate.forEach((name, optionalValue) -> { insert.value(name, bindMarker());  values.add(ctx.toStatementValue(name, optionalValue.orElse(null))); } ); 
        
        if (ifNotExists) {
            insert.ifNotExists();
            ctx.getSerialConsistencyLevel().ifPresent(serialCL -> insert.setSerialConsistencyLevel(serialCL));
        }

        ctx.getTtl().ifPresent(ttl-> { insert.using(QueryBuilder.ttl(bindMarker()));  values.add((int) ttl.getSeconds()); });

        PreparedStatement stmt = ctx.prepare(insert);
        return stmt.bind(values.toArray());
    }
}
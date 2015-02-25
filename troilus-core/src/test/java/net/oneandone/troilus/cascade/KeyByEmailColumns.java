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
package net.oneandone.troilus.cascade;


import net.oneandone.troilus.ColumnName;



public interface KeyByEmailColumns  {
   
    public static final String TABLE = "key_by_email";
    
    public static final ColumnName<String> EMAIL = ColumnName.defineString("email");
    public static final ColumnName<Long> CREATED = ColumnName.defineLong("created");
    public static final ColumnName<byte[]> KEY = ColumnName.defineBytes("key");
    public static final ColumnName<String> ACCOUNT_ID = ColumnName.defineString("account_id");
        
    public static final String DDL = "com/unitedinternet/troilus/example/key_by_email.ddl";
}

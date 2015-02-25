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
package net.oneandone.troilus.userdefinieddatatypes;




public interface CustomersTable  {
   
    public static final String TABLE = "customers";
    
    public static final String ID = "id";
    public static final String NAME = "name";
    public static final String CURRENT_ADDRESS = "current_address";
    public static final String OLD_ADDRESSES = "old_addresses";
    public static final String PHONE_NUMBERS = "phone_numbers";
    public static final String CLASSIFICATION = "classification";
    public static final String CLASSIFICATION2 = "classification2";
    public static final String ROLES = "roles";
    
    
    
    public static final String DDL = "com/unitedinternet/troilus/example/customers.ddl";
 }

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

import java.util.List;
import java.util.Map;
import java.util.Set;

import net.oneandone.troilus.ColumnName;



public final class UsersTableFields  {
    
    public static final ColumnName<String> USER_ID = ColumnName.defineString("user_id");
    public static final ColumnName<UserType> USER_TYPE = ColumnName.define("user_type", UserType.class);
    public static final ColumnName<String> NAME = ColumnName.defineString("name");
    public static final ColumnName<Boolean> IS_CUSTOMER = ColumnName.defineBool("is_customer");
    public static final ColumnName<byte[]> PICTURE = ColumnName.defineBytes("picture");
    public static final ColumnName<List<String>> ADDRESSES = ColumnName.defineList("addresses", String.class);
    public static final ColumnName<Long> MODIFIED = ColumnName.defineLong("modified");
    public static final ColumnName<Set<String>> PHONE_NUMBERS = ColumnName.defineSet("phone_numbers", String.class);
    public static final ColumnName<Map<String, String>> ROLES = ColumnName.defineMap("roles", String.class, String.class);    
 }


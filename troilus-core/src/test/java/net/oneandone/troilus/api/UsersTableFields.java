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
package net.oneandone.troilus.api;

import java.util.List;
import java.util.Map;
import java.util.Set;

import net.oneandone.troilus.Name;



public final class UsersTableFields  {
    
    public static final Name<String> USER_ID = Name.defineString("user_id");
    public static final Name<UserType> USER_TYPE = Name.define("user_type", UserType.class);
    public static final Name<String> NAME = Name.defineString("name");
    public static final Name<Boolean> IS_CUSTOMER = Name.defineBool("is_customer");
    public static final Name<byte[]> PICTURE = Name.defineBytes("picture");
    public static final Name<List<String>> ADDRESSES = Name.defineList("addresses", String.class);
    public static final Name<Long> MODIFIED = Name.defineLong("modified");
    public static final Name<Set<String>> PHONE_NUMBERS = Name.defineSet("phone_numbers", String.class);
    public static final Name<Map<String, String>> ROLES = Name.defineMap("roles", String.class, String.class);    
 }


package com.unitedinternet.troilus.api;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.unitedinternet.troilus.Name;



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


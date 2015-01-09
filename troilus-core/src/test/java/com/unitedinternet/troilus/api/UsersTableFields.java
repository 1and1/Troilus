package com.unitedinternet.troilus.api;

import java.nio.ByteBuffer;

import com.unitedinternet.troilus.ListName;
import com.unitedinternet.troilus.MapName;
import com.unitedinternet.troilus.Name;
import com.unitedinternet.troilus.SetName;



public final class UsersTableFields  {
    
    public static final Name<String> USER_ID = Name.defineString("user_id");
    public static final Name<String> NAME = Name.defineString("name");
    public static final Name<Boolean> IS_CUSTOMER = Name.defineBool("is_customer");
    public static final Name<ByteBuffer> PICTURE = Name.defineByte("picture");
    public static final ListName<String> ADDRESSES = ListName.defineString("addresses");
    public static final Name<Long> MODIFIED = Name.defineLong("modified");
    public static final SetName<String> PHONE_NUMBERS = SetName.defineString("phone_numbers");
    public static final MapName<String, String> ROLES = MapName.define("roles", String.class, String.class);    
 }


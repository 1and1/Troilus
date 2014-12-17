package com.unitedinternet.troilus.api;

import com.unitedinternet.troilus.Schema;



public interface UserTable  {
   
    public static final String TABLE = "users";
    
    public static final String USER_ID = "user_id";
    public static final String NAME = "name";
    public static final String IS_CUSTOMER = "is_customer";
    public static final String PICTURE = "picture";
    public static final String ADDRESSES = "addresses";
    public static final String MODIFIED = "modified";
    public static final String PHONE_NUMBERS = "phone_numbers";
    
    
    public static final String CREATE_STMT = Schema.load("com/unitedinternet/troilus/example/users.ddl");
 }

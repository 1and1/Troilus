package com.unitedinternet.troilus.api;



public interface UserTable  {
   
    public static final String TABLE = "user";
    
    public static final String USER_ID = "user_id";
    public static final String NAME = "name";
    public static final String IS_CUSTOMER = "is_customer";
    public static final String PICTURE = "picture";
    public static final String ADDRESSES = "addresses";
    public static final String MODIFIED = "modified";
    public static final String PHONE_NUMBERS = "phone_numbers";
    
    
    public static final String CREATE_STMT = "CREATE TABLE user (" + 
                                             "                   user_id text, " +
                                             "                   name text, " +
                                             "                   is_customer boolean, " +
                                             "                   picture blob, " +
                                             "                   modified bigint, " +
                                             "                   phone_numbers set<text>, " + 
                                             "                   addresses list<text>,"+
                                             "                   PRIMARY KEY (user_id)" +    
                                             "                  )";

}

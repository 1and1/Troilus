package net.oneandone.troilus.api;

import net.oneandone.troilus.Schema;



public interface LoginsTable  {
   
    public static final String TABLE = "logins";
    
    public static final String USER_ID = "user_id";
    public static final String LOGINS = "logins";
    
    public static final String CREATE_STMT = Schema.load("com/unitedinternet/troilus/example/logins.ddl");
 }


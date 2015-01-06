package com.unitedinternet.troilus.referentialintegrity;

import com.unitedinternet.troilus.Schema;



public interface PhonenumbersTable  {
   
    public static final String TABLE = "phone_numbers";
    
    public static final String NUMBER = "number";
    public static final String DEVICE_ID = "device_id";
    public static final String ACTIVE = "active";
        
    public static final String CREATE_STMT = Schema.load("com/unitedinternet/troilus/example/phone_numbers.ddl");
}

package com.unitedinternet.troilus.example;

import com.unitedinternet.troilus.Schema;



public interface AddressType  {
   
    public static final String STREET = "street";
    public static final String CITY = "city";
    public static final String POST_CODE = "post_code";
    

   
    
    public static final String CREATE_STMT = Schema.load("com/unitedinternet/troilus/example/address.ddl");
}

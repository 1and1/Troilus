package com.unitedinternet.troilus.userdefinieddatatypes;

import com.unitedinternet.troilus.Schema;



public interface CustomersTable  {
   
    public static final String TABLE = "customers";
    
    public static final String ID = "id";
    public static final String NAME = "name";
    public static final String CURRENT_ADDRESS = "current_address";
    public static final String OLD_ADDRESSES = "old_addresses";
    public static final String PHONE_NUMBERS = "phone_numbers";
    public static final String CLASSIFICATION = "classification";
    public static final String CLASSIFICATION2 = "classification2";
    
    
    
    
    
    public static final String CREATE_STMT = Schema.load("com/unitedinternet/troilus/example/customers.ddl");
 }
